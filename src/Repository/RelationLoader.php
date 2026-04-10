<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

use MonkeysLegion\Database\Contracts\ConnectionManagerInterface;
use MonkeysLegion\Entity\Attributes\JoinTable;
use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Entity\Attributes\OneToOne;
use MonkeysLegion\Query\Query\QueryBuilder;
use ReflectionClass;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Eager-loads entity relations via batched WHERE IN queries.
 *
 * Strategy:
 *   ManyToOne / OneToOne  → batch related entities by collected FK IDs
 *   OneToMany             → batch by parent IDs on the foreign key column
 *   ManyToMany            → batch via pivot table + parent IDs
 *
 * No proxy objects, no lazy loading — only explicit `->with()`.
 * This keeps query count **predictable** and eliminates hidden N+1.
 *
 * Performance:
 *   - Relation metadata cached per class (one reflection pass per process)
 *   - FK IDs collected during hydration (no extra queries)
 *   - Batch cap at 500 IDs per WHERE IN (split into multiple queries)
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class RelationLoader
{
    /** Batch cap for WHERE IN queries. */
    private const BATCH_SIZE = 500;

    /** @var array<string, list<RelationMetadata>> class → relation metadata */
    private static array $metadataCache = [];

    public function __construct(
        private readonly ConnectionManagerInterface $manager,
        private readonly EntityHydrator $hydrator,
        private readonly IdentityMap $identityMap,
    ) {}

    /**
     * Eager-load relations for a list of hydrated entities.
     *
     * @param list<object>  $entities     Already-hydrated parent entities.
     * @param list<string>  $relations    Relation names (supports dot-notation for nesting).
     * @param class-string  $entityClass  Parent entity class.
     * @param string        $table        Parent table name (for building query context).
     */
    public function eagerLoad(
        array $entities,
        array $relations,
        string $entityClass,
        string $table,
    ): void {
        if ($entities === [] || $relations === []) {
            return;
        }

        // Parse dot-notation into a load plan: ['user' => [], 'items' => ['product' => []]]
        $plan = $this->parseRelations($relations);

        $allMeta = $this->getRelationMetadata($entityClass);

        foreach ($plan as $relationName => $nested) {
            $meta = $this->findMeta($allMeta, $relationName);
            if ($meta === null) {
                continue;
            }

            $relatedEntities = match ($meta->relationType) {
                'ManyToOne', 'OneToOne' => $this->loadBelongsTo($entities, $meta),
                'OneToMany'             => $this->loadHasMany($entities, $meta),
                'ManyToMany'            => $this->loadManyToMany($entities, $meta),
                default                 => [],
            };

            // Recurse for nested relations (e.g., 'items.product')
            if ($nested !== [] && $relatedEntities !== []) {
                $nestedRelations = array_keys($nested);
                // Determine target table from #[Entity] or fallback to snake_case
                $targetTable = $this->resolveTable($meta->targetEntity);

                $this->eagerLoad(
                    $relatedEntities,
                    $nestedRelations,
                    $meta->targetEntity,
                    $targetTable,
                );
            }
        }
    }

    // ── Loading Strategies ──────────────────────────────────────

    /**
     * ManyToOne / OneToOne: collect FK values from parents, batch-load related entities.
     *
     * @return list<object> Loaded related entities.
     */
    private function loadBelongsTo(array $entities, RelationMetadata $meta): array
    {
        // Collect FK values from parent entities
        $fkValues = [];
        foreach ($entities as $entity) {
            $fk = $this->hydrator->getPropertyValue($entity, $meta->propertyName);
            if ($fk !== null && !is_object($fk)) {
                $fkValues[$fk] = true;
            }
        }

        if ($fkValues === []) {
            return [];
        }

        $ids = array_keys($fkValues);
        $targetTable = $this->resolveTable($meta->targetEntity);

        // Batch load related entities
        $relatedMap = $this->batchLoad($meta->targetEntity, $targetTable, 'id', $ids);

        // Assign related entities back to parents
        $ref = new \ReflectionProperty($entities[0]::class, $meta->propertyName);
        foreach ($entities as $entity) {
            $fk = $this->hydrator->getPropertyValue($entity, $meta->propertyName);
            if ($fk !== null && isset($relatedMap[$fk])) {
                $ref->setValue($entity, $relatedMap[$fk]);
            }
        }

        return array_values($relatedMap);
    }

    /**
     * OneToMany: batch-load children by parent IDs on the FK column.
     *
     * @return list<object>
     */
    private function loadHasMany(array $entities, RelationMetadata $meta): array
    {
        $parentIds = $this->collectIds($entities);
        if ($parentIds === []) {
            return [];
        }

        $targetTable = $this->resolveTable($meta->targetEntity);
        $fkColumn = $meta->foreignKey;

        // Query children: WHERE fk_column IN (parent_ids)
        $allChildren = [];
        foreach (array_chunk($parentIds, self::BATCH_SIZE) as $batch) {
            $rows = (new QueryBuilder($this->manager))
                ->from($targetTable)
                ->whereIn($fkColumn, $batch)
                ->get();

            foreach ($rows as $row) {
                $child = $this->hydrateAndTrack($meta->targetEntity, $row);
                $allChildren[] = $child;
            }
        }

        // Group children by parent FK
        $grouped = [];
        foreach ($allChildren as $child) {
            $fkValue = $this->hydrator->getPropertyValue($child, $this->fkPropertyName($meta));
            if ($fkValue !== null) {
                $key = is_object($fkValue) ? $this->hydrator->getEntityId($fkValue) : $fkValue;
                $grouped[$key][] = $child;
            }
        }

        // Assign to parents
        $ref = new \ReflectionProperty($entities[0]::class, $meta->propertyName);
        foreach ($entities as $entity) {
            $id = $this->hydrator->getEntityId($entity);
            $ref->setValue($entity, $grouped[$id] ?? []);
        }

        return $allChildren;
    }

    /**
     * ManyToMany: batch-load via pivot table.
     *
     * @return list<object>
     */
    private function loadManyToMany(array $entities, RelationMetadata $meta): array
    {
        $parentIds = $this->collectIds($entities);
        if ($parentIds === [] || $meta->joinTable === null) {
            return [];
        }

        $targetTable = $this->resolveTable($meta->targetEntity);
        $pivotTable = $meta->joinTable;
        $sourceKey = $meta->joinSourceKey ?? $this->toSnakeCase(
            $this->shortClassName($entities[0]::class)
        ) . '_id';
        $targetKey = $meta->joinTargetKey ?? $this->toSnakeCase(
            $this->shortClassName($meta->targetEntity)
        ) . '_id';

        // Query pivot: SELECT * FROM pivot WHERE source_key IN (?)
        $pivotRows = [];
        foreach (array_chunk($parentIds, self::BATCH_SIZE) as $batch) {
            $rows = (new QueryBuilder($this->manager))
                ->from($pivotTable)
                ->whereIn($sourceKey, $batch)
                ->get();

            foreach ($rows as $row) {
                $pivotRows[] = $row;
            }
        }

        if ($pivotRows === []) {
            return [];
        }

        // Collect target IDs from pivot
        $targetIds = array_unique(array_column($pivotRows, $targetKey));

        // Batch load target entities
        $relatedMap = $this->batchLoad($meta->targetEntity, $targetTable, 'id', $targetIds);

        // Group by source ID via pivot
        $grouped = [];
        foreach ($pivotRows as $pivotRow) {
            $sid = $pivotRow[$sourceKey];
            $tid = $pivotRow[$targetKey];
            if (isset($relatedMap[$tid])) {
                $grouped[$sid][] = $relatedMap[$tid];
            }
        }

        // Assign to parents
        $ref = new \ReflectionProperty($entities[0]::class, $meta->propertyName);
        foreach ($entities as $entity) {
            $id = $this->hydrator->getEntityId($entity);
            $ref->setValue($entity, $grouped[$id] ?? []);
        }

        return array_values($relatedMap);
    }

    // ── Batch Helper ────────────────────────────────────────────

    /**
     * Load entities by column value, batched, with identity map tracking.
     *
     * @return array<string|int, object> Keyed by the column value.
     */
    private function batchLoad(string $class, string $table, string $column, array $ids): array
    {
        $map = [];

        // Check identity map first
        $missing = [];
        foreach ($ids as $id) {
            if ($column === 'id' && $this->identityMap->has($class, $id)) {
                $map[$id] = $this->identityMap->get($class, $id);
            } else {
                $missing[] = $id;
            }
        }

        // Fetch missing in batches
        foreach (array_chunk($missing, self::BATCH_SIZE) as $batch) {
            $rows = (new QueryBuilder($this->manager))
                ->from($table)
                ->whereIn($column, array_values($batch))
                ->get();

            foreach ($rows as $row) {
                $entity = $this->hydrateAndTrack($class, $row);
                $map[$row[$column]] = $entity;
            }
        }

        return $map;
    }

    // ── Metadata ────────────────────────────────────────────────

    /**
     * Get relation metadata for an entity class (cached).
     *
     * @return list<RelationMetadata>
     */
    public function getRelationMetadata(string $class): array
    {
        if (isset(self::$metadataCache[$class])) {
            return self::$metadataCache[$class];
        }

        $ref = new ReflectionClass($class);
        $meta = [];

        foreach ($ref->getProperties() as $prop) {
            $propName = $prop->getName();

            // #[ManyToOne]
            $m2oAttrs = $prop->getAttributes(ManyToOne::class);
            if ($m2oAttrs) {
                /** @var ManyToOne $m2o */
                $m2o = $m2oAttrs[0]->newInstance();
                $meta[] = new RelationMetadata(
                    propertyName: $propName,
                    relationType: 'ManyToOne',
                    targetEntity: $m2o->targetEntity,
                    foreignKey: $this->toSnakeCase($propName) . '_id',
                    inversedBy: $m2o->inversedBy,
                );
                continue;
            }

            // #[OneToMany]
            $o2mAttrs = $prop->getAttributes(OneToMany::class);
            if ($o2mAttrs) {
                /** @var OneToMany $o2m */
                $o2m = $o2mAttrs[0]->newInstance();
                // FK is on the child table, named after the mappedBy property
                $meta[] = new RelationMetadata(
                    propertyName: $propName,
                    relationType: 'OneToMany',
                    targetEntity: $o2m->targetEntity,
                    foreignKey: $this->toSnakeCase($o2m->mappedBy) . '_id',
                    mappedBy: $o2m->mappedBy,
                    isCollection: true,
                );
                continue;
            }

            // #[OneToOne]
            $o2oAttrs = $prop->getAttributes(OneToOne::class);
            if ($o2oAttrs) {
                /** @var OneToOne $o2o */
                $o2o = $o2oAttrs[0]->newInstance();
                $meta[] = new RelationMetadata(
                    propertyName: $propName,
                    relationType: 'OneToOne',
                    targetEntity: $o2o->targetEntity,
                    foreignKey: $this->toSnakeCase($propName) . '_id',
                    mappedBy: $o2o->mappedBy,
                );
                continue;
            }

            // #[ManyToMany]
            $m2mAttrs = $prop->getAttributes(ManyToMany::class);
            if ($m2mAttrs) {
                /** @var ManyToMany $m2m */
                $m2m = $m2mAttrs[0]->newInstance();

                $joinTableName = null;
                $joinSourceKey = null;
                $joinTargetKey = null;

                if ($m2m->joinTable !== null) {
                    $joinTableName = $m2m->joinTable->name ?? null;
                    $joinSourceKey = $m2m->joinTable->joinColumn ?? null;
                    $joinTargetKey = $m2m->joinTable->inverseJoinColumn ?? null;
                }

                // Default pivot table name: alphabetical snake_case of both class names
                if ($joinTableName === null) {
                    $names = [
                        $this->toSnakeCase($this->shortClassName($class)),
                        $this->toSnakeCase($this->shortClassName($m2m->targetEntity)),
                    ];
                    sort($names);
                    $joinTableName = implode('_', $names);
                }

                $meta[] = new RelationMetadata(
                    propertyName: $propName,
                    relationType: 'ManyToMany',
                    targetEntity: $m2m->targetEntity,
                    foreignKey: '', // N/A for M2M
                    mappedBy: $m2m->mappedBy,
                    inversedBy: $m2m->inversedBy,
                    joinTable: $joinTableName,
                    joinSourceKey: $joinSourceKey,
                    joinTargetKey: $joinTargetKey,
                    isCollection: true,
                );
            }
        }

        self::$metadataCache[$class] = $meta;
        return $meta;
    }

    // ── Private Helpers ─────────────────────────────────────────

    /**
     * Parse dot-notation relations into a nested tree.
     *
     * Input:  ['user', 'items.product', 'items.tags']
     * Output: ['user' => [], 'items' => ['product' => [], 'tags' => []]]
     *
     * @return array<string, array<string, mixed>>
     */
    private function parseRelations(array $relations): array
    {
        $tree = [];

        foreach ($relations as $relation) {
            $parts = explode('.', $relation, 2);
            $root = $parts[0];

            if (!isset($tree[$root])) {
                $tree[$root] = [];
            }

            if (isset($parts[1])) {
                $nested = $this->parseRelations([$parts[1]]);
                $tree[$root] = array_merge_recursive($tree[$root], $nested);
            }
        }

        return $tree;
    }

    private function findMeta(array $allMeta, string $name): ?RelationMetadata
    {
        foreach ($allMeta as $meta) {
            if ($meta->propertyName === $name) {
                return $meta;
            }
        }
        return null;
    }

    /**
     * Collect entity IDs from a list of entities.
     *
     * @return list<int|string>
     */
    private function collectIds(array $entities): array
    {
        $ids = [];
        foreach ($entities as $entity) {
            $id = $this->hydrator->getEntityId($entity);
            if ($id !== null) {
                $ids[] = $id;
            }
        }
        return $ids;
    }

    /**
     * Hydrate a row and register in the identity map.
     */
    private function hydrateAndTrack(string $class, array $row): object
    {
        if (isset($row['id']) && $this->identityMap->has($class, $row['id'])) {
            return $this->identityMap->get($class, $row['id']);
        }

        $entity = $this->hydrator->hydrate($class, $row);

        if (isset($row['id'])) {
            $this->identityMap->set($class, $row['id'], $entity);
        }

        return $entity;
    }

    /**
     * Determine the FK property name from a relation's foreign key column.
     * e.g., 'order_id' column → search for property matching the ManyToOne mappedBy.
     */
    private function fkPropertyName(RelationMetadata $meta): string
    {
        // For OneToMany, the FK is on the child table, named via mappedBy
        return $meta->mappedBy ?? $this->toCamelCase($meta->foreignKey);
    }

    private function resolveTable(string $class): string
    {
        // Simple heuristic: lowercase plural of short class name
        // In production, this should read #[Entity(table: '...')] attribute
        $ref = new ReflectionClass($class);
        $entityAttrs = $ref->getAttributes(\MonkeysLegion\Entity\Attributes\Entity::class);
        if ($entityAttrs) {
            $entity = $entityAttrs[0]->newInstance();
            if (isset($entity->table) && $entity->table !== '') {
                return $entity->table;
            }
        }

        // Fallback: snake_case + 's'
        return $this->toSnakeCase($this->shortClassName($class)) . 's';
    }

    private function shortClassName(string $fqcn): string
    {
        $pos = strrpos($fqcn, '\\');
        return $pos !== false ? substr($fqcn, $pos + 1) : $fqcn;
    }

    private function toSnakeCase(string $input): string
    {
        static $cache = [];
        return $cache[$input] ??= strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $input));
    }

    private function toCamelCase(string $snakeCase): string
    {
        static $cache = [];
        return $cache[$snakeCase] ??= lcfirst(str_replace('_', '', ucwords($snakeCase, '_')));
    }

    /**
     * Clear metadata cache (for tests).
     */
    public static function clearCache(): void
    {
        self::$metadataCache = [];
    }
}
