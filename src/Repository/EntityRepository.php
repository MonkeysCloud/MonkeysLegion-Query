<?php
declare(strict_types=1);

namespace MonkeysLegion\Repository;

use InvalidArgumentException;
use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Entity\Attributes\OneToOne;
use MonkeysLegion\Entity\Hydrator;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Entity\Attributes\Field;
use ReflectionClass;
use ReflectionProperty;

/**
 * Base repository with common CRUD and query methods.
 *
 * Child classes should define:
 *   protected string $table;
 *   protected string $entityClass;
 */
abstract class EntityRepository
{
    protected string $table;
    protected string $entityClass;
    protected array $columnMap = [];

    public function __construct(public QueryBuilder $qb) {}

    /**
     * Fetch all entities matching optional criteria.
     *
     * @param array<string,mixed> $criteria
     * @param bool $loadRelations Whether to load relationships (default: true)
     * @return object[]
     * @throws \ReflectionException
     */
    public function findAll(array $criteria = [], bool $loadRelations = true): array
    {
        $qb = clone $this->qb;
        $qb->select()->from($this->table);
        $criteria = $this->normalizeCriteria($criteria);
        foreach ($criteria as $column => $value) {
            $qb->andWhere($column, '=', $value);
        }
        $rows = $qb->fetchAll();
        $entities = array_map(
            fn($r) => Hydrator::hydrate($this->entityClass, $r),
            $rows
        );

        if ($loadRelations) {
            foreach ($entities as $entity) {
                $this->loadRelations($entity);
            }
        }

        return $entities;
    }

    /**
     * Find a single entity by primary key.
     *
     * @param int $id The primary key of the entity to find.
     * @param bool $loadRelations Whether to load relationships (default: true)
     * @return object|null The found entity or null if not found.
     * @throws \ReflectionException
     */
    public function find(int|object $idOrEntity, bool $loadRelations = true): ?object
    {
        // ☞ accept entity or id
        if (is_object($idOrEntity)) {
            $rp = new \ReflectionProperty($idOrEntity, 'id');
            $rp->setAccessible(true);
            $id = (int) $rp->getValue($idOrEntity);
        } else {
            $id = (int) $idOrEntity;
        }

        $qb = clone $this->qb;
        $entity = $qb->select()
            ->from($this->table)
            ->where('id', '=', $id)
            ->fetch($this->entityClass);

        if ($entity && $loadRelations) {
            $this->loadRelations($entity);
        }
        return $entity ?: null;
    }

    /**
     * Fetch entities by criteria, with optional ordering and pagination.
     *
     * @param array<string,mixed> $criteria
     * @param array<string,string> $orderBy  column => direction
     * @param bool $loadRelations Whether to load relationships (default: true)
     * @return object[]
     * @throws \ReflectionException
     */
    public function findBy(
        array $criteria,
        array $orderBy = [],
        int|null $limit = null,
        int|null $offset = null,
        bool $loadRelations = true
    ): array {
        $qb = clone $this->qb;
        $qb->select()->from($this->table);

        $criteria = $this->normalizeCriteria($criteria);
        foreach ($criteria as $column => $value) {
            $qb->andWhere($column, '=', $value);
        }
        foreach ($orderBy as $column => $dir) {
            $qb->orderBy($this->normalizeColumn($column), $dir);
        }
        if ($limit !== null)  $qb->limit($limit);
        if ($offset !== null) $qb->offset($offset);

        $entities = $qb->fetchAll($this->entityClass);
        if ($loadRelations) {
            foreach ($entities as $entity) $this->loadRelations($entity);
        }
        return $entities;
    }

    /**
     * Find a single entity by arbitrary criteria.
     *
     * @param array<string,mixed> $criteria
     * @param bool $loadRelations Whether to load relationships (default: true)
     * @return object|null The found entity or null if not found.
     */
    public function findOneBy(array $criteria, bool $loadRelations = true): ?object
    {
        $results = $this->findBy($criteria, [], 1, null, $loadRelations);
        return $results[0] ?? null;
    }

    /**
     * Count entities matching criteria.
     *
     * @param array<string,mixed> $criteria
     * @return int The number of entities matching the criteria.
     * @throws \ReflectionException
     */
    public function count(array $criteria = []): int
    {
        $qb = clone $this->qb;
        $qb->select('COUNT(*) AS count')->from($this->table);

        $criteria = $this->normalizeCriteria($criteria);
        foreach ($criteria as $column => $value) {
            $qb->andWhere($column, '=', $value);
        }
        $row = $qb->fetch();
        return (int)($row?->count ?? 0);
    }

    /**
     * Persist a new or existing entity. Returns insert ID or affected rows.
     *
     * @param object $entity The entity instance to save.
     * @return int The ID of the saved entity or number of affected rows.
     */
    public function save(object $entity): int
    {
        $data = $this->extractPersistableData($entity);

        // ---- Normalize PHP values → DB scalars ---------------------------------
        foreach ($data as $key => $value) {
            if ($value instanceof \DateTimeInterface) {
                $data[$key] = $value->format('Y-m-d H:i:s');
            } elseif (is_bool($value)) {
                $data[$key] = $value ? 1 : 0;
            } elseif (is_float($value)) {
                $data[$key] = (string)$value;            // keep precision for DECIMAL
            } elseif (is_array($value)) {
                $data[$key] = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            }
        }

        $pdo = $this->qb->pdo();   // QueryBuilder should expose the PDO. If your class

        // UPDATE path
        if (!empty($data['id'])) {
            $id = (int)$data['id'];
            unset($data['id']);

            if (empty($data)) {
                return 0; // nothing to update
            }

            // build: UPDATE `table` SET `col` = :col, ... WHERE id = :_id
            $setParts = [];
            foreach ($data as $col => $_) {
                $setParts[] = "`{$col}` = :{$col}";
            }
            $sql = "UPDATE `{$this->table}` SET " . implode(', ', $setParts) . " WHERE `id` = :_id";

            $stmt = $pdo->prepare($sql);

            // bind SET values
            foreach ($data as $col => $val) {
                $stmt->bindValue(":{$col}", $val);
            }
            // bind WHERE id
            $stmt->bindValue(":_id", $id, \PDO::PARAM_INT);

            $stmt->execute();
            return $stmt->rowCount();
        }

        // INSERT path
        // build: INSERT INTO `table` (`c1`,`c2`,...) VALUES (:c1,:c2,...)
        $cols        = array_keys($data);
        $placeholders = array_map(fn($c) => ':' . $c, $cols);

        $sql = sprintf(
            "INSERT INTO `%s` (%s) VALUES (%s)",
            $this->table,
            implode(',', array_map(fn($c) => "`{$c}`", $cols)),
            implode(',', $placeholders)
        );

        $stmt = $pdo->prepare($sql);
        foreach ($data as $col => $val) {
            $stmt->bindValue(":{$col}", $val);
        }
        $stmt->execute();

        $id = (int)$pdo->lastInsertId();

        // reflectively set id if property exists
        if (property_exists($entity, 'id')) {
            $ref = new \ReflectionProperty($entity, 'id');
            $ref->setAccessible(true);
            $ref->setValue($entity, $id);
        }

        return $id;
    }


    /**
     * Delete entity by primary key.
     *
     * @param int|object $idOrEntity The primary key of the entity to delete or entity instance.
     * @return int The number of affected rows (should be 1 for successful delete).
     */
    public function delete(int|object $idOrEntity): int
    {
        // Extract ID from entity or use provided ID
        if (is_object($idOrEntity)) {
            if (!property_exists($idOrEntity, 'id')) {
                throw new InvalidArgumentException('Entity has no id property');
            }
            $rp = new ReflectionProperty($idOrEntity, 'id');
            $rp->setAccessible(true);
            $id = (int) $rp->getValue($idOrEntity);
        } else {
            $id = $idOrEntity;
        }

        if ($id <= 0) {
            throw new InvalidArgumentException('Invalid ID for deletion');
        }

        // Check if entity exists before deletion
        $existing = $this->find($id, false); // Don't load relations for performance
        if (!$existing) {
            return 0;
        }

        try {
            // ① wipe / null related rows first
            $this->cascadeDeleteRelations($id);

            // ② hard-delete the entity with plain PDO
            $pdo = $this->qb->pdo();
            $sql = "DELETE FROM `{$this->table}` WHERE `id` = ? LIMIT 1";
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$id]);
            return $stmt->rowCount();

        } catch (\Throwable $e) {
            throw $e;
        }
    }

    /**
     * Extract persistable data including Field properties and ManyToOne relationships.
     *
     * @param object $entity The entity instance to extract data from.
     * @return array<string, mixed> An associative array of column names and their values.
     * @throws \ReflectionException
     */
    private function extractPersistableData(object $entity): array
    {
        $data = [];
        $ref  = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            // Handle Field attributes
            if ($prop->getAttributes(Field::class)) {
                // Skip uninitialized id on insert
                if ($prop->getName() === 'id' && ! $prop->isInitialized($entity)) {
                    continue;
                }

                if ($prop->isPrivate() || $prop->isProtected()) {
                    $prop->setAccessible(true);
                }

                $data[$prop->getName()] = $prop->getValue($entity);
            }

            // Handle ManyToOne relationships
            $manyToOneAttrs = $prop->getAttributes(ManyToOne::class);
            if ($manyToOneAttrs) {
                if ($prop->isPrivate() || $prop->isProtected()) {
                    $prop->setAccessible(true);
                }

                $relatedEntity = $prop->getValue($entity);
                $columnName = $this->getRelationColumnName($prop->getName());

                if ($relatedEntity === null) {
                    $data[$columnName] = null;
                } else {
                    // Get the ID of the related entity
                    $idProp = new ReflectionProperty($relatedEntity, 'id');
                    $idProp->setAccessible(true);
                    $data[$columnName] = $idProp->getValue($relatedEntity);
                }
            }

            // Handle OneToOne relationships (owning side)
            $oneToOneAttrs = $prop->getAttributes(OneToOne::class);
            if ($oneToOneAttrs) {
                /** @var OneToOne $attr */
                $attr = $oneToOneAttrs[0]->newInstance();

                // Only persist if this is the owning side (no mappedBy)
                if ($attr->mappedBy === null) {
                    if ($prop->isPrivate() || $prop->isProtected()) {
                        $prop->setAccessible(true);
                    }

                    $relatedEntity = $prop->getValue($entity);
                    $columnName = $this->getRelationColumnName($prop->getName());

                    if ($relatedEntity === null) {
                        $data[$columnName] = null;
                    } else {
                        // Get the ID of the related entity
                        $idProp = new ReflectionProperty($relatedEntity, 'id');
                        $idProp->setAccessible(true);
                        $data[$columnName] = $idProp->getValue($relatedEntity);
                    }
                }
            }
        }

        return $data;
    }

    /**
     * Get the database column name for a relation property.
     * Converts camelCase to snake_case and appends _id.
     *
     * @param string $propertyName The property name (e.g., 'media', 'parentCategory')
     * @return string The column name (e.g., 'media_id', 'parent_category_id')
     */
    private function getRelationColumnName(string $propertyName): string
    {
        // Convert camelCase to snake_case
        $snakeCase = strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $propertyName));
        return $snakeCase . '_id';
    }

    /**
     * Extract only properties annotated with #[Field] for persistence,
     * skipping uninitialized id on new entities.
     *
     * @deprecated Use extractPersistableData() instead
     * @param object $entity The entity instance to extract fields from.
     * @return array<string, mixed> An associative array of field names and their values.
     */
    private function extractFields(object $entity): array
    {
        $data = [];
        $ref  = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            // Only include properties marked with #[Field]
            if (! $prop->getAttributes(Field::class)) {
                continue;
            }

            // Skip uninitialized id on insert
            if ($prop->getName() === 'id' && ! $prop->isInitialized($entity)) {
                continue;
            }

            if ($prop->isPrivate() || $prop->isProtected()) {
                $prop->setAccessible(true);
            }

            $data[$prop->getName()] = $prop->getValue($entity);
        }

        return $data;
    }

    /**
     * Find all entities where $relationProp (ManyToMany) contains $relatedId
     *
     * @param string       $relationProp  name of the property on the Entity marked #[ManyToMany]
     * @param int|string   $relatedId     the ID in the inverse table
     * @return object[]                   array of hydrated entities
     * @throws \ReflectionException
     */
    public function findByRelation(string $relationProp, int|string $relatedId): array
    {
        $rclass = new ReflectionClass($this->entityClass);

        if (! $rclass->hasProperty($relationProp)) {
            throw new \InvalidArgumentException("No property $relationProp on {$this->entityClass}");
        }

        $prop  = $rclass->getProperty($relationProp);
        $attrs = $prop->getAttributes(ManyToMany::class);

        if (! $attrs) {
            throw new \InvalidArgumentException("$relationProp is not a ManyToMany relation");
        }

        /** @var ManyToMany $relMeta */
        $relMeta      = $attrs[0]->newInstance();
        $jt           = $relMeta->joinTable;
        $owningSide   = true;

        // 1) if this side has no joinTable, walk to the other side
        if (! $jt) {
            $owningSide = false;
            $otherProp  = $relMeta->mappedBy
                ?? $relMeta->inversedBy
                ?? throw new \InvalidArgumentException(
                    "Relation $relationProp has no joinTable and no mappedBy/inversedBy"
                );

            $targetClass = $relMeta->targetEntity;
            $tclass      = new ReflectionClass($targetClass);

            if (! $tclass->hasProperty($otherProp)) {
                throw new \InvalidArgumentException("No property $otherProp on $targetClass");
            }

            $tprop  = $tclass->getProperty($otherProp);
            $tattrs = $tprop->getAttributes(ManyToMany::class);

            if (! $tattrs) {
                throw new \InvalidArgumentException("$otherProp is not a ManyToMany on $targetClass");
            }

            /** @var ManyToMany $ownerMeta */
            $ownerMeta = $tattrs[0]->newInstance();

            if (! $ownerMeta->joinTable) {
                throw new \InvalidArgumentException(
                    "Neither $relationProp nor $otherProp carry joinTable metadata"
                );
            }

            $jt = $ownerMeta->joinTable;
        }

        // 2) decide which columns to use
        if ($owningSide) {
            // this side declared the joinTable
            $joinCol    = $jt->joinColumn;     // FK → THIS entity ID
            $inverseCol = $jt->inverseColumn;  // FK → RELATED entity ID

            $joinFirst  = "j.$joinCol";
            $joinSecond = "e.id";
            $whereCol   = $inverseCol;
        } else {
            // joinTable came from the OTHER side, so swap
            $joinCol    = $jt->joinColumn;     // FK → OTHER entity ID
            $inverseCol = $jt->inverseColumn;  // FK → THIS entity ID

            $joinFirst  = "j.$inverseCol";
            $joinSecond = "e.id";
            $whereCol   = $joinCol;
        }

        // 3) build & run
        $alias = 'e';
        $qb    = clone $this->qb;

        return $qb
            ->select(["{$alias}.*"])
            ->from($this->table, $alias)
            ->join($jt->name, 'j', $joinFirst, '=', $joinSecond)
            ->where("j.$whereCol", '=', $relatedId)
            ->fetchAll($this->entityClass);
    }

    /**
     * Attach a many-to-many relation.
     *
     * @param object $entity The owning entity instance (must have an ->id).
     * @param string $relationProp The property name on the entity that's #[ManyToMany].
     * @param int|string $value The ID of the related record to attach.
     * @return int                      The number of affected rows (should be 1).
     * @throws \ReflectionException
     */
    public function attachRelation(object $entity, string $relationProp, int|string $value): int
    {
        // [0] = joinTable, [1] = ownCol, [2] = invCol
        [$table, $ownCol, $invCol] = $this->getJoinTableMeta($relationProp);

        // retrieve the ID of this entity
        $ref = new ReflectionProperty($entity::class, 'id');
        $ref->setAccessible(true);
        $ownId = $ref->getValue($entity);

        if (! $ownId) {
            throw new InvalidArgumentException("Entity must have an ID before attaching relations");
        }

        return $this->qb->insert($table, [
            $ownCol => $ownId,
            $invCol => $value,
        ]);
    }

    /**
     * Detach a many-to-many relation.
     *
     * @param object $entity
     * @param string $relationProp
     * @param int|string $value
     * @return int       Rows deleted (usually 1).
     * @throws \ReflectionException
     */
    public function detachRelation(object $entity, string $relationProp, int|string $value): int
    {
        [$table, $ownCol, $invCol] = $this->getJoinTableMeta($relationProp);

        $ref = new ReflectionProperty($entity::class, 'id');
        $ref->setAccessible(true);
        $ownId = $ref->getValue($entity);

        return $this->qb
            ->delete($table)
            ->where($ownCol, '=', $ownId)
            ->andWhere($invCol, '=', $value)
            ->execute();
    }

    /**
     * Internal helper to read #[ManyToMany(..., joinTable: new JoinTable(...))] metadata.
     *
     * @param string $relationProp
     * @return array{0:string,1:string,2:string}  [ join_table, join_column, inverse_column ]
     * @throws \ReflectionException
     */
    private function getJoinTableMeta(string $relationProp): array
    {
        $ownClass = new ReflectionClass($this->entityClass);

        if (! $ownClass->hasProperty($relationProp)) {
            throw new InvalidArgumentException("Property {$relationProp} not found on {$this->entityClass}");
        }

        $prop  = $ownClass->getProperty($relationProp);
        $attrs = $prop->getAttributes(ManyToMany::class);

        if (! $attrs) {
            throw new InvalidArgumentException("{$relationProp} is not a ManyToMany relation");
        }

        /** @var ManyToMany $meta */
        $meta = $attrs[0]->newInstance();
        $jt   = $meta->joinTable;
        $owning = true;

        // if no joinTable here, follow mappedBy/inversedBy to other side
        if (! $jt) {
            $owning = false;
            $otherProp = $meta->mappedBy ?? $meta->inversedBy
                ?? throw new InvalidArgumentException(
                    "Relation {$relationProp} has no joinTable or mappedBy/inversedBy"
                );

            $otherClass = new ReflectionClass($meta->targetEntity);
            if (! $otherClass->hasProperty($otherProp)) {
                throw new InvalidArgumentException("Property {$otherProp} not found on {$meta->targetEntity}");
            }

            $otherAttrs = $otherClass->getProperty($otherProp)
                ->getAttributes(ManyToMany::class);

            if (! $otherAttrs) {
                throw new InvalidArgumentException("{$otherProp} is not a ManyToMany relation on {$meta->targetEntity}");
            }

            /** @var ManyToMany $ownerMeta */
            $ownerMeta = $otherAttrs[0]->newInstance();
            $jt = $ownerMeta->joinTable
                ?? throw new InvalidArgumentException(
                    "Neither {$relationProp} nor {$otherProp} carry joinTable metadata"
                );
        }

        // determine which column belongs to this entity (ownCol) vs. the related (invCol)
        if ($owning) {
            $ownCol = $jt->joinColumn;
            $invCol = $jt->inverseColumn;
        } else {
            // swap columns when using other side's joinTable
            $ownCol = $jt->inverseColumn;
            $invCol = $jt->joinColumn;
        }

        return [$jt->name, $ownCol, $invCol];
    }

    /**
     * Load all relationships for an entity.
     *
     * @param object $entity The entity to load relationships for
     * @throws \ReflectionException
     */
    protected function loadRelations(object $entity): void
    {
        $ref = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            // Handle ManyToOne relationships
            if ($manyToOneAttrs = $prop->getAttributes(ManyToOne::class)) {
                /** @var ManyToOne $attr */
                $attr = $manyToOneAttrs[0]->newInstance();
                $this->loadManyToOne($entity, $prop, $attr);
            }

            // Handle OneToOne relationships (owning side)
            if ($oneToOneAttrs = $prop->getAttributes(OneToOne::class)) {
                /** @var OneToOne $attr */
                $attr = $oneToOneAttrs[0]->newInstance();
                if (!$attr->mappedBy) { // Only load if we're the owning side
                    $this->loadOneToOne($entity, $prop, $attr);
                }
            }

            // Handle OneToMany relationships
            if ($oneToManyAttrs = $prop->getAttributes(OneToMany::class)) {
                /** @var OneToMany $attr */
                $attr = $oneToManyAttrs[0]->newInstance();
                $this->loadOneToMany($entity, $prop, $attr);
            }

            // Handle ManyToMany relationships
            if ($manyToManyAttrs = $prop->getAttributes(ManyToMany::class)) {
                /** @var ManyToMany $attr */
                $attr = $manyToManyAttrs[0]->newInstance();
                $this->loadManyToMany($entity, $prop, $attr);
            }
        }
    }

    /**
     * Load a ManyToOne relationship.
     */
    private function loadManyToOne(object $entity, ReflectionProperty $prop, ManyToOne $attr): void
    {
        $prop->setAccessible(true);

        // Get the foreign key column name
        $fkColumn = $this->getRelationColumnName($prop->getName());

        // Get the entity ID first
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, null);
            return;
        }

        // Query for the FK value
        $qb = clone $this->qb;
        $row = $qb->select([$fkColumn])
            ->from($this->table)
            ->where('id', '=', $entityId)
            ->fetch();

        $fkValue = $row ? ($row->$fkColumn ?? null) : null;

        if ($fkValue === null) {
            $prop->setValue($entity, null);
            return;
        }

        // Load the related entity
        $targetTable = $this->tableOf($attr->targetEntity);
        $qb2 = clone $this->qb;
        $relatedEntity = $qb2->select()
            ->from($targetTable)
            ->where('id', '=', $fkValue)
            ->fetch($attr->targetEntity);

        $prop->setValue($entity, $relatedEntity ?: null);
    }

    /**
     * Load a OneToOne relationship.
     */
    private function loadOneToOne(object $entity, ReflectionProperty $prop, OneToOne $attr): void
    {
        // Same as ManyToOne for owning side
        $this->loadManyToOne($entity, $prop, new ManyToOne(
            targetEntity: $attr->targetEntity,
            inversedBy: $attr->inversedBy,
            nullable: $attr->nullable
        ));
    }

    /**
     * Load a OneToMany relationship.
     */
    private function loadOneToMany(object $entity, ReflectionProperty $prop, OneToMany $attr): void
    {
        $prop->setAccessible(true);
        $entityId = $this->getEntityId($entity);

        if (!$entityId) {
            $prop->setValue($entity, []);
            return;
        }

        // Find the FK column in the target entity
        $targetRef = new ReflectionClass($attr->targetEntity);
        $fkColumn = null;

        // Look for the inverse ManyToOne property
        if ($attr->mappedBy) {
            if ($targetRef->hasProperty($attr->mappedBy)) {
                $fkColumn = $this->getRelationColumnName($attr->mappedBy);
            }
        }

        if (!$fkColumn) {
            $prop->setValue($entity, []);
            return;
        }

        $targetTable = $this->tableOf($attr->targetEntity);
        $qb = clone $this->qb;
        $related = $qb->select()
            ->from($targetTable)
            ->where($fkColumn, '=', $entityId)
            ->fetchAll($attr->targetEntity);

        $prop->setValue($entity, $related);
    }

    /**
     * Load a ManyToMany relationship and hydrate the target entities.
     *
     * @param object              $entity The owner/inverse entity
     * @param ReflectionProperty  $prop   The property carrying #[ManyToMany]
     * @param ManyToMany          $attr   The attribute metadata
     */
    private function loadManyToMany(
        object             $entity,
        ReflectionProperty $prop,
        ManyToMany         $attr
    ): void
    {
        $prop->setAccessible(true);

        // ── If the entity hasn’t been persisted yet, nothing to load ────────────
        $ownId = $this->getEntityId($entity);
        if (!$ownId) {
            $prop->setValue($entity, []);
            return;
        }

        // ── Resolve join-table + the two FK columns this ↔ related ──────────────
        [$jt, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName());

        // ── Target table (snake-case of the related class name) ─────────────────
        $targetTable = $this->tableOf($attr->targetEntity);

        $qb = clone $this->qb;
        $rows = $qb->select(['t.*'])
            ->from($targetTable, 't')
            ->join($jt, 'j', "j.$invCol", '=', 't.id')   // j.user_id = t.id  (or the inverse)
            ->where("j.$ownCol", '=', $ownId)            // j.company_id = {$company->id}
            ->fetchAll($attr->targetEntity);

        $prop->setValue($entity, $rows);
    }

    /**
     * Get entity ID value.
     */
    private function getEntityId(object $entity): ?int
    {
        if (property_exists($entity, 'id')) {
            $idProp = new ReflectionProperty($entity, 'id');
            $idProp->setAccessible(true);
            return $idProp->isInitialized($entity) ? $idProp->getValue($entity) : null;
        }
        return null;
    }

    /**
     * Convert a class name or camelCase string to snake_case.
     *
     * Examples:
     *   App\Entity\User  -> "user"
     *   BlogPost         -> "blog_post"
     *   blogPost         -> "blog_post"
     *
     * @param string $input FQCN or plain string
     * @return string
     * @throws \ReflectionException
     */
    protected function snake(string $input): string
    {
        // If we get a FQCN, keep only the short class name
        if (str_contains($input, '\\')) {
            $input = new \ReflectionClass($input)->getShortName();
        }

        // Insert underscores before capitals, then lowercase
        $snake = strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $input));

        return $snake;
    }

    /**
     * @throws \ReflectionException
     */
    protected function tableOf(string $entityClass): string
    {
        // ① explicit constant on the entity
        if (defined("$entityClass::TABLE")) {
            return $entityClass::TABLE;
        }

        // ② `$table` default on the repository stub
        $repoClass = str_replace('\\Entity\\', '\\Repository\\', $entityClass) . 'Repository';
        if (class_exists($repoClass)) {
            $rc = new \ReflectionClass($repoClass);
            if ($rc->hasProperty('table')) {
                // read the *default* value without building an object
                $defaults = $rc->getDefaultProperties();      // PHP ≥7.4
                if (!empty($defaults['table'])) {
                    return (string) $defaults['table'];
                }
            }
        }

        // ③ fallback rule (lower-case short name)
        return strtolower(new \ReflectionClass($entityClass)->getShortName());
    }

    /**
     * Deletes or nulls every FK that references $id.
     *
     *  • Many-to-Many  → delete rows from the join-table where either column
     *                    equals $id
     *  • One-to-Many   → set the FK on the child records to NULL
     *  • One-to-One    → same — only on the *inverse* side (mappedBy)
     *  • Many-to-One   → no action required (FK sits on the row we're deleting)
     */
    private function cascadeDeleteRelations(int $id): void
    {
        $rc = new \ReflectionClass($this->entityClass);
        $pdo = $this->qb->pdo();

        foreach ($rc->getProperties() as $prop) {
            $propName = $prop->getName();

            /*────────── Many-to-Many ──────────*/
            if ($prop->getAttributes(ManyToMany::class)) {
                try {
                    [$jt, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName());

                    // Use raw PDO for better control
                    $sql = "DELETE FROM `{$jt}` WHERE `{$ownCol}` = ? OR `{$invCol}` = ?";

                    $stmt = $pdo->prepare($sql);
                    $stmt->execute([$id, $id]);
                    $affected = $stmt->rowCount();

                } catch (\Throwable $e) {
                }
            }

            /*────────── One-to-Many ───────────*/
            if ($o2m = $prop->getAttributes(OneToMany::class)) {
                try {
                    /** @var OneToMany $meta */
                    $meta = $o2m[0]->newInstance();
                    if (!$meta->mappedBy) {
                        continue;
                    }

                    $fk  = $this->getRelationColumnName($meta->mappedBy);
                    $tbl = $this->tableOf($meta->targetEntity);

                    // Use raw PDO to set FK to NULL
                    $sql = "UPDATE `{$tbl}` SET `{$fk}` = NULL WHERE `{$fk}` = ?";

                    $stmt = $pdo->prepare($sql);
                    $stmt->execute([$id]);

                } catch (\Throwable $e) {
                    throw new \RuntimeException(
                        "Failed to cascade delete for OneToMany relation '$propName': " . $e->getMessage(),
                        0, $e
                    );
                }
            }

            /*──────── One-to-One (inverse) ─────*/
            if ($o2o = $prop->getAttributes(OneToOne::class)) {
                try {
                    /** @var OneToOne $meta */
                    $meta = $o2o[0]->newInstance();
                    if ($meta->mappedBy) {
                        $fk  = $this->getRelationColumnName($meta->mappedBy);
                        $tbl = $this->tableOf($meta->targetEntity);

                        // Use raw PDO to set FK to NULL
                        $sql = "UPDATE `{$tbl}` SET `{$fk}` = NULL WHERE `{$fk}` = ?";

                        $stmt = $pdo->prepare($sql);
                        $stmt->execute([$id]);
                    } else {
                        error_log("[Repository] Skipping O2O relation '$propName' - owning side");
                    }
                } catch (\Throwable $e) {
                    throw new \RuntimeException(
                        "Failed to cascade delete for OneToOne relation '$propName': " . $e->getMessage(),
                        0, $e
                    );
                }
            }
        }
    }

    /**
     * @param string $key   e.g. 'company' or 'company_id'
     * @return string       DB column to use in WHERE
     * @throws \ReflectionException
     */
    private function normalizeColumn(string $key): string
    {
        // explicit override first
        if (isset($this->columnMap[$key])) {
            return $this->columnMap[$key];
        }

        // already looks like a concrete column
        if (str_ends_with($key, '_id') || str_contains($key, '.')) {
            return $key;
        }

        // if $key is a property on the entity and it's a relation, convert to FK
        $rc = new \ReflectionClass($this->entityClass);
        if ($rc->hasProperty($key)) {
            $prop = $rc->getProperty($key);
            if ($prop->getAttributes(\MonkeysLegion\Entity\Attributes\ManyToOne::class)
                || ($attr = $prop->getAttributes(\MonkeysLegion\Entity\Attributes\OneToOne::class))
                && $attr[0]->newInstance()->mappedBy === null /* owning side */
            ) {
                // Default relation FK rule: snake(prop) + '_id'
                $defaultFk = $this->getRelationColumnName($key);

                // If a map exists for that property, honor it (e.g. 'ipPool' => 'ippool')
                return $this->columnMap[$key] ?? $defaultFk;
            }
        }

        // plain field
        return $key;
    }

    /**
     * Accept entities or scalars in criteria values.
     * @param mixed $value
     * @return mixed
     */
    private function normalizeValue(mixed $value): mixed
    {
        if (is_object($value)) {
            // extract id if present
            if (property_exists($value, 'id')) {
                $rp = new \ReflectionProperty($value, 'id');
                $rp->setAccessible(true);
                return $rp->isInitialized($value) ? $rp->getValue($value) : null;
            }
            // fallback – let PDO try to stringify (not recommended)
            return (string) $value;
        }
        return $value;
    }

    /**
     * Normalize a criteria array (keys & values).
     * @param array<string,mixed> $criteria
     * @return array<string,mixed>
     * @throws \ReflectionException
     */
    private function normalizeCriteria(array $criteria): array
    {
        $out = [];
        foreach ($criteria as $k => $v) {
            $col = $this->normalizeColumn($k);
            $val = $this->normalizeValue($v);
            $out[$col] = $val;
        }
        return $out;
    }

}