<?php

declare(strict_types=1);

namespace MonkeysLegion\Repository;

use InvalidArgumentException;
use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Entity\Attributes\OneToOne;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Entity\Attributes\Field;
use ReflectionClass;
use ReflectionProperty;

class EntityHelper
{
    protected string $table;
    /** @var array<string, string> $tables */
    protected array $tables;
    protected string $entityClass;
    protected array $columnMap = [];
    protected HydrationContext $context;
    public QueryBuilder $qb;

    // Track original values to detect changes
    protected array $originalValues = [];

    // cache: tableName => array<string> columns
    protected array $tableColumnsCache = [];

    protected static array $reservedIdents = [
        'key',
        'keys',
        'group',
        'order',
        'index',
        'primary',
        'constraint',
        'references',
        'table',
        'column',
        'value',
        'values'
    ];

    /**
     * Store original values for change detection
     */
    protected function storeOriginalValues(object $entity): void
    {
        if (!property_exists($entity, 'id')) {
            return;
        }

        $idProp = new ReflectionProperty($entity, 'id');
        $idProp->setAccessible(true);

        if (!$idProp->isInitialized($entity)) {
            return;
        }

        $id = $idProp->getValue($entity);
        if (!$id) {
            return;
        }

        $data = $this->extractPersistableData($entity);
        $this->originalValues[spl_object_id($entity)] = $data;
    }

    /**
     * Get only changed fields for an entity
     */
    protected function getChangedFields(object $entity): array
    {
        $objectId = spl_object_id($entity);

        // If no original values stored, consider all fields as changed
        if (!isset($this->originalValues[$objectId])) {
            return $this->extractPersistableData($entity);
        }

        $currentData = $this->extractPersistableData($entity);
        $originalData = $this->originalValues[$objectId];
        $changedData = [];

        foreach ($currentData as $key => $value) {
            // Always include ID for WHERE clause
            if ($key === 'id') {
                $changedData[$key] = $value;
                continue;
            }

            // Check if value has changed
            $originalValue = $originalData[$key] ?? null;

            // Handle DateTime comparison
            if ($value instanceof \DateTimeInterface && $originalValue instanceof \DateTimeInterface) {
                if ($value->format('Y-m-d H:i:s') !== $originalValue->format('Y-m-d H:i:s')) {
                    $changedData[$key] = $value;
                }
            }
            // Handle array comparison (prevent re-encoding if unchanged)
            elseif (is_array($value) && is_array($originalValue)) {
                if (json_encode($value) !== json_encode($originalValue)) {
                    $changedData[$key] = $value;
                }
            }
            // Standard comparison
            elseif ($value !== $originalValue) {
                $changedData[$key] = $value;
            }
        }

        return $changedData;
    }

    protected function assertRelationsAreSerialized(object $entity, array $data): void
    {
        $ref = new \ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            $attrs = $prop->getAttributes(\MonkeysLegion\Entity\Attributes\ManyToOne::class);
            if (!$attrs) continue;

            $prop->setAccessible(true);
            $val = $prop->getValue($entity);
            if ($val === null) continue; // relation not set → nothing to enforce

            $col = $this->getRelationColumnName($prop->getName());
            if (!array_key_exists($col, $data)) {
                error_log('' . get_class($val) . ' is not a DateTime, checking for ManyToOne');
                throw new \LogicException(
                    "Missing FK column `$col` for relation `{$prop->getName()}` on `{$this->table}`. " .
                        "Did you annotate the property with #[ManyToOne] and map the column correctly?"
                );
            }
            if ($data[$col] === null) {
                error_log('' . get_class($val) . ' is not a DateTime, checking for ManyToOne');
                throw new \LogicException(
                    "FK `$col` is NULL even though relation `{$prop->getName()}` is set. " .
                        "Check that the related entity has an initialized id."
                );
            }
        }
    }

    /**
     * If a payload key isn't an actual column, try to remap it to a column
     * that DOES exist on the table (e.g. listGroup_id → list_group_id).
     */
    protected function remapColumnsToTable(array $data, array $colsInTable): array
    {
        $colsSet = array_flip($colsInTable);
        $out = $data;

        foreach ($data as $key => $val) {
            if (isset($colsSet[$key])) {
                continue; // already a real column
            }

            // 1) camelCase_id → snake_case_id  (e.g. listGroup_id → list_group_id)
            if (str_ends_with($key, '_id') && preg_match('/[A-Z]/', $key)) {
                $base = substr($key, 0, -3); // drop "_id"
                $snake = strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $base)) . '_id';
                if (isset($colsSet[$snake])) {
                    unset($out[$key]);
                    $out[$snake] = $val;
                    continue;
                }
            }

            // 2) fuzzy: remove underscores and compare prefixes of *_id columns
            //    (handles odd naming like list_id, group_id, etc.)
            $needle = preg_replace('/_+/', '', strtolower($key));
            foreach ($colsInTable as $col) {
                if (!str_ends_with($col, '_id')) continue;
                $colKey = preg_replace('/_+/', '', strtolower($col));
                if ($colKey === $needle) {
                    unset($out[$key]);
                    $out[$col] = $val;
                    continue 2;
                }
            }
        }

        return $out;
    }

    /**
     * Extract persistable data including Field properties and ManyToOne relationships.
     *
     * @param object $entity The entity instance to extract data from.
     * @return array<string, mixed> An associative array of column names and their values.
     * @throws \ReflectionException
     */
    protected function extractPersistableData(object $entity): array
    {
        $data = [];
        $ref  = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            if (!$prop->isInitialized($entity)) continue;

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
     * Return list of column names for a table (cached).
     */
    protected function listTableColumns(string $table): array
    {
        if (isset($this->tableColumnsCache[$table])) {
            return $this->tableColumnsCache[$table];
        }

        $pdo = $this->qb->pdo();

        // Prefer information_schema (works across engines and preserves case)
        $sql = "SELECT COLUMN_NAME 
              FROM information_schema.columns 
             WHERE table_schema = DATABASE() 
               AND table_name = :t";
        $stmt = $pdo->prepare($sql);
        if ($stmt->execute([':t' => $table])) {
            $cols = array_map(fn($r) => $r['COLUMN_NAME'], $stmt->fetchAll(\PDO::FETCH_ASSOC));
        } else {
            // Fallback to DESCRIBE
            $stmt = $pdo->query("DESCRIBE `{$table}`");
            $cols = $stmt ? array_map(fn($r) => $r['Field'], $stmt->fetchAll(\PDO::FETCH_ASSOC)) : [];
        }

        $this->tableColumnsCache[$table] = $cols ?: [];
        return $this->tableColumnsCache[$table];
    }

    /**
     * Internal helper to read #[ManyToMany(..., joinTable: new JoinTable(...))] metadata.
     *
     * @param string $relationProp
     * @return array{0:string,1:string,2:string}  [ join_table, join_column, inverse_column ]
     * @throws \ReflectionException
     */
    protected function getJoinTableMeta(string $relationProp, ?object $entity = null): array
    {
        $className = $entity ? get_class($entity) : $this->entityClass;
        $ownClass = new ReflectionClass($className);

        if (! $ownClass->hasProperty($relationProp)) {
            throw new InvalidArgumentException("Property {$relationProp} not found on {$className}");
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
     * Get entity ID value.
     */
    protected function getEntityId(object $entity): ?int
    {
        if (property_exists($entity, 'id')) {
            $idProp = new ReflectionProperty($entity, 'id');
            $idProp->setAccessible(true);
            return $idProp->isInitialized($entity) ? $idProp->getValue($entity) : null;
        }
        return null;
    }

    /**
     * Get entity Property value.
     */
    protected function getEntityPropValue(object $entity, string $propName): mixed
    {
        if (property_exists($entity, $propName)) {
            $prop = new ReflectionProperty($entity, $propName);
            $prop->setAccessible(true);
            return $prop->isInitialized($entity) ? $prop->getValue($entity) : null;
        }
        return null;
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
    protected function cascadeDeleteRelations(int $id): void
    {
        $rc  = new \ReflectionClass($this->entityClass);
        $pdo = $this->qb->pdo();

        foreach ($rc->getProperties() as $prop) {
            $propName = $prop->getName();

            // Many-to-Many: delete both sides where either column matches $id
            if ($prop->getAttributes(ManyToMany::class)) {
                try {
                    [$jt, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName());
                    $sql  = "DELETE FROM `{$jt}` WHERE `{$ownCol}` = ? OR `{$invCol}` = ?";
                    $stmt = $pdo->prepare($sql);
                    $stmt->execute([$id, $id]);
                } catch (\Throwable $e) {
                    // ignore: not fatal for delete
                }
            }

            // One-to-Many: FK sits on TARGET table; set it to NULL where it equals $id
            if ($o2m = $prop->getAttributes(OneToMany::class)) {
                try {
                    /** @var OneToMany $meta */
                    $meta = $o2m[0]->newInstance();
                    if (!$meta->mappedBy) {
                        continue;
                    }
                    $tbl = $this->tableOf($meta->targetEntity);
                    $fk  = $this->getRelationColumnName($meta->mappedBy, $tbl);

                    $sql  = "UPDATE `{$tbl}` SET `{$fk}` = NULL WHERE `{$fk}` = ?";
                    $stmt = $pdo->prepare($sql);
                    $stmt->execute([$id]);
                } catch (\Throwable $e) {
                    throw new \RuntimeException(
                        "Failed to cascade delete for OneToMany relation '$propName': " . $e->getMessage(),
                        0,
                        $e
                    );
                }
            }

            // One-to-One (inverse): FK sits on TARGET table (inverse side)
            if ($o2o = $prop->getAttributes(OneToOne::class)) {
                try {
                    /** @var OneToOne $meta */
                    $meta = $o2o[0]->newInstance();
                    if ($meta->mappedBy) {
                        $tbl = $this->tableOf($meta->targetEntity);
                        $fk  = $this->getRelationColumnName($meta->mappedBy, $tbl);

                        $sql  = "UPDATE `{$tbl}` SET `{$fk}` = NULL WHERE `{$fk}` = ?";
                        $stmt = $pdo->prepare($sql);
                        $stmt->execute([$id]);
                    } else {
                        // owning side: FK on THIS table, nothing to null here for a delete of THIS row
                        // (deleting THIS row automatically removes the owning-side FK)
                    }
                } catch (\Throwable $e) {
                    throw new \RuntimeException(
                        "Failed to cascade delete for OneToOne relation '$propName': " . $e->getMessage(),
                        0,
                        $e
                    );
                }
            }
        }
    }

    /**
     * Resolve the FK column name for a relation property against an optional table.
     * Strategy:
     *  1) explicit map → return
     *  2) build variants from the property name (camelCase, acronyms, Id suffix)
     *  3) check exact matches against table columns (if available)
     *  4) fuzzy match: underscore-insensitive prefix match on *_id columns
     *  5) fallback to snake_case + '_id'
     */
    protected function getRelationColumnName(string $propertyName, ?string $table = null): string
    {
        // 0) explicit mapping wins
        if (isset($this->columnMap[$propertyName])) {
            return $this->columnMap[$propertyName];
        }

        // Base: strip trailing Id/_id on the *property* (common on inverse sides)
        $prop = $propertyName;
        if (str_ends_with($prop, '_id')) {
            $prop = substr($prop, 0, -3);
        } elseif (preg_match('/Id$/', $prop)) {
            $prop = substr($prop, 0, -2);
        }

        // Tokenize camelCase, keeping acronym runs together (e.g. IP, API, UUID)
        $tokens = preg_split('/(?<=[a-z0-9])(?=[A-Z])|(?<=[A-Z])(?=[A-Z][a-z])/', $prop) ?: [$prop];

        // Normalize tokens to lowercase (DB cols are usually lowercase)
        $ltokens = array_map(fn($t) => strtolower($t), $tokens);

        // Candidate builders
        $snake     = implode('_', $ltokens);              // ip_pool
        $noUnders  = implode('', $ltokens);               // ippool
        $camelLike = $propertyName . '_id';               // listGroup_id (DB uses camel + _id)
        $camelId   = $propertyName . 'Id';                // companyId (DB uses camel + Id)

        // Heuristic: merge leading short acronym-ish token with next (ip+pool → ippool)
        $merged = $ltokens;
        if (count($ltokens) >= 2 && strlen($ltokens[0]) <= 3) {
            $merged = [$ltokens[0] . $ltokens[1], ...array_slice($ltokens, 2)];
        }
        $mergedSnake    = implode('_', $merged);          // ippool
        $mergedNoUnders = implode('', $merged);           // ippool

        // Full candidate list (ordered by likelihood)
        $candidates = array_values(array_unique([
            $snake . '_id',           // ip_pool_id
            $noUnders . '_id',        // ippool_id   (handles your ipPool → ippool_id)
            $mergedSnake . '_id',     // ippool_id   (duplicate-safe due to unique())
            $mergedNoUnders . '_id',  // ippool_id
            $camelLike,               // listGroup_id
            $camelId,                 // companyId
            $prop . '_id',            // company_id (if prop was "company" after stripping)
            // rare but cheap:
            $snake . 'id',            // ip_poolid
            $noUnders . 'id',         // ippoolid
        ]));

        // If we can inspect the table, try to find an exact match
        $table ??= $this->table;
        if ($table) {
            $cols = $this->listTableColumns($table);          // exact case from information_schema when possible
            if ($cols) {
                // 1) exact match
                foreach ($candidates as $cand) {
                    if (in_array($cand, $cols, true)) {
                        return $this->columnMap[$propertyName] = $cand;
                    }
                }

                // 2) fuzzy: pick a column that ends with _id and whose prefix,
                //    after removing underscores, equals our property base (also underscore-free)
                $targetKey = preg_replace('/_+/', '', $noUnders); // 'ippool'
                $best = null;

                foreach ($cols as $col) {
                    if (!str_ends_with($col, '_id')) {
                        continue;
                    }
                    $prefix = substr($col, 0, -3);                 // drop '_id'
                    $prefixKey = preg_replace('/_+/', '', strtolower($prefix));
                    if ($prefixKey === $targetKey) {
                        $best = $col;
                        break;
                    }
                }

                if ($best !== null) {
                    return $this->columnMap[$propertyName] = $best;
                }
            }
        }

        // Fallback
        return $this->columnMap[$propertyName] = $snake . '_id';
    }

    /**
     * @param string $key   e.g. 'company' or 'company_id'
     * @return string       DB column to use in WHERE
     * @throws \ReflectionException
     */
    protected function normalizeColumn(string $key): string
    {
        // explicit override first
        if (isset($this->columnMap[$key])) {
            return $this->quoteIfReserved($this->columnMap[$key]);
        }

        // already concrete column form? still ensure quoting if reserved
        if (str_contains($key, '.')) {
            // handle alias.column (quote the column part if needed)
            [$left, $right] = explode('.', $key, 2);
            return $left . '.' . $this->quoteIfReserved($right);
        }

        if (str_ends_with($key, '_id')) {
            return $this->quoteIfReserved($key);
        }

        // if $key is a property on the entity and it's a relation, convert to FK
        $rc = new \ReflectionClass($this->entityClass);
        if ($rc->hasProperty($key)) {
            $prop = $rc->getProperty($key);

            $isOwningOneToOne = false;
            if ($oneToOneAttrs = $prop->getAttributes(\MonkeysLegion\Entity\Attributes\OneToOne::class)) {
                $isOwningOneToOne = ($oneToOneAttrs[0]->newInstance()->mappedBy === null);
            }

            if ($prop->getAttributes(\MonkeysLegion\Entity\Attributes\ManyToOne::class) || $isOwningOneToOne) {
                $fk = $this->getRelationColumnName($key);
                // respect explicit map for that property if present
                $fk = $this->columnMap[$key] ?? $fk;
                return $this->quoteIfReserved($fk);
            }
        }

        // plain field on this table
        return $this->quoteIfReserved($key);
    }

    protected function quoteIfReserved(string $ident): string
    {
        // already quoted or an expression — leave as-is
        if ($ident === '' || str_contains($ident, '`') || strpbrk($ident, " \t\n\r()")) {
            return $ident;
        }

        // if alias.column sneaks in here, quote the right-hand side only
        if (str_contains($ident, '.')) {
            [$l, $r] = explode('.', $ident, 2);
            return $l . '.' . $this->quoteIfReserved($r);
        }

        // simple identifier: quote if in reserved set
        if (in_array(strtolower($ident), self::$reservedIdents, true)) {
            return '`' . $ident . '`';
        }
        return $ident;
    }

    /**
     * Accept entities or scalars in criteria values.
     * @param mixed $value
     * @return mixed
     */
    protected function normalizeValue(mixed $value): mixed
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
    protected function normalizeCriteria(array $criteria): array
    {
        $out = [];
        foreach ($criteria as $k => $v) {
            $col = $this->normalizeColumn($k);
            $val = $this->normalizeValue($v);
            $out[$col] = $val;
        }
        return $out;
    }

    /**
     * Normalize a foreign key value to string|null.
     */
    protected function normalizeFk(mixed $fkValue): ?string
    {
        if ($fkValue === '' || $fkValue === 0 || $fkValue === '0') {
            return null;
        }

        if (is_int($fkValue) || (is_string($fkValue) && ctype_digit($fkValue))) {
            return (string) $fkValue;
        }

        return null;
    }

    protected function getRowValue(object|false $row, string $column): mixed
    {
        if (!$row || !property_exists($row, $column)) {
            return null;
        }

        $rp = new \ReflectionProperty($row, $column);
        $rp->setAccessible(true);
        return $rp->isInitialized($row) ? $rp->getValue($row) : null;
    }
}
