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

    // Track original values to detect changes
    private array $originalValues = [];

    // cache: tableName => array<string> columns
    private array $tableColumnsCache = [];

    private static array $reservedIdents = [
        'key','keys','group','order','index','primary','constraint','references',
        'table','column','value','values'
    ];

    public function __construct(public QueryBuilder $qb) {}

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

        if ($entity) {
            // Store original values for change detection
            $this->storeOriginalValues($entity);

            if ($loadRelations) {
                $this->loadRelations($entity);
            }
        }

        return $entity ?: null;
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
        if (!empty($results)) {
            $this->storeOriginalValues($results[0]);
        }
        return $results[0] ?? null;
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

        // Track loaded entities to prevent circular loading
        $loadedEntities = [];

        foreach ($entities as $entity) {
            $this->storeOriginalValues($entity);

            if ($loadRelations) {
                $entityId = $this->getEntityId($entity);
                $entityKey = get_class($entity) . ':' . $entityId;

                // Mark this entity as being loaded
                $loadedEntities[$entityKey] = true;

                // Load relations with protection against circular references
                $this->loadRelationsWithGuard($entity, $loadedEntities);
            }
        }

        return $entities;
    }

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

        foreach ($entities as $entity) {
            $this->storeOriginalValues($entity);
            if ($loadRelations) {
                $this->loadRelations($entity);
            }
        }

        return $entities;
    }

    /**
     * Store original values for change detection
     */
    private function storeOriginalValues(object $entity): void
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
    private function getChangedFields(object $entity): array
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

    private function assertRelationsAreSerialized(object $entity, array $data): void
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
    private function remapColumnsToTable(array $data, array $colsInTable): array
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
     * Persist a new or existing entity. Returns insert ID or affected rows.
     *
     * @param object $entity The entity instance to save.
     * @param bool $partial If true, only update changed fields (default: true for updates)
     * @return int The ID of the saved entity or number of affected rows.
     */
    public function save(object $entity, bool $partial = true, bool $upsert = false): int
    {
        // ——— decide update vs insert
        $isUpdate = false;
        if (property_exists($entity, 'id')) {
            $idProp = new \ReflectionProperty($entity, 'id');
            $idProp->setAccessible(true);
            if ($idProp->isInitialized($entity) && $idProp->getValue($entity)) $isUpdate = true;
        }

        // ——— collect data
        $data = ($isUpdate && $partial) ? $this->getChangedFields($entity) : $this->extractPersistableData($entity);

        // ——— normalize scalars
        foreach ($data as $key => $value) {
            if     ($value instanceof \DateTimeInterface) $data[$key] = $value->format('Y-m-d H:i:s');
            elseif (is_bool($value))                      $data[$key] = $value ? 1 : 0;
            elseif (is_float($value))                     $data[$key] = (string)$value;
            elseif (is_array($value))                     $data[$key] = json_encode($value, JSON_UNESCAPED_UNICODE|JSON_UNESCAPED_SLASHES);
            elseif (is_object($value))                    throw new \InvalidArgumentException("Column `$key` holds object ".get_class($value));
        }

        // ——— table columns
        $colsInTable = $this->listTableColumns($this->table);

        // Derive FK column names from entity attributes (fallback if FK metadata is absent)
        $fkColsFromProps = [];
        try {
            $ref = new \ReflectionClass($entity);
            foreach ($ref->getProperties() as $prop) {
                $attrs = $prop->getAttributes(\MonkeysLegion\Entity\Attributes\ManyToOne::class);
                if (!$attrs) continue;
                try { $fkColsFromProps[] = $this->getRelationColumnName($prop->getName()); } catch (\Throwable $ignored) {}
            }
        } catch (\Throwable $ignored) {}

        // ——— remap odd keys to real columns
        $data = $this->remapColumnsToTable($data, $colsInTable);

        // ——— validate columns exist
        $unknown = array_diff(array_keys($data), $colsInTable);
        if ($unknown) {
            throw new \RuntimeException("Unknown columns for `{$this->table}`: ".implode(', ', $unknown));
        }

        // ——— DB handle
        $pdo = $this->qb->pdo();
        if ($pdo->getAttribute(\PDO::ATTR_ERRMODE) !== \PDO::ERRMODE_EXCEPTION) {
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        }
        $schema = (string)$pdo->query('SELECT DATABASE()')->fetchColumn();

        // STATIC caches
        static $___fkCache = [];
        static $___uniqueCache = [];
        static $___tablesCache = [];

        // Load all table names in schema once
        $loadTables = function(string $schema) use ($pdo, &$___tablesCache): array {
            if (isset($___tablesCache[$schema])) return $___tablesCache[$schema];
            $st = $pdo->prepare("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :s");
            $st->execute([':s' => $schema]);
            return $___tablesCache[$schema] = array_map(fn($r) => $r['TABLE_NAME'], $st->fetchAll(\PDO::FETCH_ASSOC));
        };

        // Resolve actual table name (handles underscores→none, case-insensitive)
        $resolveTable = function(string $schema, string $name) use ($loadTables): ?string {
            $tables = $loadTables($schema);
            $map = [];
            foreach ($tables as $t) { $map[strtolower($t)] = $t; }
            $needle = strtolower($name);
            if (isset($map[$needle])) return $map[$needle];
            $needleNoUnderscore = str_replace('_','', $needle);
            foreach ($map as $low => $orig) {
                if (str_replace('_','',$low) === $needleNoUnderscore) return $orig;
            }
            return null;
        };

        // FKs: COLUMN_NAME => ['ref_schema','ref_table','ref_col','constraint_name']
        $loadFks = function(string $schema, string $table) use ($pdo, &$___fkCache): array {
            $key = "{$schema}.{$table}";
            if (isset($___fkCache[$key])) return $___fkCache[$key];
            $sql = "SELECT COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                  FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                 WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t AND REFERENCED_TABLE_NAME IS NOT NULL";
            $st  = $pdo->prepare($sql);
            $st->execute([':s' => $schema, ':t' => $table]);
            $out = [];
            foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                $out[$r['COLUMN_NAME']] = [
                    'constraint_name' => $r['CONSTRAINT_NAME'],
                    'ref_schema'      => $r['REFERENCED_TABLE_SCHEMA'] ?: $schema,
                    'ref_table'       => $r['REFERENCED_TABLE_NAME'],
                    'ref_col'         => $r['REFERENCED_COLUMN_NAME'] ?: 'id',
                ];
            }
            return $___fkCache[$key] = $out;
        };

        // Unique indexes (incl. PRIMARY)
        $loadUniqueIndexes = function(string $schema, string $table) use ($pdo, &$___uniqueCache): array {
            $key = "{$schema}.{$table}";
            if (isset($___uniqueCache[$key])) return $___uniqueCache[$key];
            $sql = "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX
                  FROM INFORMATION_SCHEMA.STATISTICS
                 WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t
              ORDER BY INDEX_NAME, SEQ_IN_INDEX";
            $st  = $pdo->prepare($sql);
            $st->execute([':s' => $schema, ':t' => $table]);
            $idx = [];
            foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                if ((int)$r['NON_UNIQUE'] !== 0) continue;
                $name = $r['INDEX_NAME'];
                if (!isset($idx[$name])) $idx[$name] = ['name' => $name, 'columns' => [], 'is_primary' => ($name === 'PRIMARY')];
                $idx[$name]['columns'][] = $r['COLUMN_NAME'];
            }
            return $___uniqueCache[$key] = array_values($idx);
        };

        // Select id by composite key
        $selectIdByKey = function(array $key) {
            $q = (clone $this->qb)->select(['id'])->from($this->table);
            if (method_exists($q,'useWrite')) $q->useWrite();
            $first = true;
            foreach ($key as $c => $v) {
                if ($first) { $q->where($c,'=',$v); $first=false; }
                else        { $q->andWhere($c,'=',$v); }
            }
            return $q->fetch();
        };

        // Self-heal FK (when 1452 is thrown due to wrong referenced table name)
        $attemptRepairFk = function(string $schema, string $table, array $fkMap) use ($pdo, $resolveTable): bool {
            foreach ($fkMap as $col => $meta) {
                $refSchema   = $meta['ref_schema'] ?: $schema;
                $refTableOrg = $meta['ref_table'] ?? '';
                $refCol      = $meta['ref_col'] ?? 'id';
                $cname       = $meta['constraint_name'] ?? null;
                if (!$refTableOrg || !$cname) continue;

                $resolved = $resolveTable($refSchema, $refTableOrg);
                if (!$resolved || strcasecmp($resolved, $refTableOrg) === 0) continue;

                // verify resolved table really exists
                $chk = $pdo->prepare("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t");
                $chk->execute([':s'=>$refSchema, ':t'=>$resolved]);
                if (!$chk->fetchColumn()) continue;

                try {
                    $pdo->exec("ALTER TABLE `{$table}` DROP FOREIGN KEY `{$cname}`");
                    $pdo->exec("ALTER TABLE `{$table}` ADD CONSTRAINT `{$cname}` FOREIGN KEY (`{$col}`) REFERENCES `{$resolved}` (`{$refCol}`) ON DELETE SET NULL");
                    return true;
                } catch (\Throwable $e) {
                    // ignore and continue
                }
            }
            return false;
        };

        // Build metadata
        $fkMap     = $loadFks($schema, $this->table);
        $uniqueIdx = $loadUniqueIndexes($schema, $this->table);

        // Choose a natural key (prefer real UNIQUEs, de-prioritize FK-only)
        $dataKeys = array_keys($data);
        $candidateUnique = [];
        foreach ($uniqueIdx as $idx) {
            $cols = $idx['columns'];
            $isOnlyId = (count($cols) === 1 && $cols[0]==='id');
            if ($idx['is_primary'] && $isOnlyId) continue;
            if (count(array_diff($cols, $dataKeys)) === 0) {
                $fkOnly = count(array_diff($cols, array_keys($fkMap))) === 0;
                $bonus = 0;
                foreach ($cols as $c) {
                    if (stripos($c,'email') !== false || stripos($c,'uuid') !== false || stripos($c,'external') !== false) { $bonus += 2; break; }
                }
                $candidateUnique[] = ['name'=>$idx['name'],'cols'=>$cols,'fkOnly'=>$fkOnly,'arity'=>count($cols),'bonus'=>$bonus];
            }
        }
        usort($candidateUnique, function($a,$b){
            if ($a['fkOnly'] !== $b['fkOnly']) return $a['fkOnly'] ? 1 : -1; // prefer non-FK-only
            if ($a['arity']  !== $b['arity'])  return $b['arity'] <=> $a['arity'];
            if ($a['bonus']  !== $b['bonus'])  return $b['bonus'] <=> $a['bonus'];
            return count($a['cols']) <=> count($b['cols']);
        });
        $chosenUnique   = $candidateUnique[0] ?? null;
        $naturalKeyCols = $chosenUnique['cols'] ?? [];

        // Try to match existing row by natural key
        if (!$isUpdate && $naturalKeyCols) {
            $keyMap = []; foreach ($naturalKeyCols as $c) $keyMap[$c] = $data[$c];
            $existing = $selectIdByKey($keyMap);
            if ($existing && isset($existing->id)) {
                $isUpdate = true; $data['id'] = (int)$existing->id;
            }
        }

        // ——— UPDATE path
        if (!empty($data['id'])) {
            $id = (int)$data['id']; unset($data['id']);
            if (empty($data)) return 0;
            $setParts = []; foreach ($data as $col => $_) $setParts[] = "`{$col}` = :{$col}";
            $sql = "UPDATE `{$this->table}` SET ".implode(', ', $setParts)." WHERE `id` = :_id";
            try {
                $stmt = $pdo->prepare($sql);
                foreach ($data as $col => $val) { $stmt->bindValue(":{$col}", $val); }
                $stmt->bindValue(":_id", $id, \PDO::PARAM_INT);
                $stmt->execute();
                $this->storeOriginalValues($entity);
                return $stmt->rowCount();
            } catch (\PDOException $e) { throw $e; }
        }

        // ——— INSERT / UPSERT
        if (empty($data)) throw new \LogicException("No data to INSERT for `{$this->table}` and entity ".get_class($entity));

        // Soft FK preflight with resolution (log-only previously; now silent — DB enforces truth)
        $fkCols = array_keys($fkMap);
        foreach ($fkColsFromProps as $fkPropCol) if (!in_array($fkPropCol, $fkCols, true)) $fkCols[] = $fkPropCol;
        foreach ($fkCols as $col) {
            if (!array_key_exists($col, $data)) continue;
            try {
                $meta = $fkMap[$col] ?? ['ref_schema'=>$schema, 'ref_table'=>preg_replace('/_id$/','',$col), 'ref_col'=>'id'];
                $refSchema = $meta['ref_schema'] ?? $schema;
                $refTableOrig = $meta['ref_table'] ?? preg_replace('/_id$/','',$col);
                $refTable = $resolveTable($refSchema, $refTableOrig) ?: $refTableOrig;
                $refCol   = $meta['ref_col'] ?? 'id';

                $q = (clone $this->qb)->select([$refCol])->from($refTable)->where($refCol, '=', $data[$col]);
                if (method_exists($q,'useWrite')) $q->useWrite();
                $q->fetch(); // ignore result; DB will enforce for real
            } catch (\Throwable $fkEx) {
                // soft-skip any preflight errors
            }
        }

        $cols         = array_keys($data);
        $placeholders = array_map(fn($c) => ':' . $c, $cols);

        $sql = sprintf(
            "INSERT INTO `%s` (%s) VALUES (%s)",
            $this->table,
            implode(',', array_map(fn($c) => "`{$c}`", $cols)),
            implode(',', $placeholders)
        );

        if ($chosenUnique && !$upsert) { $upsert = true; }
        if ($upsert && $chosenUnique) {
            $sql .= " ON DUPLICATE KEY UPDATE `id` = LAST_INSERT_ID(`id`)";
            if (in_array('subscribed_at', $cols, true)) $sql .= ", `subscribed_at` = VALUES(`subscribed_at`)";
        }

        $id = 0;
        $retriedAfterRepair = false;

        while (true) {
            try {
                $stmt = $pdo->prepare($sql);
                foreach ($data as $col => $val) { $stmt->bindValue(":{$col}", $val); }
                $stmt->execute();
                $id = (int)$pdo->lastInsertId();
                break;
            } catch (\PDOException $e) {
                $mysqlCode = isset($e->errorInfo[1]) ? (int)$e->errorInfo[1] : 0;

                // Duplicate → update by natural key
                if ($mysqlCode === 1062 && $naturalKeyCols) {
                    $keyMap = []; foreach ($naturalKeyCols as $c) $keyMap[$c] = $data[$c];
                    $row = $selectIdByKey($keyMap);
                    if ($row && isset($row->id)) {
                        $existingId = (int)$row->id;
                        $setParts = []; foreach ($data as $col => $_) $setParts[] = "`{$col}` = :{$col}";
                        $updateSql = "UPDATE `{$this->table}` SET ".implode(', ', $setParts)." WHERE `id` = :_id";
                        $stmt = $pdo->prepare($updateSql);
                        foreach ($data as $col => $val) { $stmt->bindValue(":{$col}", $val); }
                        $stmt->bindValue(":_id", $existingId, \PDO::PARAM_INT);
                        $stmt->execute();

                        if (property_exists($entity, 'id')) {
                            $rp = new \ReflectionProperty($entity, 'id'); $rp->setAccessible(true);
                            $rp->setValue($entity, $existingId);
                        }
                        $this->storeOriginalValues($entity);
                        return $existingId;
                    }
                }

                // FK wrong referenced table: try FK repair once, then retry
                if ($mysqlCode === 1452 && !$retriedAfterRepair) {
                    if ($attemptRepairFk($schema, $this->table, $fkMap)) {
                        $retriedAfterRepair = true;
                        continue; // retry INSERT
                    }
                }

                throw $e; // not handled
            }
        }

        // ——— VERIFY
        $verifyRow = null;
        if ($naturalKeyCols) {
            $keyMap = []; foreach ($naturalKeyCols as $c) $keyMap[$c] = $data[$c];
            try {
                $v = (clone $this->qb);
                if (method_exists($v,'useWrite')) $v->useWrite();
                $verify = $v->select(['id'])->from($this->table)->where(array_key_first($keyMap), '=', reset($keyMap));
                $first = true;
                foreach ($keyMap as $k => $vv) { if ($first) { $first=false; continue; } $verify->andWhere($k,'=',$vv); }
                $verifyRow = $verify->fetch();
            } catch (\Exception $ex) { /* ignore */ }
        } elseif ($id > 0) {
            try {
                $v = (clone $this->qb);
                if (method_exists($v,'useWrite')) $v->useWrite();
                $verifyRow = $v->select(['id'])->from($this->table)->where('id','=',$id)->fetch();
            } catch (\Exception $ex) { /* ignore */ }
        }

        if (!$verifyRow && $id <= 0) {
            throw new \RuntimeException("INSERT appeared to succeed but row not visible on same connection.");
        }

        if (property_exists($entity, 'id')) {
            $ref = new \ReflectionProperty($entity, 'id'); $ref->setAccessible(true);
            $ref->setValue($entity, (int)($verifyRow->id ?? $id));
        }

        $this->storeOriginalValues($entity);
        return (int)($verifyRow->id ?? $id);
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

            // Clean up stored original values
            unset($this->originalValues[spl_object_id($idOrEntity)]);
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
     * Return list of column names for a table (cached).
     */
    private function listTableColumns(string $table): array
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

        $entities = $qb
            ->select(["{$alias}.*"])
            ->from($this->table, $alias)
            ->join($jt->name, 'j', $joinFirst, '=', $joinSecond)
            ->where("j.$whereCol", '=', $relatedId)
            ->fetchAll($this->entityClass);

        foreach ($entities as $entity) {
            $this->storeOriginalValues($entity);
        }

        return $entities;
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
     * @param array $loadedClasses Track which entity classes have been loaded to prevent infinite recursion
     * @throws \ReflectionException
     */
    protected function loadRelations(object $entity, array $loadedClasses = []): void
    {
        $ref = new ReflectionClass($entity);

        // Track this entity class to prevent circular loading
        $entityClass = get_class($entity);
        $loadedClasses[$entityClass] = true;

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
                $this->loadManyToManyWithGuard($entity, $prop, $attr, $loadedClasses);
            }
        }
    }

    /**
     * Load relations with guard against circular references
     *
     * @param object $entity
     * @param array $loadedEntities Already loaded entities in this query
     */
    protected function loadRelationsWithGuard(object $entity, array &$loadedEntities = []): void
    {
        $ref = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            try {
                // Handle ManyToOne relationships
                if ($manyToOneAttrs = $prop->getAttributes(ManyToOne::class)) {
                    /** @var ManyToOne $attr */
                    $attr = $manyToOneAttrs[0]->newInstance();
                    $this->loadManyToOneWithGuard($entity, $prop, $attr, $loadedEntities);
                }

                // Handle OneToOne relationships (owning side)
                if ($oneToOneAttrs = $prop->getAttributes(OneToOne::class)) {
                    /** @var OneToOne $attr */
                    $attr = $oneToOneAttrs[0]->newInstance();
                    if (!$attr->mappedBy) { // Only load if we're the owning side
                        $this->loadOneToOneWithGuard($entity, $prop, $attr, $loadedEntities);
                    }
                }

                // Handle OneToMany relationships
                if ($oneToManyAttrs = $prop->getAttributes(OneToMany::class)) {
                    /** @var OneToMany $attr */
                    $attr = $oneToManyAttrs[0]->newInstance();
                    $this->loadOneToManyWithGuard($entity, $prop, $attr, $loadedEntities);
                }

                // Handle ManyToMany relationships
                if ($manyToManyAttrs = $prop->getAttributes(ManyToMany::class)) {
                    /** @var ManyToMany $attr */
                    $attr = $manyToManyAttrs[0]->newInstance();
                    $this->loadManyToManyWithGuard($entity, $prop, $attr, $loadedEntities);
                }
            } catch (\Exception $e) {
                // Log error but don't fail the entire load
                error_log("Failed to load relation {$prop->getName()}: " . $e->getMessage());
                $prop->setAccessible(true);
                // Set empty value for failed relations
                if ($oneToManyAttrs || $manyToManyAttrs) {
                    $prop->setValue($entity, []);
                } else {
                    $prop->setValue($entity, null);
                }
            }
        }
    }

    /**
     * Load ManyToOne with guard
     */
    private function loadManyToOneWithGuard(object $entity, ReflectionProperty $prop, ManyToOne $attr, array &$loadedEntities): void
    {
        $prop->setAccessible(true);

        // FK column lives on THIS table
        $fkColumn = $this->getRelationColumnName($prop->getName());

        // Get this entity's id
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, null);
            return;
        }

        // Fetch FK value
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

        // Avoid circular re-load
        $relatedKey = $attr->targetEntity . ':' . $fkValue;
        if (isset($loadedEntities[$relatedKey])) {
            $prop->setValue($entity, null);
            return;
        }

        // Load related entity by its table/id
        $targetTable = $this->tableOf($attr->targetEntity);
        $qb2 = clone $this->qb;
        $relatedEntity = $qb2->select()
            ->from($targetTable)
            ->where('id', '=', $fkValue)
            ->fetch($attr->targetEntity);

        $prop->setValue($entity, $relatedEntity ?: null);
    }


    /**
     * Load OneToOne with guard
     */
    private function loadOneToOneWithGuard(object $entity, ReflectionProperty $prop, OneToOne $attr, array &$loadedEntities): void
    {
        // Same as ManyToOne for owning side
        $this->loadManyToOneWithGuard($entity, $prop, new ManyToOne(
            targetEntity: $attr->targetEntity,
            inversedBy: $attr->inversedBy,
            nullable: $attr->nullable
        ), $loadedEntities);
    }

    /**
     * Load OneToMany with guard
     */
    private function loadOneToManyWithGuard(object $entity, ReflectionProperty $prop, OneToMany $attr, array &$loadedEntities): void
    {
        $prop->setAccessible(true);
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, []);
            return;
        }

        $targetTable = $this->tableOf($attr->targetEntity);

        // Determine FK on the TARGET table using mappedBy
        if (!$attr->mappedBy) {
            $prop->setValue($entity, []);
            return;
        }
        $fkColumn = $this->getRelationColumnName($attr->mappedBy, $targetTable);

        $qb = clone $this->qb;
        $related = $qb->select()
            ->from($targetTable)
            ->where($fkColumn, '=', $entityId)
            ->fetchAll($attr->targetEntity);

        $prop->setValue($entity, $related);
    }


    /**
     * Load ManyToMany with guard
     */
    private function loadManyToManyWithGuard(
        object             $entity,
        ReflectionProperty $prop,
        ManyToMany         $attr,
        array              &$loadedEntities
    ): void {
        $prop->setAccessible(true);

        $ownId = $this->getEntityId($entity);
        if (!$ownId) {
            $prop->setValue($entity, []);
            return;
        }

        try {
            [$jt, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName());
        } catch (\Exception $e) {
            $prop->setValue($entity, []);
            return;
        }

        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);

        $qb = clone $this->qb;
        $rows = $qb->select(['t.*'])
            ->from($targetTable, 't')
            ->join($jt, 'j', "j.$invCol", '=', 't.id')
            ->where("j.$ownCol", '=', $ownId)
            ->fetchAll($targetClass);

        // Don't recursively load relations for ManyToMany to prevent circular references
        $prop->setValue($entity, $rows);
    }

    /**
     * Load a ManyToOne relationship.
     */
    private function loadManyToOne(object $entity, ReflectionProperty $prop, ManyToOne $attr): void
    {
        $prop->setAccessible(true);

        $fkColumn = $this->getRelationColumnName($prop->getName());

        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, null);
            return;
        }

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

        $targetTable = $this->tableOf($attr->targetEntity);

        if (!$attr->mappedBy) {
            $prop->setValue($entity, []);
            return;
        }
        $fkColumn = $this->getRelationColumnName($attr->mappedBy, $targetTable);

        $qb = clone $this->qb;
        $related = $qb->select()
            ->from($targetTable)
            ->where($fkColumn, '=', $entityId)
            ->fetchAll($attr->targetEntity);

        $prop->setValue($entity, $related);
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
    private function getRelationColumnName(string $propertyName, ?string $table = null): string
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
    private function normalizeColumn(string $key): string
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

    private function quoteIfReserved(string $ident): string
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