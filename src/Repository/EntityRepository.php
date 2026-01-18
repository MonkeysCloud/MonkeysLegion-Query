<?php

declare(strict_types=1);

namespace MonkeysLegion\Repository;

use InvalidArgumentException;
use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\Uuid;
use MonkeysLegion\Entity\Hydrator;
use MonkeysLegion\Entity\Utils\Uuid as UtilsUuid;
use MonkeysLegion\Query\QueryBuilder;
use ReflectionClass;
use ReflectionProperty;

/**
 * Base repository with common CRUD and query methods.
 *
 * Child classes should define:
 *   protected string $table;
 *   protected string $entityClass;
 */
abstract class EntityRepository extends RelationLoader
{
    public function __construct(QueryBuilder $qb)
    {
        $this->context = new HydrationContext(3); // default max depth
        $this->qb = $qb;
    }

    /**
     * Find a single entity by primary key.
     *
     * @param int|object $idOrEntity The primary key of the entity or the entity itself to find.
     * @param bool $loadRelations Whether to load relationships (default: true)
     * @return object|null The found entity or null if not found.
     * @throws \ReflectionException
     */
    public function find(int|string|object $idOrEntity, bool $loadRelations = true): ?object
    {
        // ☞ accept entity or id
        if (is_object($idOrEntity)) {
            $rp = new \ReflectionProperty($idOrEntity, 'id');
            $id = (string) $rp->getValue($idOrEntity);
        } else {
            $id = (string) $idOrEntity;
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
        if ($limit !== null) {
            $qb->limit($limit);
        }
        if ($offset !== null) {
            $qb->offset($offset);
        }

        $entities = $qb->fetchAll($this->entityClass);

        foreach ($entities as $entity) {
            $this->storeOriginalValues($entity);

            if ($loadRelations) {
                // Reset context for each root entity to ensure proper depth calculation
                $entityId = $this->getEntityId($entity);
                if ($entityId !== null) {
                    // Register as a root entity with depth 0
                    $entityClass = get_class($entity);
                    $this->context->registerInstance($entityClass, $entityId, $entity);
                    $this->context->setDepth($entity, 0);
                    // Load relations with guard
                    $this->loadRelationsWithGuard($entity);
                }
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
     * Persist a new or existing entity. Returns insert ID or affected rows.
     *
     * @param object $entity The entity instance to save.
     * @param bool $partial If true, only update changed fields (default: true for updates)
     * @return int The ID of the saved entity or number of affected rows.
     */
    public function save(object $entity, bool $partial = true, bool $upsert = false): int|string
    {
        // ——— Check if entity uses UUID
        $rc = new \ReflectionClass($entity);
        $isUuid = false;
        $uuidValue = null;

        if ($rc->hasProperty('id')) {
            $idProp = $rc->getProperty('id');

            // Check for #[Uuid] attribute
            $uuidAttrs = $idProp->getAttributes(Uuid::class);
            if ($uuidAttrs) {
                $isUuid = true;
            }

            // Check for #[Field(type: 'uuid')]
            if (!$isUuid) {
                $fieldAttrs = $idProp->getAttributes(\MonkeysLegion\Entity\Attributes\Field::class);
                if ($fieldAttrs) {
                    $fieldAttr = $fieldAttrs[0]->newInstance();
                    if (isset($fieldAttr->type) && strtolower($fieldAttr->type) === 'uuid') {
                        $isUuid = true;
                    }
                }
            }
        }

        // ——— decide update vs insert
        $isUpdate = false;
        if (property_exists($entity, 'id')) {
            $idProp = new \ReflectionProperty($entity, 'id');
            if ($idProp->isInitialized($entity) && $idProp->getValue($entity)) {
                // For UUID: only consider it an update if we can verify the record exists
                if ($isUuid) {
                    $existingId = $idProp->getValue($entity);
                    // Check if this UUID already exists in the database
                    try {
                        $check = (clone $this->qb)->select(['id'])->from($this->table)->where('id', '=', $existingId)->fetch();
                        if ($check && isset($check->id)) {
                            $isUpdate = true;
                            $uuidValue = $existingId;
                        }
                    } catch (\Throwable $e) {
                        error_log("[EntityRepository::save] Error checking UUID existence: " . $e->getMessage());
                    }
                } else {
                    // For auto-increment IDs, if it's set and > 0, it's an update
                    $isUpdate = true;
                }
            } elseif ($isUuid && !$idProp->isInitialized($entity)) {
                // Generate UUID for new entity
                $uuidValue = UtilsUuid::v4();
                $idProp->setValue($entity, $uuidValue);
            }
        }

        // ——— collect data
        $data = ($isUpdate && $partial) ? $this->getChangedFields($entity) : $this->extractPersistableData($entity);

        // If UUID and not updating, ensure UUID is in data
        if ($isUuid && !$isUpdate && $uuidValue) {
            $data['id'] = $uuidValue;
        }

        // ——— normalize scalars
        foreach ($data as $key => $value) {
            if ($value instanceof \DateTimeInterface) {
                $data[$key] = $value->format('Y-m-d H:i:s');
            } elseif (is_bool($value)) {
                $data[$key] = $value ? 1 : 0;
            } elseif (is_float($value)) {
                $data[$key] = (string)$value;
            } elseif (is_array($value)) {
                $data[$key] = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            } elseif (is_object($value)) {
                throw new \InvalidArgumentException("Column `$key` holds object " . get_class($value));
            }
        }

        // ——— table columns
        $colsInTable = $this->listTableColumns($this->table);

        // Derive FK column names from entity attributes (fallback if FK metadata is absent)
        $fkColsFromProps = [];
        try {
            $ref = new \ReflectionClass($entity);
            foreach ($ref->getProperties() as $prop) {
                $attrs = $prop->getAttributes(\MonkeysLegion\Entity\Attributes\ManyToOne::class);
                if (!$attrs) {
                    continue;
                }
                try {
                    $fkColsFromProps[] = $this->getRelationColumnName($prop->getName());
                } catch (\Throwable $ignored) {
                }
            }
        } catch (\Throwable $ignored) {
        }

        // ——— remap odd keys to real columns
        $data = $this->remapColumnsToTable($data, $colsInTable);

        // ——— validate columns exist
        $unknown = array_diff(array_keys($data), $colsInTable);
        if ($unknown) {
            throw new \RuntimeException("Unknown columns for `{$this->table}`: " . implode(', ', $unknown));
        }

        // ——— DB handle
        $pdo = $this->qb->pdo();
        if ($pdo->getAttribute(\PDO::ATTR_ERRMODE) !== \PDO::ERRMODE_EXCEPTION) {
            $pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        }

        // Detect database driver and get schema name
        $driver = $pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);
        $schema = '';

        switch ($driver) {
            case 'sqlite':
                $schema = 'main'; // SQLite uses 'main' as the default schema
                break;
            case 'pgsql':
                $stmt = $pdo->query('SELECT current_database()');
                $schema = $stmt ? (string) $stmt->fetchColumn() : 'public';
                break;
            default: // mysql, mariadb
                $stmt = $pdo->query('SELECT DATABASE()');
                if (!$stmt) {
                    throw new \RuntimeException('Failed to get current database name.');
                }
                $schema = (string) $stmt->fetchColumn();
                break;
        }

        // STATIC caches
        static $___fkCache = [];
        static $___uniqueCache = [];
        static $___tablesCache = [];

        // Load all table names in schema once (multi-DB)
        $loadTables = function (string $schema) use ($pdo, $driver, &$___tablesCache): array {
            if (isset($___tablesCache[$schema])) {
                return $___tablesCache[$schema];
            }

            switch ($driver) {
                case 'sqlite':
                    $st = $pdo->query("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'");
                    return $___tablesCache[$schema] = $st ? array_map(fn($r) => $r['name'], $st->fetchAll(\PDO::FETCH_ASSOC)) : [];

                case 'pgsql':
                    $st = $pdo->prepare("SELECT table_name FROM information_schema.tables WHERE table_catalog = current_database() AND table_schema = 'public'");
                    $st->execute();
                    return $___tablesCache[$schema] = array_map(fn($r) => $r['table_name'], $st->fetchAll(\PDO::FETCH_ASSOC));

                default: // mysql
                    $st = $pdo->prepare("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :s");
                    $st->execute([':s' => $schema]);
                    return $___tablesCache[$schema] = array_map(fn($r) => $r['TABLE_NAME'], $st->fetchAll(\PDO::FETCH_ASSOC));
            }
        };

        // Resolve actual table name (handles underscores→none, case-insensitive)
        $resolveTable = function (string $schema, string $name) use ($loadTables): ?string {
            $tables = $loadTables($schema);
            $map = [];
            foreach ($tables as $t) {
                $map[strtolower($t)] = $t;
            }
            $needle = strtolower($name);
            if (isset($map[$needle])) {
                return $map[$needle];
            }
            $needleNoUnderscore = str_replace('_', '', $needle);
            foreach ($map as $low => $orig) {
                if (str_replace('_', '', $low) === $needleNoUnderscore) {
                    return $orig;
                }
            }
            return null;
        };

        // FKs: COLUMN_NAME => ['ref_schema','ref_table','ref_col','constraint_name'] (multi-DB)
        $loadFks = function (string $schema, string $table) use ($pdo, $driver, &$___fkCache): array {
            $key = "{$schema}.{$table}";
            if (isset($___fkCache[$key])) {
                return $___fkCache[$key];
            }

            $out = [];

            switch ($driver) {
                case 'sqlite':
                    // SQLite: Use PRAGMA foreign_key_list
                    try {
                        $st = $pdo->query("PRAGMA foreign_key_list(\"{$table}\")");
                        if ($st) {
                            foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                                $out[$r['from']] = [
                                    'constraint_name' => "fk_{$table}_{$r['id']}",
                                    'ref_schema'      => $schema,
                                    'ref_table'       => $r['table'],
                                    'ref_col'         => $r['to'] ?: 'id',
                                ];
                            }
                        }
                    } catch (\Throwable $e) {
                        // SQLite FK introspection may not be available
                    }
                    break;

                case 'pgsql':
                    // PostgreSQL: Use information_schema
                    $sql = "SELECT kcu.column_name, tc.constraint_name, 
                                   ccu.table_catalog as ref_schema, ccu.table_name as ref_table, ccu.column_name as ref_col
                              FROM information_schema.table_constraints tc
                              JOIN information_schema.key_column_usage kcu ON tc.constraint_name = kcu.constraint_name
                              JOIN information_schema.constraint_column_usage ccu ON ccu.constraint_name = tc.constraint_name
                             WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = :t";
                    $st = $pdo->prepare($sql);
                    $st->execute([':t' => $table]);
                    foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                        $out[$r['column_name']] = [
                            'constraint_name' => $r['constraint_name'],
                            'ref_schema'      => $r['ref_schema'] ?: $schema,
                            'ref_table'       => $r['ref_table'],
                            'ref_col'         => $r['ref_col'] ?: 'id',
                        ];
                    }
                    break;

                default: // mysql
                    $sql = "SELECT COLUMN_NAME, CONSTRAINT_NAME, REFERENCED_TABLE_SCHEMA, REFERENCED_TABLE_NAME, REFERENCED_COLUMN_NAME
                          FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                         WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t AND REFERENCED_TABLE_NAME IS NOT NULL";
                    $st  = $pdo->prepare($sql);
                    $st->execute([':s' => $schema, ':t' => $table]);
                    foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                        $out[$r['COLUMN_NAME']] = [
                            'constraint_name' => $r['CONSTRAINT_NAME'],
                            'ref_schema'      => $r['REFERENCED_TABLE_SCHEMA'] ?: $schema,
                            'ref_table'       => $r['REFERENCED_TABLE_NAME'],
                            'ref_col'         => $r['REFERENCED_COLUMN_NAME'] ?: 'id',
                        ];
                    }
                    break;
            }

            return $___fkCache[$key] = $out;
        };

        // Unique indexes (incl. PRIMARY) - multi-DB
        $loadUniqueIndexes = function (string $schema, string $table) use ($pdo, $driver, &$___uniqueCache): array {
            $key = "{$schema}.{$table}";
            if (isset($___uniqueCache[$key])) {
                return $___uniqueCache[$key];
            }

            $idx = [];

            switch ($driver) {
                case 'sqlite':
                    // SQLite: Use PRAGMA index_list
                    try {
                        $st = $pdo->query("PRAGMA index_list(\"{$table}\")");
                        if ($st) {
                            foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                                if ((int)$r['unique'] !== 1) {
                                    continue;
                                }
                                $indexName = $r['name'];
                                $isPrimary = str_starts_with($indexName, 'sqlite_autoindex_') || $indexName === 'PRIMARY';

                                // Get columns for this index
                                $colSt = $pdo->query("PRAGMA index_info(\"{$indexName}\")");
                                $cols = $colSt ? array_map(fn($c) => $c['name'], $colSt->fetchAll(\PDO::FETCH_ASSOC)) : [];

                                if ($cols) {
                                    $idx[$indexName] = ['name' => $indexName, 'columns' => $cols, 'is_primary' => $isPrimary];
                                }
                            }
                        }
                    } catch (\Throwable $e) {
                        // Ignore
                    }
                    break;

                case 'pgsql':
                    // PostgreSQL: Use pg_catalog
                    $sql = "SELECT i.relname as index_name, a.attname as column_name,
                                   ix.indisunique as is_unique, ix.indisprimary as is_primary
                              FROM pg_catalog.pg_class t
                              JOIN pg_catalog.pg_index ix ON t.oid = ix.indrelid
                              JOIN pg_catalog.pg_class i ON i.oid = ix.indexrelid
                              JOIN pg_catalog.pg_attribute a ON a.attrelid = t.oid AND a.attnum = ANY(ix.indkey)
                             WHERE t.relname = :t AND ix.indisunique = true
                          ORDER BY i.relname, a.attnum";
                    $st = $pdo->prepare($sql);
                    $st->execute([':t' => $table]);
                    foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                        $name = $r['index_name'];
                        if (!isset($idx[$name])) {
                            $idx[$name] = ['name' => $name, 'columns' => [], 'is_primary' => (bool)$r['is_primary']];
                        }
                        $idx[$name]['columns'][] = $r['column_name'];
                    }
                    break;

                default: // mysql
                    $sql = "SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX
                          FROM INFORMATION_SCHEMA.STATISTICS
                         WHERE TABLE_SCHEMA = :s AND TABLE_NAME = :t
                      ORDER BY INDEX_NAME, SEQ_IN_INDEX";
                    $st  = $pdo->prepare($sql);
                    $st->execute([':s' => $schema, ':t' => $table]);
                    foreach ($st->fetchAll(\PDO::FETCH_ASSOC) as $r) {
                        if ((int)$r['NON_UNIQUE'] !== 0) {
                            continue;
                        }
                        $name = $r['INDEX_NAME'];
                        if (!isset($idx[$name])) {
                            $idx[$name] = ['name' => $name, 'columns' => [], 'is_primary' => ($name === 'PRIMARY')];
                        }
                        $idx[$name]['columns'][] = $r['COLUMN_NAME'];
                    }
                    break;
            }

            return $___uniqueCache[$key] = array_values($idx);
        };

        // Select id by composite key
        $selectIdByKey = function (array $key) {
            $q = (clone $this->qb)->select(['id'])->from($this->table);
            $first = true;
            foreach ($key as $c => $v) {
                if ($first) {
                    $q->where($c, '=', $v);
                    $first = false;
                } else {
                    $q->andWhere($c, '=', $v);
                }
            }
            return $q->fetch();
        };

        // Self-heal FK (when 1452 is thrown due to wrong referenced table name)
        $attemptRepairFk = function (string $schema, string $table, array $fkMap) use ($pdo, $resolveTable): bool {
            foreach ($fkMap as $col => $meta) {
                $refSchema   = $meta['ref_schema'] ?: $schema;
                $refTableOrg = $meta['ref_table'] ?? '';
                $refCol      = $meta['ref_col'] ?? 'id';
                $cname       = $meta['constraint_name'] ?? null;
                if (!$refTableOrg || !$cname) {
                    continue;
                }

                $resolved = $resolveTable($refSchema, $refTableOrg);
                if (!$resolved || strcasecmp($resolved, $refTableOrg) === 0) {
                    continue;
                }

                // verify resolved table really exists
                $chk = $pdo->prepare("SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA=:s AND TABLE_NAME=:t");
                $chk->execute([':s' => $refSchema, ':t' => $resolved]);
                if (!$chk->fetchColumn()) {
                    continue;
                }

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
            $isOnlyId = (count($cols) === 1 && $cols[0] === 'id');
            if ($idx['is_primary'] && $isOnlyId) {
                continue;
            }
            if (count(array_diff($cols, $dataKeys)) === 0) {
                $fkOnly = count(array_diff($cols, array_keys($fkMap))) === 0;
                $bonus = 0;
                foreach ($cols as $c) {
                    if (stripos($c, 'email') !== false || stripos($c, 'uuid') !== false || stripos($c, 'external') !== false) {
                        $bonus += 2;
                        break;
                    }
                }
                $candidateUnique[] = ['name' => $idx['name'], 'cols' => $cols, 'fkOnly' => $fkOnly, 'arity' => count($cols), 'bonus' => $bonus];
            }
        }
        usort($candidateUnique, function ($a, $b) {
            if ($a['fkOnly'] !== $b['fkOnly']) {
                return $a['fkOnly'] ? 1 : -1; // prefer non-FK-only
            }
            if ($a['arity']  !== $b['arity']) {
                return $b['arity'] <=> $a['arity'];
            }
            if ($a['bonus']  !== $b['bonus']) {
                return $b['bonus'] <=> $a['bonus'];
            }
            return count($a['cols']) <=> count($b['cols']);
        });
        $chosenUnique   = $candidateUnique[0] ?? null;
        $naturalKeyCols = $chosenUnique['cols'] ?? [];

        // Try to match existing row by natural key (only if not already determined to be update)
        if (!$isUpdate && $naturalKeyCols) {
            $keyMap = [];
            foreach ($naturalKeyCols as $c) {
                $keyMap[$c] = $data[$c];
            }
            $existing = $selectIdByKey($keyMap);
            if ($existing && isset($existing->id)) {
                $isUpdate = true;
                $data['id'] = $existing->id;
                if ($isUuid) {
                    $uuidValue = $existing->id;
                }
            }
        }

        // ——— UPDATE path (use $isUpdate flag, not just presence of id)
        if ($isUpdate && !empty($data['id'])) {
            $id = $data['id'];
            unset($data['id']);
            if (empty($data)) {
                // Even if no scalar data changed, sync ManyToMany relations
                $this->syncManyToManyRelations($entity);
                return 0;
            }
            $setParts = [];
            foreach ($data as $col => $_) {
                $setParts[] = $this->quoteIdentifier($col, $driver) . " = :{$col}";
            }
            $tableName = $this->quoteIdentifier($this->table, $driver);
            $idColumn = $this->quoteIdentifier('id', $driver);
            $sql = "UPDATE {$tableName} SET " . implode(', ', $setParts) . " WHERE {$idColumn} = :_id";
            try {
                $stmt = $pdo->prepare($sql);
                foreach ($data as $col => $val) {
                    $stmt->bindValue(":{$col}", $val);
                }
                $stmt->bindValue(":_id", $id);
                $stmt->execute();
                $rowCount = $stmt->rowCount();
                
                // Sync ManyToMany relations on update
                $this->syncManyToManyRelations($entity);
                
                $this->storeOriginalValues($entity);
                return $rowCount;
            } catch (\PDOException $e) {
                throw $e;
            }
        }

        // ——— INSERT / UPSERT
        if (empty($data)) {
            throw new \LogicException("No data to INSERT for `{$this->table}` and entity " . get_class($entity));
        }

        // Soft FK preflight with resolution (log-only previously; now silent — DB enforces truth)
        $fkCols = array_keys($fkMap);
        foreach ($fkColsFromProps as $fkPropCol) {
            if (!in_array($fkPropCol, $fkCols, true)) {
                $fkCols[] = $fkPropCol;
            }
        }
        foreach ($fkCols as $col) {
            if (!array_key_exists($col, $data)) {
                continue;
            }
            try {
                $meta = $fkMap[$col] ?? ['ref_schema' => $schema, 'ref_table' => preg_replace('/_id$/', '', $col), 'ref_col' => 'id'];
                $refSchema = $meta['ref_schema'] ?? $schema;
                $refTableOrig = $meta['ref_table'] ?? preg_replace('/_id$/', '', $col);
                $refTable = $resolveTable($refSchema, $refTableOrig) ?: $refTableOrig;
                $refCol   = $meta['ref_col'] ?? 'id';

                $q = (clone $this->qb)->select([$refCol])->from($refTable)->where($refCol, '=', $data[$col]);
                $q->fetch(); // ignore result; DB will enforce for real
            } catch (\Throwable $fkEx) {
                // soft-skip any preflight errors
            }
        }

        $cols         = array_keys($data);
        $placeholders = array_map(fn($c) => ':' . $c, $cols);

        $sql = sprintf(
            "INSERT INTO %s (%s) VALUES (%s)",
            $this->quoteIdentifier($this->table, $driver),
            implode(',', array_map(fn($c) => $this->quoteIdentifier($c, $driver), $cols)),
            implode(',', $placeholders)
        );

        if ($chosenUnique && !$upsert) {
            $upsert = true;
        }

        // Multi-DB upsert syntax
        if ($upsert && $chosenUnique) {
            switch ($driver) {
                case 'pgsql':
                    // PostgreSQL: ON CONFLICT DO UPDATE
                    $conflictCols = implode(',', array_map(fn($c) => $this->quoteIdentifier($c, $driver), $chosenUnique['cols']));
                    $sql .= " ON CONFLICT ({$conflictCols}) DO UPDATE SET id = EXCLUDED.id";
                    if (in_array('subscribed_at', $cols, true)) {
                        $sql .= ", subscribed_at = EXCLUDED.subscribed_at";
                    }
                    break;
                case 'sqlite':
                    // SQLite: Use ON CONFLICT to preserve the ID (INSERT OR REPLACE would delete+insert)
                    $conflictCols = implode(',', array_map(fn($c) => $this->quoteIdentifier($c, $driver), $chosenUnique['cols']));
                    $sql .= " ON CONFLICT ({$conflictCols}) DO UPDATE SET id = excluded.id";
                    if (in_array('subscribed_at', $cols, true)) {
                        $sql .= ", subscribed_at = excluded.subscribed_at";
                    }
                    break;
                default: // mysql
                    $quotedId = $this->quoteIdentifier('id', $driver);
                    $sql .= " ON DUPLICATE KEY UPDATE {$quotedId} = LAST_INSERT_ID({$quotedId})";
                    if (in_array('subscribed_at', $cols, true)) {
                        $quotedSubscribedAt = $this->quoteIdentifier('subscribed_at', $driver);
                        $sql .= ", {$quotedSubscribedAt} = VALUES({$quotedSubscribedAt})";
                    }
                    break;
            }
        }

        $id = $isUuid ? $uuidValue : 0;
        $retriedAfterRepair = false;

        while (true) {
            try {
                $stmt = $pdo->prepare($sql);
                foreach ($data as $col => $val) {
                    $stmt->bindValue(":{$col}", $val);
                }
                $stmt->execute();

                // For UUID, use the value we set; for auto-increment, get lastInsertId
                if (!$isUuid) {
                    $id = (int)$pdo->lastInsertId();
                }
                break;
            } catch (\PDOException $e) {
                $errorCode = isset($e->errorInfo[1]) ? (int)$e->errorInfo[1] : 0;
                $sqlState = $e->errorInfo[0] ?? '';

                // Detect duplicate key error across databases
                $isDuplicate = match ($driver) {
                    'mysql' => $errorCode === 1062,
                    'pgsql' => $sqlState === '23505',
                    'sqlite' => $errorCode === 19 || str_contains($e->getMessage(), 'UNIQUE constraint'),
                    default => false,
                };

                // Duplicate → update by natural key
                if ($isDuplicate && $naturalKeyCols) {
                    $keyMap = [];
                    foreach ($naturalKeyCols as $c) {
                        $keyMap[$c] = $data[$c];
                    }
                    $row = $selectIdByKey($keyMap);
                    if ($row && isset($row->id)) {
                        $existingId = $row->id;
                        $setParts = [];
                        foreach ($data as $col => $_) {
                            $setParts[] = $this->quoteIdentifier($col, $driver) . " = :{$col}";
                        }
                        $updateSql = "UPDATE " . $this->quoteIdentifier($this->table, $driver) . " SET " . implode(', ', $setParts) . " WHERE id = :_id";
                        $stmt = $pdo->prepare($updateSql);
                        foreach ($data as $col => $val) {
                            $stmt->bindValue(":{$col}", $val);
                        }
                        $stmt->bindValue(":_id", $existingId);
                        $stmt->execute();

                        if (property_exists($entity, 'id')) {
                            $rp = new \ReflectionProperty($entity, 'id');
                            $rp->setValue($entity, $existingId);
                        }
                        $this->storeOriginalValues($entity);
                        return $existingId;
                    }
                }


                // FK wrong referenced table: try FK repair once, then retry (MySQL only)
                if ($driver === 'mysql' && $errorCode === 1452 && !$retriedAfterRepair) {
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
            $keyMap = [];
            foreach ($naturalKeyCols as $c) {
                $keyMap[$c] = $data[$c];
            }
            try {
                $v = (clone $this->qb);
                $firstKey = array_key_first($keyMap);
                $verify = $v->select(['id'])
                    ->from($this->table)
                    ->where((string)$firstKey, '=', reset($keyMap));
                $first = true;
                foreach ($keyMap as $k => $vv) {
                    if ($first) {
                        $first = false;
                        continue;
                    }
                    $verify->andWhere($k, '=', $vv);
                }
                $verifyRow = $verify->fetch();
            } catch (\Exception $ex) { /* ignore */
            }
        } elseif ($id) {
            try {
                $v = (clone $this->qb);
                $verifyRow = $v->select(['id'])->from($this->table)->where('id', '=', $id)->fetch();
            } catch (\Exception $ex) { /* ignore */
            }
        }

        if (!$verifyRow && !$id) {
            throw new \RuntimeException("INSERT appeared to succeed but row not visible on same connection.");
        }

        $finalId = $verifyRow->id ?? $id;

        if (property_exists($entity, 'id')) {
            $ref = new \ReflectionProperty($entity, 'id');
            $ref->setValue($entity, $finalId);
        }

        // Sync ManyToMany relations
        $this->syncManyToManyRelations($entity);

        $this->storeOriginalValues($entity);
        return $finalId;
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
        if (!$row) {
            return 0;
        }
        return (int) ($row->count ?? 0);
    }

    /**
     * Delete entity by primary key.
     *
     * @param int|object $idOrEntity The primary key of the entity to delete or entity instance.
     * @return int The number of affected rows (should be 1 for successful delete).
     */
    public function delete(int|string|object $idOrEntity): int
    {
        // Extract ID from entity or use provided ID
        if (is_object($idOrEntity)) {
            if (!property_exists($idOrEntity, 'id')) {
                throw new InvalidArgumentException('Entity has no id property');
            }
            $rp = new ReflectionProperty($idOrEntity, 'id');
            $id = (string) $rp->getValue($idOrEntity);

            // Clean up stored original values
            unset($this->originalValues[spl_object_id($idOrEntity)]);
        } else {
            $id = $idOrEntity;
        }

        // Validate ID - handle both numeric and string (UUID) IDs
        if ($id === '' || $id === null || (is_numeric($id) && (int)$id <= 0)) {
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
            // Note: Using standard DELETE syntax without LIMIT for broader DB compatibility
            $pdo = $this->qb->pdo();
            $driver = $pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);
            $tableName = $this->quoteIdentifier($this->table, $driver);
            $idColumn = $this->quoteIdentifier('id', $driver);
            $sql = "DELETE FROM {$tableName} WHERE {$idColumn} = ?";
            $stmt = $pdo->prepare($sql);
            $stmt->execute([$id]);
            return $stmt->rowCount();
        } catch (\Throwable $e) {
            throw $e;
        }
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
        [$table, $ownCol, $invCol] = $this->getJoinTableMeta($relationProp, $entity);

        // retrieve the ID of this entity
        $ref = new ReflectionProperty($entity::class, 'id');
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
        [$table, $ownCol, $invCol] = $this->getJoinTableMeta($relationProp, $entity);

        $ref = new ReflectionProperty($entity::class, 'id');
        $ownId = $ref->getValue($entity);

        return $this->qb
            ->delete($table)
            ->where($ownCol, '=', $ownId)
            ->andWhere($invCol, '=', $value)
            ->execute();
    }

    /**
     * Sync ManyToMany relations from entity collections to the join table.
     * This is called automatically by save() to persist ManyToMany changes.
     *
     * @param object $entity
     * @throws \ReflectionException
     */
    protected function syncManyToManyRelations(object $entity): void
    {
        $rc = new \ReflectionClass($entity);
        $entityId = $this->getEntityId($entity);
        
        if (!$entityId) {
            return; // Cannot sync without an ID
        }

        foreach ($rc->getProperties() as $prop) {
            $attrs = $prop->getAttributes(ManyToMany::class);
            if (!$attrs) {
                continue;
            }

            // Check if this is the owning side (has joinTable)
            /** @var ManyToMany $m2m */
            $m2m = $attrs[0]->newInstance();
            
            // Only sync from the owning side (the one with joinTable defined)
            if (!$m2m->joinTable && ($m2m->mappedBy || $m2m->inversedBy)) {
                continue;
            }

            // Get the collection from the entity
            if (!$prop->isInitialized($entity)) {
                continue;
            }
            
            $collection = $prop->getValue($entity);
            if (!is_array($collection) && !($collection instanceof \Traversable)) {
                continue;
            }

            // SAFETY: Only sync if:
            // 1. Collection is non-empty (explicit intent to have relations), OR
            // 2. Entity has stored original values (was properly loaded with find($id, true))
            // This prevents accidental deletion when saving an entity loaded without relations
            $hasOriginalValues = isset($this->originalValues[spl_object_id($entity)]);
            $collectionIsEmpty = is_array($collection) ? count($collection) === 0 : !iterator_count($collection);
            
            if ($collectionIsEmpty && !$hasOriginalValues) {
                // Skip sync - entity appears to be loaded without relations
                // and collection is empty, so we can't tell if user wants to clear all
                continue;
            }

            // Get join table metadata
            try {
                [$joinTable, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName(), $entity);
            } catch (\Throwable $e) {
                continue; // Skip if we can't get join table info
            }

            // Get current IDs in the join table (normalize to strings)
            $currentIds = [];
            try {
                // Use QueryBuilder for proper identifier quoting and parameter binding
                $rows = (clone $this->qb)
                    ->select([$invCol])
                    ->from($joinTable)
                    ->where($ownCol, '=', $entityId)
                    ->fetchAll();
                foreach ($rows as $row) {
                    // Handle both object and array access patterns
                    $value = is_object($row) ? ($row->$invCol ?? null) : ($row[$invCol] ?? null);
                    if ($value !== null) {
                        $currentIds[] = (string) $value;
                    }
                }
            } catch (\Throwable $e) {
                // Table might not exist yet
            }

            // Get desired IDs from the collection (normalize to strings)
            $desiredIds = [];
            foreach ($collection as $related) {
                if (is_object($related)) {
                    $relatedId = $this->getEntityId($related);
                    if ($relatedId !== null) {
                        $desiredIds[] = (string) $relatedId;
                    }
                } elseif (is_int($related) || is_string($related)) {
                    $desiredIds[] = (string) $related;
                }
            }

            // Calculate differences (both arrays now contain strings)
            $toAttach = array_diff($desiredIds, $currentIds);
            $toDetach = array_diff($currentIds, $desiredIds);

            // Attach new relations
            foreach ($toAttach as $relatedId) {
                try {
                    $this->qb->insert($joinTable, [
                        $ownCol => $entityId,
                        $invCol => $relatedId,
                    ]);
                } catch (\Throwable $e) {
                    // Ignore duplicates
                }
            }

            // Detach removed relations
            foreach ($toDetach as $relatedId) {
                (clone $this->qb)
                    ->delete($joinTable)
                    ->where($ownCol, '=', $entityId)
                    ->andWhere($invCol, '=', $relatedId)
                    ->execute();
            }
        }
    }
}

