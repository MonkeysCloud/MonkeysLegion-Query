<?php

declare(strict_types=1);

namespace MonkeysLegion\Query;

use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Query\Traits\Identifier;
use MonkeysLegion\Query\Traits\TableOperations;
use PDO;

abstract class AbstractQueryBuilder
{
    use TableOperations;
    use Identifier;

    /** @var array<string,mixed> */
    protected array $parts = [
        'select'   => '*',
        'distinct' => false,
        'from'     => '',
        'joins'    => [],
        'where'    => [],
        'groupBy'  => [],
        'having'   => [],
        'orderBy'  => [],
        'limit'    => null,
        'offset'   => null,
        'custom'   => null,
        'unions'   => [],
    ];

    /** @var array<string,mixed> */
    protected array $params = [];

    /** @var int */
    protected int $counter = 0;

    /** @var bool */
    protected bool $inTransaction = false;

    /** @var bool */
    protected bool $preflightDone = false;

    /**
     *  @var array<string,string>
     * Deterministic mapping first. Extend at bootstrap via setTableMap()
     */
    protected array $tableMap = [
        // 'messages' => 'message',
    ];

    /** alias => [schema|null, table] */
    private array $aliasMap = [];

    /** cache: "schema.table.column" => bool */
    private array $columnExistsCache = [];

    /**
     * Constructor.
     *
     * @param ConnectionInterface $conn Database connection instance.
     */
    public function __construct(protected ConnectionInterface $conn)
    {
    }

    /**
     * Expose the Connection.
     */
    public function connection(): ConnectionInterface
    {
        return $this->conn;
    }

    /**
     * Expose the underlying PDO so low-level operations can be performed.
     */
    public function pdo(): \PDO
    {
        return $this->conn->pdo();
    }

    /**
     * Returns the SQL query as a string.
     */
    public function toSql(): string
    {
        if ($this->parts['custom']) {
            $sql = $this->parts['custom'];

            if (preg_match('/^(UPDATE|DELETE)/i', $sql) && !empty($this->parts['where'])) {
                $clauses = [];
                foreach ($this->parts['where'] as $i => $w) {
                    $prefix = $i && $w['type'] ? ' ' . $w['type'] . ' ' : '';
                    $clauses[] = $prefix . $w['expr'];
                }
                $sql .= ' WHERE ' . implode('', $clauses);
            }

            return $sql;
        }

        // Ensure FROM/JOIN table names are resolved (once per instance)
        if (!$this->preflightDone) {
            $this->preflightResolveTables();
            $this->preflightDone = true;
        }

        $sql = 'SELECT ' .
            ($this->parts['distinct'] ? 'DISTINCT ' : '') .
            $this->parts['select'] .
            ' FROM ' . $this->parts['from'];

        if ($this->parts['joins']) {
            $sql .= ' ' . implode(' ', $this->parts['joins']);
        }

        if ($this->parts['where']) {
            $clauses = [];
            foreach ($this->parts['where'] as $i => $w) {
                $prefix = $i && $w['type'] ? ' ' . $w['type'] . ' ' : '';
                $clauses[] = $prefix . $w['expr'];
            }
            $sql .= ' WHERE ' . implode('', $clauses);
        }

        if ($this->parts['groupBy']) {
            $sql .= ' GROUP BY ' . implode(', ', $this->parts['groupBy']);
        }

        if ($this->parts['having']) {
            $sql .= ' HAVING ' . implode(' AND ', $this->parts['having']);
        }

        if ($this->parts['orderBy']) {
            $sql .= ' ORDER BY ' . implode(', ', $this->parts['orderBy']);
        }

        if ($this->parts['limit'] !== null) {
            $sql .= ' LIMIT ' . $this->parts['limit'];
        }

        if ($this->parts['offset'] !== null) {
            $sql .= ' OFFSET ' . $this->parts['offset'];
        }

        foreach ($this->parts['unions'] as $union) {
            $sql .= $union['all'] ? ' UNION ALL ' : ' UNION ';
            $sql .= $union['sql'];
            $this->params = array_merge($this->params, $union['params']);
        }

        return $sql;
    }

    /**
     * Get the SQL with parameters replaced (for debugging).
     */
    public function toDebugSql(): string
    {
        $sql = $this->toSql();
        foreach ($this->params as $key => $value) {
            $quotedValue = is_numeric($value) ? $value : "'" . addslashes((string)$value) . "'";
            $sql = str_replace($key, $quotedValue, $sql);
        }
        return $sql;
    }

    /**
     * Resolve FROM and JOIN tables before SQL generation.
     * Logs BEFORE/AFTER for from/fromTable and each join.
     */
    protected function preflightResolveTables(): static
    {
        // reset per-query alias map
        $this->aliasMap = [];

        // ---------- FROM ----------
        if (!empty($this->parts['from']) && is_string($this->parts['from'])) {
            $raw = trim($this->parts['from']);

            $reFrom = '/^\s*((?:`?\w+`?\.)?`?\w+`?)\s*(?:AS\s+|\s+)?(`?\w+`?)?\s*$/i';
            if (preg_match($reFrom, $raw, $m)) {
                $ref   = $m[1];
                $alias = isset($m[2]) ? $m[2] : '';

                [$schema, $table] = $this->parseQualifiedRef($ref);
                $resolved = $this->mapOrResolve($table);

                $qualified = $this->quoteQualified($schema, $resolved) . ($alias ? ' ' . $alias : '');
                $this->parts['from'] = $qualified;

                // Record alias→(schema, table)
                $aliasName = $alias ? trim($alias, ' `') : $resolved;
                $this->aliasMap[$aliasName] = [$schema, $resolved];
                // also allow raw table name without alias
                $this->aliasMap[$resolved] = [$schema, $resolved];
            }
        }

        // ---------- JOINs ----------
        if (!empty($this->parts['joins']) && is_array($this->parts['joins'])) {
            foreach ($this->parts['joins'] as $i => $joinStr) {
                if (!is_string($joinStr)) {
                    continue;
                }

                $orig = $joinStr;

                $reJoin = '/\bJOIN\s+((?:`?\w+`?\.)?`?\w+`?)' .    // 1: ref
                    '(?:\s+(?:AS\s+)?(`?\w+`?))?' .          // 2: alias (optional)
                    '(\s+(?:ON|USING)\b)/i';                 // 3: " ON" or " USING"

                $joinStr = preg_replace_callback($reJoin, function ($mm) {
                    $ref        = $mm[1];
                    $aliasChunk = isset($mm[2]) ? (' ' . $mm[2]) : '';
                    $afterKW    = $mm[3];

                    [$schema, $table] = $this->parseQualifiedRef($ref);
                    $resolved  = $this->mapOrResolve($table);
                    $qualified = 'JOIN ' . $this->quoteQualified($schema, $resolved) . $aliasChunk . $afterKW;

                    // Record alias mapping if present
                    if (!empty($mm[2])) {
                        $aliasName = trim($mm[2], ' `');
                        $this->aliasMap[$aliasName] = [$schema, $resolved];
                    } else {
                        // if no alias, allow table name as alias
                        $this->aliasMap[$resolved] = [$schema, $resolved];
                    }

                    return $qualified;
                }, $joinStr, 1);

                if ($orig !== $joinStr) {
                    $this->parts['joins'][$i] = $joinStr;
                }

                // Normalize ON/USING clause columns (alias.col)
                $posOn = stripos($joinStr, ' ON ');
                $posUsing = $posOn === false ? stripos($joinStr, ' USING ') : false;

                if ($posOn !== false) {
                    $head = substr($joinStr, 0, $posOn + 4);
                    $tail = substr($joinStr, $posOn + 4);
                    $tail = $this->normalizeColumnsClause($tail);
                    $this->parts['joins'][$i] = $head . $tail;
                } elseif ($posUsing !== false) {
                    $head = substr($joinStr, 0, $posUsing + 7);
                    $tail = substr($joinStr, $posUsing + 7);
                    $tail = $this->normalizeColumnsClause($tail);
                    $this->parts['joins'][$i] = $head . $tail;
                }
            }
        }

        // Also normalize columns inside WHERE/HAVING expressions
        if (!empty($this->parts['where'])) {
            foreach ($this->parts['where'] as $k => $w) {
                if (isset($w['expr']) && is_string($w['expr'])) {
                    $this->parts['where'][$k]['expr'] = $this->normalizeColumnsClause($w['expr']);
                }
            }
        }
        if (!empty($this->parts['having'])) {
            foreach ($this->parts['having'] as $k => $h) {
                if (is_string($h)) {
                    $this->parts['having'][$k] = $this->normalizeColumnsClause($h);
                }
            }
        }
        // ---------- SELECT / ORDER BY / GROUP BY ----------
        if (is_string($this->parts['select']) && $this->parts['select'] !== '*') {
            $this->parts['select'] = $this->normalizeColumnsClause($this->parts['select'], true);
        }

        if (!empty($this->parts['orderBy'])) {
            foreach ($this->parts['orderBy'] as $k => $ob) {
                if (is_string($ob)) {
                    $this->parts['orderBy'][$k] = $this->normalizeColumnsClause($ob, true);
                }
            }
        }

        if (!empty($this->parts['groupBy'])) {
            foreach ($this->parts['groupBy'] as $k => $gb) {
                if (is_string($gb)) {
                    $this->parts['groupBy'][$k] = $this->normalizeColumnsClause($gb, true);
                }
            }
        }

        return $this;
    }

    protected function findAliasForColumn(string $column): ?array
    {
        $candidates = [];
        foreach ($this->aliasMap as $alias => [$schema, $table]) {
            if (!$table) {
                continue;
            }

            // original
            if ($this->columnExists($schema, $table, $column)) {
                $candidates[] = [$alias, $column];
                continue;
            }
            // snake⇄camel before _id
            if (str_ends_with($column, '_id')) {
                $alt1 = $this->snakeToCamelId($column);
                if ($alt1 !== $column && $this->columnExists($schema, $table, $alt1)) {
                    $candidates[] = [$alias, $alt1];
                }
                $alt2 = $this->camelToSnakeId($column);
                if ($alt2 !== $column && $this->columnExists($schema, $table, $alt2)) {
                    $candidates[] = [$alias, $alt2];
                }
            }
        }
        // only rewrite if unambiguous
        return count($candidates) === 1 ? $candidates[0] : null;
    }

    /** Map first; if not found, try singular/plural fallbacks using information_schema. */
    protected function mapOrResolve(string $table, ?string $schema = null): string
    {
        if (isset($this->tableMap[$table])) {
            $mapped = $this->tableMap[$table];
            return $mapped;
        }

        // exact
        if ($this->tableExists($table, $schema)) {
            return $table;
        }

        // plural/singular toggles
        $candidates = str_ends_with($table, 's')
            ? [substr($table, 0, -1)]
            : [$table . 's'];

        foreach ($candidates as $cand) {
            if ($this->tableExists($cand, $schema)) {
                return $cand;
            }
        }

        return $table;
    }

    /**
     * Performs an aggregate function.
     */
    protected function aggregate(string $function, string $column): float
    {
        $originalSelect = $this->parts['select'];
        $originalOrderBy = $this->parts['orderBy'];
        $originalLimit = $this->parts['limit'];
        $originalOffset = $this->parts['offset'];

        $this->parts['select'] = "$function($column) as result";
        $this->parts['orderBy'] = [];  // Not needed for aggregates
        $this->parts['limit'] = null;   // Not needed for aggregates
        $this->parts['offset'] = null;  // Not needed for aggregates

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Aggregate query failed: $state/$code – $msg");
        }

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->parts['select'] = $originalSelect;
        $this->parts['orderBy'] = $originalOrderBy;
        $this->parts['limit'] = $originalLimit;
        $this->parts['offset'] = $originalOffset;

        return (float) ($result['result'] ?? 0);
    }

    /**
     * Performs an aggregate function returning raw value.
     */
    protected function aggregateRaw(string $function, string $column): mixed
    {
        $originalSelect = $this->parts['select'];
        $originalOrderBy = $this->parts['orderBy'];
        $originalLimit = $this->parts['limit'];
        $originalOffset = $this->parts['offset'];

        $this->parts['select'] = "$function($column) as result";
        $this->parts['orderBy'] = [];  // Not needed for aggregates
        $this->parts['limit'] = null;   // Not needed for aggregates
        $this->parts['offset'] = null;  // Not needed for aggregates

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Aggregate query failed: $state/$code – $msg");
        }

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->parts['select'] = $originalSelect;
        $this->parts['orderBy'] = $originalOrderBy;
        $this->parts['limit'] = $originalLimit;
        $this->parts['offset'] = $originalOffset;

        return $result['result'] ?? null;
    }

    /**
     * Adds a parameter to the query and returns its placeholder.
     */
    protected function addParam(mixed $value): string
    {
        $key = ':p' . $this->counter++;
        $this->params[$key] = $value;
        return $key;
    }

    /** Cache for column existence checks. */
    protected function columnExists(?string $schema, string $table, string $column): bool
    {
        $schema = $schema ?: null;
        $key = ($schema ? $schema . '.' : '') . $table . '.' . $column;
        if (array_key_exists($key, $this->columnExistsCache)) {
            return $this->columnExistsCache[$key];
        }

        try {
            $driver = $this->pdo()->getAttribute(PDO::ATTR_DRIVER_NAME);

            if ($driver === 'sqlite') {
                // PRAGMA table_info(table)
                $safeTable = preg_replace('/[^A-Za-z0-9_]/', '', $table);
                $stmt = $this->conn->pdo()->query("PRAGMA table_info($safeTable)");
                $cols = $stmt ? $stmt->fetchAll(PDO::FETCH_ASSOC) : [];
                $exists = false;
                foreach ($cols as $c) {
                    if (isset($c['name']) && strcasecmp($c['name'], $column) === 0) {
                        $exists = true;
                        break;
                    }
                }
                return $this->columnExistsCache[$key] = $exists;
            }

            // MySQL / MariaDB
            $sql = "SELECT 1
                  FROM information_schema.columns
                 WHERE table_name = :t
                   AND column_name = :c
                   AND table_schema = COALESCE(:s, DATABASE())
                 LIMIT 1";
            $stmt = $this->conn->pdo()->prepare($sql);
            $stmt->execute([':t' => $table, ':c' => $column, ':s' => $schema]);
            $exists = (bool) $stmt->fetchColumn();
            return $this->columnExistsCache[$key] = $exists;
        } catch (\Throwable $e) {
            error_log("[qb.columnExists] {$e->getMessage()} for " . ($schema ? "$schema." : "") . "$table.$column");
            return $this->columnExistsCache[$key] = false;
        }
    }

    /** Convert snake_case_id to camelCase_id */
    protected function snakeToCamelId(string $col): string
    {
        if (!str_ends_with($col, '_id')) {
            return $col;
        }
        $base = substr($col, 0, -3); // drop _id
        $parts = explode('_', $base);
        $first = array_shift($parts);
        $camel = $first . implode('', array_map(fn($p) => ucfirst(strtolower($p)), $parts));
        return $camel . '_id';
    }

    /** Convert camelCase_id to snake_case_id */
    protected function camelToSnakeId(string $col): string
    {
        if (!str_ends_with($col, '_id')) {
            return $col;
        }
        $base = substr($col, 0, -3);
        $snake = strtolower(preg_replace('/([a-z0-9])([A-Z])/', '$1_$2', $base));
        return $snake . '_id';
    }

    /** Try resolve a column for an alias, allowing snake⇄camel before `_id`. Returns the chosen column name or null. */
    protected function resolveColumnForAlias(string $alias, string $column): ?string
    {
        $alias = trim($alias, ' `');
        $column = trim($column, ' `');

        [$schema, $table] = $this->aliasMap[$alias] ?? [null, null];

        // If alias is actually the table name (no alias), fall back.
        if (!$table) {
            // best-effort: treat alias as table name in current DB
            if ($this->tableExists($alias, null)) {
                $schema = null;
                $table = $alias;
            } else {
                return null;
            }
        }

        // 1) original
        if ($this->columnExists($schema, $table, $column)) {
            return $column;
        }

        // 2) if ends with _id, try snake⇄camel before _id
        if (str_ends_with($column, '_id')) {
            $alt = $this->snakeToCamelId($column);
            if ($alt !== $column && $this->columnExists($schema, $table, $alt)) {
                return $alt;
            }
            $alt2 = $this->camelToSnakeId($column);
            if ($alt2 !== $column && $this->columnExists($schema, $table, $alt2)) {
                return $alt2;
            }
        }

        // 3) no luck
        return null;
    }

    /** Rewrite alias.column occurrences inside a clause using resolveColumnForAlias(). */
    protected function normalizeColumnsClause(string $clause, bool $qualifyBare = true): string
    {
        // Pass 1: alias.column  (m.project_gallery_id → m.`projectGallery_id`)
        $reAliased = '/(?<![\w`])(`?)([A-Za-z0-9_]+)\1\.(\`?)([A-Za-z0-9_]+)\3(?![\w`])/';
        $clause = preg_replace_callback($reAliased, function ($m) {
            $aliasRaw = $m[2];
            $colRaw   = $m[4];
            $resolved = $this->resolveColumnForAlias($aliasRaw, $colRaw);
            if ($resolved === null || $resolved === $colRaw) {
                return $m[0];
            }
            $aliasToken = ($m[1] ?: '') . $aliasRaw . ($m[1] ?: '');
            return $aliasToken . '.' . $this->quoteIdent($resolved);
        }, $clause);

        // Pass 2: bare *_id  (project_gallery_id → m.`projectGallery_id` OR just ``projectGallery_id``)
        $reBare = '/(?<![:\.\w`])(`?)([A-Za-z0-9_]+_id)\1(?![\w`])/';
        $clause = preg_replace_callback($reBare, function ($m) use ($qualifyBare) {
            $token = $m[2];
            $match = $this->findAliasForColumn($token);
            if (!$match) {
                return $m[0];
            }
            [$alias, $resolvedCol] = $match;
            return $qualifyBare
                ? $this->quoteIdent($alias) . '.' . $this->quoteIdent($resolvedCol)
                : $this->quoteIdent($resolvedCol);
        }, $clause);

        return $clause;
    }
}
