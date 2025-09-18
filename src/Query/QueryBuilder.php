<?php
declare(strict_types=1);

namespace MonkeysLegion\Query;

use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Database\MySQL\Connection;
use MonkeysLegion\Entity\Hydrator;
use PDO;

/**
 * QueryBuilder — a fluent SQL builder supporting SELECT/INSERT/UPDATE/DELETE
 * with joins, conditions, grouping, ordering, pagination, and transactions.
 */
final class QueryBuilder
{
    /** @var array<string,mixed> */
    private array $parts = [
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
    private array $params = [];

    /** @var int */
    private int $counter = 0;

    /** @var bool */
    private bool $inTransaction = false;

    private bool $preflightDone = false;

    /**
     * Constructor.
     *
     * @param ConnectionInterface $conn Database connection instance.
     */
    public function __construct(private ConnectionInterface $conn) {}

    /**
     * Sets the SELECT columns.
     */
    public function select(string|array $columns = ['*']): self
    {
        $this->parts['select'] = is_array($columns)
            ? implode(', ', $columns)
            : $columns;
        return $this;
    }

    /**
     * Adds columns to existing SELECT.
     */
    public function addSelect(string|array $columns): self
    {
        $existing = $this->parts['select'] === '*' ? [] : explode(', ', $this->parts['select']);
        $new = is_array($columns) ? $columns : [$columns];
        $this->parts['select'] = implode(', ', array_merge($existing, $new));
        return $this;
    }

    /**
     * Adds DISTINCT to the SELECT statement.
     */
    public function distinct(): self
    {
        $this->parts['distinct'] = true;
        return $this;
    }

    /**
     * Sets the FROM clause.
     */
    public function from(string $table, ?string $alias = null): self
    {
        $this->parts['from'] = $alias ? "$table AS $alias" : $table;
        return $this;
    }

    public function join(
        string $table,
        string $alias,
        string $first,
        string $operator,
        string $second,
        string $type = 'INNER'
    ): self {
        $clause = strtoupper($type) . " JOIN $table AS $alias ON $first $operator $second";
        $this->parts['joins'][] = $clause;
        return $this;
    }

    /**
     * Adds an INNER JOIN clause.
     */
    public function innerJoin(string $table, string $alias, string $first, string $operator, string $second): self
    {
        return $this->join($table, $alias, $first, $operator, $second, 'INNER');
    }

    /**
     * Adds a LEFT JOIN clause.
     */
    public function leftJoin(string $table, string $alias, string $first, string $operator, string $second): self
    {
        return $this->join($table, $alias, $first, $operator, $second, 'LEFT');
    }

    /**
     * Adds a RIGHT JOIN clause.
     */
    public function rightJoin(string $table, string $alias, string $first, string $operator, string $second): self
    {
        return $this->join($table, $alias, $first, $operator, $second, 'RIGHT');
    }

    /**
     * Adds a WHERE condition.
     */
    public function where(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column $operator $placeholder",
        ];
        return $this;
    }

    /**
     * Adds an AND condition to the WHERE clause.
     */
    public function andWhere(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type' => 'AND', 'expr' => "$column $operator $placeholder"];
        return $this;
    }

    /**
     * Adds an OR condition to the WHERE clause.
     */
    public function orWhere(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type' => 'OR', 'expr' => "$column $operator $placeholder"];
        return $this;
    }

    /**
     * Adds a raw WHERE condition.
     */
    public function whereRaw(string $sql, array $params = []): self
    {
        foreach ($params as $value) {
            $placeholder = $this->addParam($value);
            $sql = preg_replace('/\?/', $placeholder, $sql, 1);
        }
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => $sql,
        ];
        return $this;
    }

    /**
     * Adds a WHERE IN condition.
     */
    public function whereIn(string $column, array $values): self
    {
        if (empty($values)) {
            $this->parts['where'][] = ['type' => empty($this->parts['where']) ? '' : 'AND', 'expr' => '1=0'];
            return $this;
        }

        $placeholders = array_map(fn($v) => $this->addParam($v), $values);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column IN (" . implode(', ', $placeholders) . ")",
        ];
        return $this;
    }

    /**
     * Adds a WHERE NOT IN condition.
     */
    public function whereNotIn(string $column, array $values): self
    {
        if (empty($values)) {
            return $this;
        }

        $placeholders = array_map(fn($v) => $this->addParam($v), $values);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column NOT IN (" . implode(', ', $placeholders) . ")",
        ];
        return $this;
    }

    /**
     * Adds a WHERE BETWEEN condition.
     */
    public function whereBetween(string $column, mixed $min, mixed $max): self
    {
        $minPlaceholder = $this->addParam($min);
        $maxPlaceholder = $this->addParam($max);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column BETWEEN $minPlaceholder AND $maxPlaceholder",
        ];
        return $this;
    }

    /**
     * Adds a WHERE IS NULL condition with proper handling.
     */
    public function whereNull(string $column): self
    {
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column IS NULL",
        ];
        return $this;
    }

    /**
     * Adds a WHERE IS NOT NULL condition with proper handling.
     */
    public function whereNotNull(string $column): self
    {
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column IS NOT NULL",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE IS NULL condition.
     */
    public function orWhereNull(string $column): self
    {
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column IS NULL",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE IS NOT NULL condition.
     */
    public function orWhereNotNull(string $column): self
    {
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column IS NOT NULL",
        ];
        return $this;
    }

    /**
     * Adds a WHERE EXISTS condition.
     */
    public function whereExists(string $subquery, array $params = []): self
    {
        foreach ($params as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "EXISTS ($subquery)",
        ];
        return $this;
    }

    /**
     * Adds a WHERE NOT EXISTS condition.
     */
    public function whereNotExists(string $subquery, array $params = []): self
    {
        foreach ($params as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "NOT EXISTS ($subquery)",
        ];
        return $this;
    }

    /**
     * Adds a WHERE LIKE condition.
     */
    public function whereLike(string $column, string $pattern): self
    {
        $placeholder = $this->addParam($pattern);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column LIKE $placeholder",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE LIKE condition.
     */
    public function orWhereLike(string $column, string $pattern): self
    {
        $placeholder = $this->addParam($pattern);
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column LIKE $placeholder",
        ];
        return $this;
    }

    /**
     * Groups WHERE conditions with parentheses.
     */
    public function whereGroup(callable $callback): self
    {
        $subBuilder = new self($this->conn);
        $callback($subBuilder);

        if (!empty($subBuilder->parts['where'])) {
            $clauses = [];
            foreach ($subBuilder->parts['where'] as $i => $w) {
                $prefix = $i && $w['type'] ? ' ' . $w['type'] . ' ' : '';
                $clauses[] = $prefix . $w['expr'];
            }

            $this->parts['where'][] = [
                'type' => empty($this->parts['where']) ? '' : 'AND',
                'expr' => '(' . implode('', $clauses) . ')',
            ];

            // Merge params from subbuilder
            foreach ($subBuilder->params as $key => $value) {
                // Avoid key collision by checking if key exists
                if (isset($this->params[$key])) {
                    // Generate a new unique key
                    $newKey = $key . '_g' . $this->counter++;
                    // Update the expression to use the new key
                    $this->parts['where'][count($this->parts['where']) - 1]['expr'] =
                        str_replace($key, $newKey, $this->parts['where'][count($this->parts['where']) - 1]['expr']);
                    $this->params[$newKey] = $value;
                } else {
                    $this->params[$key] = $value;
                }
            }

            // Update counter to avoid future collisions
            if ($subBuilder->counter > $this->counter) {
                $this->counter = $subBuilder->counter;
            }
        }

        return $this;
    }

    /**
     * Groups WHERE conditions with OR (for OR groups).
     */
    public function orWhereGroup(callable $callback): self
    {
        $subBuilder = new self($this->conn);
        $callback($subBuilder);

        if (!empty($subBuilder->parts['where'])) {
            $clauses = [];
            foreach ($subBuilder->parts['where'] as $i => $w) {
                $prefix = $i && $w['type'] ? ' ' . $w['type'] . ' ' : '';
                $clauses[] = $prefix . $w['expr'];
            }

            $this->parts['where'][] = [
                'type' => empty($this->parts['where']) ? '' : 'OR',
                'expr' => '(' . implode('', $clauses) . ')',
            ];

            // Merge params from subbuilder
            foreach ($subBuilder->params as $key => $value) {
                if (isset($this->params[$key])) {
                    $newKey = $key . '_g' . $this->counter++;
                    $this->parts['where'][count($this->parts['where']) - 1]['expr'] =
                        str_replace($key, $newKey, $this->parts['where'][count($this->parts['where']) - 1]['expr']);
                    $this->params[$newKey] = $value;
                } else {
                    $this->params[$key] = $value;
                }
            }

            if ($subBuilder->counter > $this->counter) {
                $this->counter = $subBuilder->counter;
            }
        }

        return $this;
    }

    /**
     * Adds a GROUP BY clause.
     */
    public function groupBy(string ...$columns): self
    {
        $this->parts['groupBy'] = array_unique([...$this->parts['groupBy'], ...$columns]);
        return $this;
    }

    /**
     * Adds a HAVING clause.
     */
    public function having(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['having'][] = "$column $operator $placeholder";
        return $this;
    }

    /**
     * Adds a raw HAVING clause.
     */
    public function havingRaw(string $sql, array $params = []): self
    {
        foreach ($params as $value) {
            $placeholder = $this->addParam($value);
            $sql = preg_replace('/\?/', $placeholder, $sql, 1);
        }
        $this->parts['having'][] = $sql;
        return $this;
    }

    /**
     * Adds an ORDER BY clause.
     */
    public function orderBy(string $column, string $direction = 'ASC'): self
    {
        $this->parts['orderBy'][] = "$column " . strtoupper($direction);
        return $this;
    }

    /**
     * Orders by raw SQL expression.
     */
    public function orderByRaw(string $sql): self
    {
        $this->parts['orderBy'][] = $sql;
        return $this;
    }

    /**
     * Sets the LIMIT for the query.
     */
    public function limit(int $limit): self
    {
        $this->parts['limit'] = max(0, $limit);
        return $this;
    }

    /**
     * Sets the OFFSET for the query.
     */
    public function offset(int $offset): self
    {
        $this->parts['offset'] = max(0, $offset);
        return $this;
    }

    /**
     * Sets pagination (convenience method).
     */
    public function paginate(int $page, int $perPage = 15): self
    {
        $page = max(1, $page);
        return $this->limit($perPage)->offset(($page - 1) * $perPage);
    }

    /**
     * Adds a UNION clause.
     */
    public function union(string $sql, array $params = [], bool $all = false): self
    {
        $this->parts['unions'][] = [
            'sql' => $sql,
            'params' => $params,
            'all' => $all,
        ];
        return $this;
    }

    /**
     * Sets a custom SQL statement.
     */
    public function custom(string $sql, array $params = []): self
    {
        $this->parts['custom'] = $sql;
        $this->params = $params;
        return $this;
    }

    /**
     * Inserts a new row into the specified table.
     */
    public function insert(string $table, array $data): int
    {
        if (empty($data)) {
            throw new \InvalidArgumentException("Cannot insert empty data");
        }

        $cols = implode(', ', array_keys($data));
        $phs = implode(', ', array_map(fn(string $k) => ":$k", array_keys($data)));
        $sql = "INSERT INTO $table ($cols) VALUES ($phs)";

        $stmt = $this->conn->pdo()->prepare($sql);
        $bound = array_combine(
            array_map(fn(string $k) => ":$k", array_keys($data)),
            $data
        );

        if (!$stmt->execute($bound)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Insert failed: $state/$code – $msg");
        }

        $id = (int) $this->conn->pdo()->lastInsertId();
        $this->reset();
        return $id;
    }

    /**
     * Inserts multiple rows into the specified table.
     */
    public function insertBatch(string $table, array $rows): int
    {
        if (empty($rows)) {
            return 0;
        }

        $cols = array_keys(reset($rows));
        $colsStr = implode(', ', $cols);

        $values = [];
        $params = [];
        $counter = 0;

        foreach ($rows as $row) {
            $placeholders = [];
            foreach ($cols as $col) {
                $key = ":p{$counter}";
                $placeholders[] = $key;
                $params[$key] = $row[$col] ?? null;
                $counter++;
            }
            $values[] = '(' . implode(', ', $placeholders) . ')';
        }

        $sql = "INSERT INTO $table ($colsStr) VALUES " . implode(', ', $values);
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Batch insert failed: $state/$code – $msg");
        }

        $count = $stmt->rowCount();
        $this->reset();
        return $count;
    }

    /**
     * Updates rows in the specified table.
     */
    public function update(string $table, array $data): self
    {
        if (empty($data)) {
            throw new \InvalidArgumentException("Cannot update with empty data");
        }

        $sets = implode(', ', array_map(fn(string $k) => "$k = :set_$k", array_keys($data)));
        $this->parts['custom'] = "UPDATE $table SET $sets";

        // Prefix params to avoid collision with WHERE params
        foreach ($data as $key => $value) {
            $this->params[":set_$key"] = $value;
        }

        return $this;
    }

    /**
     * Deletes rows from the specified table.
     */
    public function delete(string $table): self
    {
        $this->parts['custom'] = "DELETE FROM $table";
        return $this;
    }

    /**
     * Executes the query and returns the number of affected rows.
     */
    public function execute(): int
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query execution failed: $state/$code – $msg");
        }

        $count = $stmt->rowCount();
        $this->reset();
        return $count;
    }

    /**
     * @throws \Throwable
     */
    public function count(): int
    {
        $t0 = microtime(true);

        // Duplicate and resolve tables before SQL generation
        $countQb = $this->duplicate();

        try {
            $countQb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log('[qb.count] preflightResolveTables FAILED: ' . $e->getMessage());
            throw $e;
        }

        $hasGroupingOrDistinct = !empty($countQb->parts['groupBy']) || !empty($countQb->parts['distinct']);

        try {
            if ($hasGroupingOrDistinct) {
                // Build inner query without limit/offset/order
                $countQb->parts['limit']   = null;
                $countQb->parts['offset']  = null;
                $countQb->parts['orderBy'] = [];
                $innerSql    = $countQb->toSql();
                $innerParams = $countQb->params;
                $sql  = "SELECT COUNT(*) AS cnt FROM ($innerSql) AS count_subquery";
                $stmt = $this->conn->pdo()->prepare($sql);
                $ok = $stmt->execute($innerParams);

                if (!$ok) {
                    [$state, $code, $msg] = $stmt->errorInfo();
                    error_log("[qb.count] OUTER errorInfo: $state/$code – $msg");
                }

                $row = $stmt->fetch(PDO::FETCH_ASSOC);
                $cnt = (int)($row['cnt'] ?? 0);

                return $cnt;
            }

            // SIMPLE count
            $countQb->parts['select']  = 'COUNT(*) AS cnt';
            $countQb->parts['orderBy'] = [];
            $countQb->parts['limit']   = null;
            $countQb->parts['offset']  = null;

            $sql = $countQb->toSql();

            $stmt = $this->conn->pdo()->prepare($sql);
            $ok = $stmt->execute($countQb->params);

            if (!$ok) {
                [$state, $code, $msg] = $stmt->errorInfo();
                error_log("[qb.count] SIMPLE errorInfo: $state/$code – $msg");
            }

            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $cnt = (int)($row['cnt'] ?? 0);

            return $cnt;

        } catch (\PDOException $e) {
            error_log('[qb.count] PDOException: ' . $e->getMessage() . ' code=' . $e->getCode());
            throw $e;
        } catch (\Throwable $e) {
            error_log('[qb.count] Throwable: ' . $e->getMessage() . ' code=' . $e->getCode());
            throw $e;
        }
    }

    /* ===================== helpers (with logs) ===================== */

// Deterministic mapping first. Extend at bootstrap via setTableMap().
    private array $tableMap = [
        // 'messages' => 'message',
    ];

    public function setTableMap(array $map): void
    {
        $this->tableMap = $map + $this->tableMap;
    }

    /**
     * Resolve FROM and JOIN tables before SQL generation.
     * Logs BEFORE/AFTER for from/fromTable and each join.
     */
    private function preflightResolveTables(): self
    {
        // ---------- FROM ----------
        if (!empty($this->parts['from']) && is_string($this->parts['from'])) {
            $raw = trim($this->parts['from']);

            // ref = (schema.table | table), alias optional (with or without AS)
            $reFrom = '/^\s*((?:`?\w+`?\.)?`?\w+`?)\s*(?:AS\s+|\s+)?(`?\w+`?)?\s*$/i';
            if (preg_match($reFrom, $raw, $m)) {
                $ref   = $m[1];                  // schema.table or table
                $alias = isset($m[2]) ? $m[2] : '';

                // Parse ref into schema + table
                [$schema, $table] = $this->parseQualifiedRef($ref);

                // Resolve plural/singular (uses your existing mapOrResolve)
                $resolved = $this->mapOrResolve($table);

                // Rebuild qualified FROM with original schema + alias
                $qualified = $this->quoteQualified($schema, $resolved) . ($alias ? ' ' . $alias : '');
                $this->parts['from'] = $qualified;
            }
        }

        // ---------- JOINs (string form like: 'LEFT JOIN ml_mail.domains d ON ...') ----------
        if (!empty($this->parts['joins']) && is_array($this->parts['joins'])) {
            foreach ($this->parts['joins'] as $i => $joinStr) {
                if (!is_string($joinStr)) {
                    continue;
                }

                $orig = $joinStr;

                // Capture: JOIN <ref> [AS] <alias> (until ON|USING)
                $reJoin = '/\bJOIN\s+((?:`?\w+`?\.)?`?\w+`?)' .          // 1: ref
                    '(?:\s+(?:AS\s+)?(`?\w+`?))?' .                 // 2: alias (optional)
                    '(\s+(?:ON|USING)\b)/i';                        // 3: " ON" or " USING"

                $joinStr = preg_replace_callback($reJoin, function ($mm) use ($i, $orig) {
                    $ref        = $mm[1];
                    $aliasChunk = isset($mm[2]) ? (' ' . $mm[2]) : '';
                    $afterKW    = $mm[3]; // includes leading space + ON/USING

                    [$schema, $table] = $this->parseQualifiedRef($ref);

                    $resolved   = $this->mapOrResolve($table);
                    $qualified  = $this->quoteQualified($schema, $resolved);
                    $replacement = 'JOIN ' . $qualified . $aliasChunk . $afterKW;

                    return $replacement;
                }, $joinStr, 1);

                if ($orig !== $joinStr) {
                    $this->parts['joins'][$i] = $joinStr;
                }
            }
        }

        return $this;
    }

    /** Parse "schema.table" or "table" (with/without backticks). */
    private function parseQualifiedRef(string $ref): array
    {
        $ref = trim($ref);
        // Matches: `schema`.`table`, schema.`table`, `schema`.table, schema.table, `table`, table
        if (preg_match('/^\s*(?:`?(\w+)`?\.)?`?(\w+)`?\s*$/', $ref, $m)) {
            $schema = $m[1] ?? null;
            $table  = $m[2];
            return [$schema, $table];
        }
        // Fallback: treat entire ref as table
        return [null, $ref];
    }

    /** Quote qualified name preserving schema when present. */
    private function quoteQualified(?string $schema, string $table): string
    {
        return $schema
            ? $this->quoteIdent($schema) . '.' . $this->quoteIdent($table)
            : $this->quoteIdent($table);
    }

    /** Map first; if not found, try singular/plural fallbacks using information_schema. */
    private function mapOrResolve(string $table, ?string $schema = null): string
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

    private function tableExists(string $table, ?string $schema = null): bool
    {
        try {
            $sql = "SELECT 1
                  FROM information_schema.tables
                 WHERE table_name = :t
                   AND table_schema = COALESCE(:s, DATABASE())
                 LIMIT 1";
            $stmt = $this->conn->pdo()->prepare($sql);
            $ok   = $stmt->execute([':t' => $table, ':s' => $schema]);

            if (!$ok) {
                [$state, $code, $msg] = $stmt->errorInfo();
                error_log("[qb.exists] errorInfo for '" . ($schema ? "$schema.$table" : $table) . "': $state/$code – $msg");
                return false;
            }

            $exists = (bool)$stmt->fetchColumn();
            return $exists;
        } catch (\Throwable $e) {
            error_log("[qb.exists] Throwable for '" . ($schema ? "$schema.$table" : $table) . "': " . $e->getMessage());
            return false;
        }
    }

    /**
     * Parse a possibly-qualified identifier like:
     *   ml_mail.domains, `ml_mail`.`domains`, `domains`, domains
     * Returns [schema|null, table]
     */
    private function parseQualified(string $ref): array
    {
        $ref = trim($ref);
        if (preg_match('~^\s*(?:`?([A-Za-z0-9_]+)`?\.)?`?([A-Za-z0-9_]+)`?\s*$~', $ref, $m)) {
            $schema = $m[1] ?? null;
            $table  = $m[2];
            return [$schema, $table];
        }
        return [null, $ref];
    }

    private function quoteIdent(string $ident): string
    {
        $q = '`' . str_replace('`', '``', $ident) . '`';
        return $q;
    }

    /**
     * Gets the sum of a column.
     */
    public function sum(string $column): float
    {
        return $this->aggregate('SUM', $column);
    }

    /**
     * Gets the average of a column.
     */
    public function avg(string $column): float
    {
        return $this->aggregate('AVG', $column);
    }

    /**
     * Gets the minimum value of a column.
     */
    public function min(string $column): mixed
    {
        return $this->aggregateRaw('MIN', $column);
    }

    /**
     * Gets the maximum value of a column.
     */
    public function max(string $column): mixed
    {
        return $this->aggregateRaw('MAX', $column);
    }

    /**
     * Performs an aggregate function.
     */
    private function aggregate(string $function, string $column): float
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
    private function aggregateRaw(string $function, string $column): mixed
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
     * Checks if any rows exist.
     */
    public function exists(): bool
    {
        $originalSelect = $this->parts['select'];
        $originalLimit = $this->parts['limit'];
        $originalOrderBy = $this->parts['orderBy'];
        $originalOffset = $this->parts['offset'];

        $this->parts['select'] = '1';
        $this->parts['limit'] = 1;
        $this->parts['orderBy'] = [];  // Not needed for existence check
        $this->parts['offset'] = null;  // Not needed for existence check

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Exists query failed: $state/$code – $msg");
        }

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->parts['select'] = $originalSelect;
        $this->parts['limit'] = $originalLimit;
        $this->parts['orderBy'] = $originalOrderBy;
        $this->parts['offset'] = $originalOffset;

        return $result !== false;
    }

    /**
     * Fetches all results as an array of objects.
     */
    public function fetchAll(string $class = 'stdClass'): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->reset();

        if ($class !== 'stdClass' && class_exists($class)) {
            return array_map(
                fn(array $r) => Hydrator::hydrate($class, $r),
                $rows
            );
        }

        return $rows;
    }

    /**
     * Fetches a single row as an object.
     */
    public function fetch(string $class = 'stdClass'): object|false
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->reset();

        if (!$row) {
            return false;
        }

        if ($class !== 'stdClass' && class_exists($class)) {
            return Hydrator::hydrate($class, $row);
        }

        return (object) $row;
    }

    /**
     * Fetches a single row as an associative array.
     */
    public function fetchOne(string $sql, array $params = []): array|null
    {
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->reset();
        return $row ?: null;
    }

    /**
     * Fetches a single column value.
     */
    public function value(string $column): mixed
    {
        $originalSelect = $this->parts['select'];
        $this->parts['select'] = $column;

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);

        $result = $stmt->fetchColumn();
        $this->parts['select'] = $originalSelect;

        return $result;
    }

    /**
     * Fetches values from a single column as an array.
     */
    public function pluck(string $column, ?string $key = null): array
    {
        $originalSelect = $this->parts['select'];
        $this->parts['select'] = $key ? "$key, $column" : $column;

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->parts['select'] = $originalSelect;

        if ($key) {
            $result = [];
            foreach ($rows as $row) {
                $result[$row[$key]] = $row[$column];
            }
            return $result;
        }

        return array_column($rows, $column);
    }

    /**
     * Starts a database transaction.
     */
    public function beginTransaction(): self
    {
        if (!$this->inTransaction) {
            $this->conn->pdo()->beginTransaction();
            $this->inTransaction = true;
        }
        return $this;
    }

    /**
     * Commits the current transaction.
     */
    public function commit(): self
    {
        if ($this->inTransaction) {
            $this->conn->pdo()->commit();
            $this->inTransaction = false;
        }
        return $this;
    }

    /**
     * Rolls back the current transaction.
     */
    public function rollback(): self
    {
        if ($this->inTransaction) {
            $this->conn->pdo()->rollBack();
            $this->inTransaction = false;
        }
        return $this;
    }

    /**
     * Executes a callback within a transaction.
     */
    public function transaction(callable $callback): mixed
    {
        $this->beginTransaction();

        try {
            $result = $callback($this);
            $this->commit();
            return $result;
        } catch (\Exception $e) {
            $this->rollback();
            throw $e;
        }
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
     * Adds a parameter to the query and returns its placeholder.
     */
    private function addParam(mixed $value): string
    {
        $key = ':p' . $this->counter++;
        $this->params[$key] = $value;
        return $key;
    }

    /**
     * Resets the query builder to its initial state.
     */
    public function reset(): self
    {
        $this->parts = [
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
        $this->params = [];
        $this->counter = 0;
        return $this;
    }

    /**
     * Creates a deep duplicate of the query builder for reuse.
     */
    public function duplicate(): self
    {
        $clone = new self($this->conn);
        $clone->parts = $this->deepCopyArray($this->parts);
        $clone->params = $this->deepCopyArray($this->params);
        $clone->counter = $this->counter;
        return $clone;
    }

    /**
     * Deep copy an array.
     */
    private function deepCopyArray(array $array): array
    {
        $copy = [];
        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopyArray($value);
            } elseif (is_object($value)) {
                $copy[$key] = clone $value;
            } else {
                $copy[$key] = $value;
            }
        }
        return $copy;
    }

    /**
     * Get the current bound parameters for debugging.
     */
    public function getParams(): array
    {
        return $this->params;
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
     * Expose the underlying PDO so low-level operations can be performed.
     */
    public function pdo(): \PDO
    {
        return $this->conn->pdo();
    }

    /**
     * Expose the Connection.
     */
    public function connection(): Connection
    {
        return $this->conn;
    }
}