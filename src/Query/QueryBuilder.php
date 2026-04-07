<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Query;

use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Database\Contracts\ConnectionManagerInterface;
use MonkeysLegion\Database\Types\DatabaseDriver;
use MonkeysLegion\Query\Clause\GroupByClause;
use MonkeysLegion\Query\Clause\HavingClause;
use MonkeysLegion\Query\Clause\JoinClause;
use MonkeysLegion\Query\Clause\LimitOffsetClause;
use MonkeysLegion\Query\Clause\OrderByClause;
use MonkeysLegion\Query\Clause\SelectClause;
use MonkeysLegion\Query\Clause\UnionClause;
use MonkeysLegion\Query\Clause\WhereClause;
use MonkeysLegion\Query\Compiler\GrammarInterface;
use MonkeysLegion\Query\Compiler\QueryCompiler;
use MonkeysLegion\Query\Contracts\ExpressionInterface;
use MonkeysLegion\Query\Enums\JoinType;
use MonkeysLegion\Query\Enums\Operator;
use MonkeysLegion\Query\Enums\SortDirection;
use MonkeysLegion\Query\Enums\WhereBoolean;
use MonkeysLegion\Query\RawExpression;
use PDO;
use PDOStatement;
use Psr\SimpleCache\CacheInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Performance-first fluent SQL query builder.
 *
 * Architecture:
 *  • Each fluent method appends a typed clause VO — no string work until execution
 *  • Read queries route to ConnectionManager::read(), DML to ::write()
 *  • Structural SQL caching via QueryCompiler eliminates repeated compilation
 *  • Statement caching reuses PDOStatement objects for identical SQL
 *
 * PHP 8.4 features:
 *  • Property hooks for computed observability ($bindingCount, $connectionName)
 *  • Asymmetric visibility (public private(set) readonly)
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class QueryBuilder
{
    // ── Clause State ────────────────────────────────────────────

    /** @var list<string|ExpressionInterface> */
    private array $selectColumns = ['*'];

    private bool $distinct = false;

    private string $fromTable = '';

    /** @var list<JoinClause> */
    private array $joins = [];

    /** @var list<WhereClause> */
    private array $wheres = [];

    /** @var list<OrderByClause> */
    private array $orders = [];

    private ?GroupByClause $groupBy = null;

    /** @var list<HavingClause> */
    private array $havings = [];

    private ?int $limitValue = null;
    private ?int $offsetValue = null;

    /** @var list<UnionClause> */
    private array $unions = [];

    // ── Caching ─────────────────────────────────────────────────

    private ?CacheInterface $resultCache = null;
    private int $resultCacheTtl = 60;
    private ?string $resultCacheKey = null;

    /** @var array<string, PDOStatement> SQL hash → cached statement */
    private static array $stmtCache = [];

    // ── Resolved State ──────────────────────────────────────────

    private GrammarInterface $grammar;
    private string $resolvedName;

    // ── Computed Properties (PHP 8.4) ───────────────────────────

    /** Total number of parameter bindings in the current query. */
    public int $bindingCount {
        get {
            $count = 0;
            foreach ($this->wheres as $w) {
                $count += count($w->getBindings());
            }
            foreach ($this->joins as $j) {
                $count += count($j->getBindings());
            }
            foreach ($this->havings as $h) {
                $count += count($h->getBindings());
            }
            return $count;
        }
    }

    /** The resolved connection name. */
    public string $connectionName {
        get => $this->resolvedName;
    }

    // ── Constructor ─────────────────────────────────────────────

    public function __construct(
        private readonly ConnectionManagerInterface $manager,
        ?string $connectionName = null,
    ) {
        $this->resolvedName = $connectionName ?? $manager->getDefaultConnectionName();
        $driver = $manager->connection($this->resolvedName)->getDriver();
        $this->grammar = QueryCompiler::grammarFor($driver);
    }

    // ── SELECT ──────────────────────────────────────────────────

    /**
     * Set the columns for the SELECT.
     *
     * @param list<string|ExpressionInterface> $columns
     */
    public function select(array $columns = ['*']): self
    {
        $this->selectColumns = $columns;
        return $this;
    }

    /**
     * Add a raw SELECT expression.
     */
    public function selectRaw(string $expression, array $bindings = []): self
    {
        $this->selectColumns[] = new RawExpression($expression, $bindings);
        return $this;
    }

    /**
     * Apply DISTINCT to the SELECT.
     */
    public function distinct(): self
    {
        $this->distinct = true;
        return $this;
    }

    // ── FROM ────────────────────────────────────────────────────

    /**
     * Set the FROM table.
     */
    public function from(string $table, ?string $alias = null): self
    {
        $this->fromTable = $alias !== null ? "{$table} AS {$alias}" : $table;
        return $this;
    }

    /**
     * Alias for from() — sets the primary table.
     */
    public function table(string $table, ?string $alias = null): self
    {
        return $this->from($table, $alias);
    }

    // ── JOIN ────────────────────────────────────────────────────

    /**
     * Add a join with callback-based conditions.
     *
     * @param string                    $table    Table to join.
     * @param callable(JoinBuilder): void $callback Join condition builder.
     * @param string|null               $alias    Optional table alias.
     * @param JoinType                  $type     Join type (default: INNER).
     */
    public function join(
        string $table,
        callable $callback,
        ?string $alias = null,
        JoinType $type = JoinType::Inner,
    ): self {
        $builder = new JoinBuilder($table, $alias, $type);
        $callback($builder);

        $this->joins[] = new JoinClause(
            type: $builder->getType(),
            table: $builder->getTable(),
            alias: $builder->getAlias(),
            conditions: $builder->getConditions(),
            bindings: $builder->getBindings(),
        );

        return $this;
    }

    /**
     * Add a simple join with column-to-column condition.
     */
    public function joinOn(
        string $table,
        string $first,
        string $operator,
        string $second,
        ?string $alias = null,
        JoinType $type = JoinType::Inner,
    ): self {
        return $this->join($table, fn(JoinBuilder $j) => $j->on($first, $operator, $second), $alias, $type);
    }

    /**
     * Add a LEFT JOIN.
     */
    public function leftJoin(string $table, callable $callback, ?string $alias = null): self
    {
        return $this->join($table, $callback, $alias, JoinType::Left);
    }

    /**
     * Add a simple LEFT JOIN with column-to-column condition.
     */
    public function leftJoinOn(string $table, string $first, string $operator, string $second, ?string $alias = null): self
    {
        return $this->joinOn($table, $first, $operator, $second, $alias, JoinType::Left);
    }

    /**
     * Add a RIGHT JOIN.
     */
    public function rightJoin(string $table, callable $callback, ?string $alias = null): self
    {
        return $this->join($table, $callback, $alias, JoinType::Right);
    }

    /**
     * Add a CROSS JOIN.
     */
    public function crossJoin(string $table, ?string $alias = null): self
    {
        $this->joins[] = new JoinClause(type: JoinType::Cross, table: $table, alias: $alias);
        return $this;
    }

    // ── WHERE ───────────────────────────────────────────────────

    /**
     * Add a WHERE condition.
     */
    public function where(string $column, string $operator, mixed $value = null): self
    {
        $this->wheres[] = new WhereClause($column, Operator::fromLoose($operator), $value, WhereBoolean::And);
        return $this;
    }

    /**
     * Add an OR WHERE condition.
     */
    public function orWhere(string $column, string $operator, mixed $value = null): self
    {
        $this->wheres[] = new WhereClause($column, Operator::fromLoose($operator), $value, WhereBoolean::Or);
        return $this;
    }

    /**
     * Add a WHERE IN condition.
     */
    public function whereIn(string $column, array $values): self
    {
        $this->wheres[] = new WhereClause($column, Operator::In, $values, WhereBoolean::And);
        return $this;
    }

    /**
     * Add a WHERE NOT IN condition.
     */
    public function whereNotIn(string $column, array $values): self
    {
        $this->wheres[] = new WhereClause($column, Operator::NotIn, $values, WhereBoolean::And);
        return $this;
    }

    /**
     * Add a WHERE BETWEEN condition.
     */
    public function whereBetween(string $column, mixed $min, mixed $max): self
    {
        $this->wheres[] = new WhereClause($column, Operator::Between, [$min, $max], WhereBoolean::And);
        return $this;
    }

    /**
     * Add a WHERE IS NULL condition.
     */
    public function whereNull(string $column): self
    {
        $this->wheres[] = new WhereClause($column, Operator::IsNull, null, WhereBoolean::And);
        return $this;
    }

    /**
     * Add a WHERE IS NOT NULL condition.
     */
    public function whereNotNull(string $column): self
    {
        $this->wheres[] = new WhereClause($column, Operator::IsNotNull, null, WhereBoolean::And);
        return $this;
    }

    /**
     * Add a WHERE EXISTS sub-query.
     */
    public function whereExists(ExpressionInterface $subQuery): self
    {
        $this->wheres[] = new WhereClause('', Operator::Exists, $subQuery, WhereBoolean::And);
        return $this;
    }

    /**
     * Add a raw WHERE expression.
     */
    public function whereRaw(string $sql, array $bindings = []): self
    {
        $this->wheres[] = new WhereClause(
            column: $sql,
            operator: Operator::Equal,
            value: null,
            boolean: WhereBoolean::And,
            bindings: $bindings,
        );
        return $this;
    }

    // ── ORDER BY ────────────────────────────────────────────────

    /**
     * Add an ORDER BY clause.
     */
    public function orderBy(string $column, string $direction = 'ASC'): self
    {
        $this->orders[] = new OrderByClause($column, SortDirection::fromLoose($direction));
        return $this;
    }

    /**
     * Convenience: ORDER BY ... DESC.
     */
    public function orderByDesc(string $column): self
    {
        return $this->orderBy($column, 'DESC');
    }

    /**
     * Convenience: ORDER BY created_at DESC (latest first).
     */
    public function latest(string $column = 'created_at'): self
    {
        return $this->orderByDesc($column);
    }

    /**
     * Convenience: ORDER BY created_at ASC (oldest first).
     */
    public function oldest(string $column = 'created_at'): self
    {
        return $this->orderBy($column, 'ASC');
    }

    // ── GROUP BY / HAVING ───────────────────────────────────────

    /**
     * Add a GROUP BY clause.
     *
     * @param string|list<string> $columns
     */
    public function groupBy(string|array $columns): self
    {
        $this->groupBy = new GroupByClause(is_array($columns) ? $columns : [$columns]);
        return $this;
    }

    /**
     * Add a HAVING clause.
     */
    public function having(string $expression, array $bindings = []): self
    {
        $this->havings[] = new HavingClause($expression, $bindings);
        return $this;
    }

    // ── LIMIT / OFFSET ──────────────────────────────────────────

    /**
     * Set the LIMIT.
     */
    public function limit(int $limit): self
    {
        $this->limitValue = $limit;
        return $this;
    }

    /**
     * Set the OFFSET.
     */
    public function offset(int $offset): self
    {
        $this->offsetValue = $offset;
        return $this;
    }

    /**
     * Convenience: set LIMIT and OFFSET for pagination.
     */
    public function forPage(int $page, int $perPage = 15): self
    {
        return $this->limit($perPage)->offset(($page - 1) * $perPage);
    }

    // ── UNION ───────────────────────────────────────────────────

    /**
     * Add a UNION query.
     */
    public function union(self $query, bool $all = false): self
    {
        $compiled = $query->compile();
        $this->unions[] = new UnionClause($compiled['sql'], $compiled['bindings'], $all);
        return $this;
    }

    /**
     * Add a UNION ALL query.
     */
    public function unionAll(self $query): self
    {
        return $this->union($query, all: true);
    }

    // ── Caching ─────────────────────────────────────────────────

    /**
     * Set a PSR-16 cache for result caching.
     */
    public function setCache(CacheInterface $cache, int $defaultTtl = 60): self
    {
        $this->resultCache = $cache;
        $this->resultCacheTtl = $defaultTtl;
        return $this;
    }

    /**
     * Enable result caching for the current query.
     */
    public function cache(int $ttl = 0, ?string $key = null): self
    {
        if ($ttl > 0) {
            $this->resultCacheTtl = $ttl;
        }
        $this->resultCacheKey = $key;
        return $this;
    }

    // ── Compilation ─────────────────────────────────────────────

    /**
     * Compile the current query into SQL + bindings without executing.
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public function compile(): array
    {
        return QueryCompiler::compileSelect(
            select: new SelectClause($this->selectColumns, $this->distinct),
            from: $this->fromTable,
            wheres: $this->wheres,
            joins: $this->joins,
            orders: $this->orders,
            groupBy: $this->groupBy,
            havings: $this->havings,
            limit: ($this->limitValue !== null || $this->offsetValue !== null)
                ? new LimitOffsetClause($this->limitValue, $this->offsetValue)
                : null,
            unions: $this->unions,
            grammar: $this->grammar,
        );
    }

    /**
     * Get the compiled SQL string (for debugging).
     */
    public function toSql(): string
    {
        return $this->compile()['sql'];
    }

    /**
     * Get the compiled SQL with bindings replaced (for debugging only).
     */
    public function toDebugSql(): string
    {
        $compiled = $this->compile();
        $sql = $compiled['sql'];

        foreach ($compiled['bindings'] as $value) {
            $quoted = is_numeric($value) ? (string) $value : "'" . addslashes((string) $value) . "'";
            $pos = strpos($sql, '?');
            if ($pos !== false) {
                $sql = substr_replace($sql, $quoted, $pos, 1);
            }
        }

        return $sql;
    }

    // ── Execution: Read Queries ─────────────────────────────────

    /**
     * Execute a SELECT and return all rows as associative arrays.
     *
     * @return list<array<string, mixed>>
     */
    public function get(): array
    {
        // Check result cache
        if ($this->resultCache !== null) {
            $cacheKey = $this->generateCacheKey();
            $cached = $this->resultCache->get($cacheKey);
            if ($cached !== null) {
                return $cached;
            }
        }

        $compiled = $this->compile();
        $stmt = $this->executeRead($compiled['sql'], $compiled['bindings']);
        $result = $stmt->fetchAll(PDO::FETCH_ASSOC);

        // Store in cache
        if ($this->resultCache !== null) {
            $this->resultCache->set($cacheKey ?? $this->generateCacheKey(), $result, $this->resultCacheTtl);
        }

        return $result;
    }

    /**
     * Execute a SELECT and return the first row.
     *
     * @return array<string, mixed>|null
     */
    public function first(): ?array
    {
        $this->limitValue = 1;
        $result = $this->get();
        return $result[0] ?? null;
    }

    /**
     * Execute a SELECT and return a single column value from the first row.
     */
    public function value(string $column): mixed
    {
        $row = $this->first();
        return $row[$column] ?? null;
    }

    /**
     * Execute a SELECT and return a single column from all rows.
     *
     * @return list<mixed>
     */
    public function pluck(string $column): array
    {
        return array_column($this->get(), $column);
    }

    /**
     * Check if any rows exist matching the current query.
     */
    public function exists(): bool
    {
        $this->limitValue = 1;
        return $this->get() !== [];
    }

    // ── Execution: Aggregates ───────────────────────────────────

    /**
     * Execute a COUNT aggregate.
     */
    public function count(string $column = '*'): int
    {
        return (int) $this->aggregate('COUNT', $column);
    }

    /**
     * Execute a SUM aggregate.
     */
    public function sum(string $column): float
    {
        return (float) $this->aggregate('SUM', $column);
    }

    /**
     * Execute an AVG aggregate.
     */
    public function avg(string $column): float
    {
        return (float) $this->aggregate('AVG', $column);
    }

    /**
     * Execute a MIN aggregate.
     */
    public function min(string $column): mixed
    {
        return $this->aggregate('MIN', $column);
    }

    /**
     * Execute a MAX aggregate.
     */
    public function max(string $column): mixed
    {
        return $this->aggregate('MAX', $column);
    }

    /**
     * Execute a generic aggregate function.
     */
    private function aggregate(string $function, string $column): mixed
    {
        $original = $this->selectColumns;
        $originalOrders = $this->orders;
        $originalLimit = $this->limitValue;
        $originalOffset = $this->offsetValue;

        $this->selectColumns = [new RawExpression("{$function}({$column}) AS aggregate")];
        $this->orders = [];
        $this->limitValue = null;
        $this->offsetValue = null;

        $row = $this->first();

        $this->selectColumns = $original;
        $this->orders = $originalOrders;
        $this->limitValue = $originalLimit;
        $this->offsetValue = $originalOffset;

        return $row['aggregate'] ?? null;
    }

    // ── Execution: DML ──────────────────────────────────────────

    /**
     * Execute an INSERT.
     *
     * @param array<string, mixed> $values Column => value pairs for a single row.
     *
     * @return string|false The last insert ID (or false).
     */
    public function insert(array $values): string|false
    {
        $columns = array_keys($values);
        $compiled = QueryCompiler::compileInsert(
            $this->fromTable,
            $columns,
            [array_values($values)],
            $this->grammar,
        );

        $this->executeWrite($compiled['sql'], $compiled['bindings']);

        return $this->writeConnection()->lastInsertId();
    }

    /**
     * Insert multiple rows at once.
     *
     * @param list<array<string, mixed>> $rows
     */
    public function insertMany(array $rows): int
    {
        if ($rows === []) {
            return 0;
        }

        $columns = array_keys($rows[0]);
        $rowValues = array_map(fn(array $row) => array_values($row), $rows);

        $compiled = QueryCompiler::compileInsert(
            $this->fromTable,
            $columns,
            $rowValues,
            $this->grammar,
        );

        return $this->executeWrite($compiled['sql'], $compiled['bindings']);
    }

    /**
     * Execute an UPDATE.
     *
     * @param array<string, mixed> $values Column => value pairs.
     *
     * @return int Number of affected rows.
     */
    public function update(array $values): int
    {
        $compiled = QueryCompiler::compileUpdate(
            $this->fromTable,
            $values,
            $this->wheres,
            $this->grammar,
        );

        return $this->executeWrite($compiled['sql'], $compiled['bindings']);
    }

    /**
     * Execute a DELETE.
     *
     * @return int Number of affected rows.
     */
    public function delete(): int
    {
        $compiled = QueryCompiler::compileDelete(
            $this->fromTable,
            $this->wheres,
            $this->grammar,
        );

        return $this->executeWrite($compiled['sql'], $compiled['bindings']);
    }

    /**
     * Execute an UPSERT.
     *
     * @param array<string, mixed>     $values         All column values.
     * @param list<string>             $updateColumns  Columns to update on conflict.
     * @param string|list<string>|null $conflictTarget Conflict column(s) (required for PG/SQLite).
     *
     * @return int Affected rows.
     */
    public function upsert(
        array $values,
        array $updateColumns,
        string|array|null $conflictTarget = null,
    ): int {
        $columns = array_keys($values);
        $compiled = QueryCompiler::compileUpsert(
            $this->fromTable,
            $columns,
            array_values($values),
            $updateColumns,
            $conflictTarget,
            $this->grammar,
        );

        return $this->executeWrite($compiled['sql'], $compiled['bindings']);
    }

    // ── Transaction Support ─────────────────────────────────────

    /**
     * Execute a callback within a database transaction on the write connection.
     *
     * @template T
     *
     * @param callable(self): T $callback
     *
     * @return T
     */
    public function transaction(callable $callback): mixed
    {
        return $this->writeConnection()->transaction(fn() => $callback($this));
    }

    // ── Cloning & Reset ─────────────────────────────────────────

    /**
     * Create a fresh query builder for the same table.
     */
    public function newQuery(): self
    {
        $qb = new self($this->manager, $this->resolvedName);
        $qb->fromTable = $this->fromTable;
        return $qb;
    }

    /**
     * Reset all clauses for query reuse.
     */
    public function reset(): self
    {
        $this->selectColumns = ['*'];
        $this->distinct = false;
        $this->joins = [];
        $this->wheres = [];
        $this->orders = [];
        $this->groupBy = null;
        $this->havings = [];
        $this->limitValue = null;
        $this->offsetValue = null;
        $this->unions = [];
        $this->resultCacheKey = null;
        return $this;
    }

    // ── Static Cache Management ─────────────────────────────────

    /**
     * Clear the PDOStatement cache.
     */
    public static function clearStatementCache(): void
    {
        self::$stmtCache = [];
    }

    // ── Private: Connection Resolution ──────────────────────────

    private function readConnection(): ConnectionInterface
    {
        return $this->manager->read($this->resolvedName);
    }

    private function writeConnection(): ConnectionInterface
    {
        return $this->manager->write($this->resolvedName);
    }

    // ── Private: Execution ──────────────────────────────────────

    /**
     * Execute a read query using the read connection.
     *
     * @param list<mixed> $bindings
     */
    private function executeRead(string $sql, array $bindings): PDOStatement
    {
        $conn = $this->readConnection();
        $stmt = $this->prepareStatement($conn, $sql);
        $stmt->execute($bindings);
        return $stmt;
    }

    /**
     * Execute a write query using the write connection.
     *
     * @param list<mixed> $bindings
     *
     * @return int Affected rows.
     */
    private function executeWrite(string $sql, array $bindings): int
    {
        $conn = $this->writeConnection();
        $stmt = $this->prepareStatement($conn, $sql);
        $stmt->execute($bindings);
        return $stmt->rowCount();
    }

    /**
     * Prepare a statement with caching.
     */
    private function prepareStatement(ConnectionInterface $conn, string $sql): PDOStatement
    {
        $key = spl_object_id($conn) . ':' . $sql;

        if (isset(self::$stmtCache[$key])) {
            return self::$stmtCache[$key];
        }

        $stmt = $conn->pdo()->prepare($sql);
        self::$stmtCache[$key] = $stmt;

        return $stmt;
    }

    /**
     * Generate a cache key for result caching.
     */
    private function generateCacheKey(): string
    {
        if ($this->resultCacheKey !== null) {
            return 'mlq:' . $this->resultCacheKey;
        }

        $compiled = $this->compile();
        return 'mlq:' . hash('xxh128', $compiled['sql'] . serialize($compiled['bindings']));
    }
}
