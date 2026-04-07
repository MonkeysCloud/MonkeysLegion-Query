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
use WeakMap;

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

    /** @phpstan-ignore-next-line (asymmetric visibility read by external collaborators) */
    public private(set) string $fromTable = '';

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

    /** @var list<string> Columns for RETURNING clause (empty = disabled). */
    private array $returningColumns = [];

    /** @var 'update'|'share'|null Pessimistic lock mode. */
    private ?string $lockMode = null;

    private bool $lockNoWait = false;

    /** CTE builder — null until first withCte() call. */
    private ?CteBuilder $cteBuilder = null;

    // ── Caching ─────────────────────────────────────────────────

    private ?CacheInterface $resultCache = null;
    private int $resultCacheTtl = 60;
    private ?string $resultCacheKey = null;

    /**
     * PDOStatement cache keyed by connection object + SQL.
     * Using WeakMap ensures stale entries are freed when the connection is GC'd (#3).
     *
     * @var \WeakMap<object, array<string, \PDOStatement>>|null
     */
    private static ?\WeakMap $stmtCache = null;

    /** Maximum number of cached statements per connection. */
    private const STMT_CACHE_MAX = 256;

    // ── Resolved State ──────────────────────────────────────────

    /** @phpstan-ignore-next-line (asymmetric visibility read by external collaborators) */
    public private(set) GrammarInterface $grammar;

    private string $resolvedName;

    // ── Computed Properties (PHP 8.4) ───────────────────────────

    /** Total number of parameter bindings in the current query (all clause types). */
    public int $bindingCount {
        get {
            $count = 0;
            // SELECT raw expression bindings
            foreach ($this->selectColumns as $col) {
                if ($col instanceof ExpressionInterface) {
                    $count += count($col->getBindings());
                }
            }
            // CTE bindings
            if ($this->cteBuilder !== null) {
                $count += count($this->cteBuilder->getBindings());
            }
            // JOIN bindings
            foreach ($this->joins as $j) {
                $count += count($j->getBindings());
            }
            // WHERE bindings
            foreach ($this->wheres as $w) {
                $count += count($w->getBindings());
            }
            // HAVING bindings
            foreach ($this->havings as $h) {
                $count += count($h->getBindings());
            }
            // ORDER BY bindings (vector distance expressions)
            foreach ($this->orders as $o) {
                $count += count($o->getBindings());
            }
            // UNION bindings
            foreach ($this->unions as $u) {
                $count += count($u->getBindings());
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
     * @param string                      $table    Table to join.
     * @param \Closure(JoinBuilder): void $callback Join condition builder.
     * @param string|null                 $alias    Optional table alias.
     * @param JoinType                    $type     Join type (default: INNER).
     */
    public function join(
        string $table,
        \Closure $callback,
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

    /**
     * Add a FULL OUTER JOIN.
     */
    public function fullJoin(string $table, \Closure $callback, ?string $alias = null): self
    {
        return $this->join($table, $callback, $alias, JoinType::Full);
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
     * Add a WHERE EXISTS sub-query.
     */
    public function whereExists(ExpressionInterface $subQuery): self
    {
        $this->wheres[] = new WhereClause('', Operator::Exists, $subQuery, WhereBoolean::And);
        return $this;
    }

    /**
     * Add an OR WHERE EXISTS sub-query.
     */
    public function orWhereExists(ExpressionInterface $subQuery): self
    {
        $this->wheres[] = new WhereClause('', Operator::Exists, $subQuery, WhereBoolean::Or);
        return $this;
    }

    /**
     * Add a WHERE IN condition.
     */
    public function whereIn(string $column, array $values): self
    {
        if (!array_is_list($values)) {
            throw new \InvalidArgumentException('whereIn() expects a list (sequential array).');
        }
        $this->wheres[] = new WhereClause($column, Operator::In, $values, WhereBoolean::And);
        return $this;
    }

    /**
     * Add an OR WHERE IN condition.
     */
    public function orWhereIn(string $column, array $values): self
    {
        if (!array_is_list($values)) {
            throw new \InvalidArgumentException('orWhereIn() expects a list (sequential array).');
        }
        $this->wheres[] = new WhereClause($column, Operator::In, $values, WhereBoolean::Or);
        return $this;
    }

    /**
     * Add a WHERE NOT IN condition.
     */
    public function whereNotIn(string $column, array $values): self
    {
        if (!array_is_list($values)) {
            throw new \InvalidArgumentException('whereNotIn() expects a list (sequential array).');
        }
        $this->wheres[] = new WhereClause($column, Operator::NotIn, $values, WhereBoolean::And);
        return $this;
    }

    /**
     * Add an OR WHERE NOT IN condition.
     */
    public function orWhereNotIn(string $column, array $values): self
    {
        if (!array_is_list($values)) {
            throw new \InvalidArgumentException('orWhereNotIn() expects a list (sequential array).');
        }
        $this->wheres[] = new WhereClause($column, Operator::NotIn, $values, WhereBoolean::Or);
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
     * Add an OR WHERE BETWEEN condition.
     */
    public function orWhereBetween(string $column, mixed $min, mixed $max): self
    {
        $this->wheres[] = new WhereClause($column, Operator::Between, [$min, $max], WhereBoolean::Or);
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
     * Add an OR WHERE IS NULL condition.
     */
    public function orWhereNull(string $column): self
    {
        $this->wheres[] = new WhereClause($column, Operator::IsNull, null, WhereBoolean::Or);
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
     * Add an OR WHERE IS NOT NULL condition.
     */
    public function orWhereNotNull(string $column): self
    {
        $this->wheres[] = new WhereClause($column, Operator::IsNotNull, null, WhereBoolean::Or);
        return $this;
    }

    /**
     * Add grouped WHERE conditions wrapped in parentheses: AND (...)
     *
     * @param \Closure(self): void $callback Receives a fresh builder to accumulate inner conditions.
     */
    public function whereGroup(\Closure $callback, WhereBoolean $boolean = WhereBoolean::And): self
    {
        $inner = new self($this->manager, $this->resolvedName);
        $callback($inner);

        $this->wheres[] = new WhereClause(
            column: '',
            operator: Operator::Group,
            value: $inner->wheres,
            boolean: $boolean,
        );

        return $this;
    }

    /**
     * Add grouped WHERE conditions wrapped in parentheses: OR (...)
     *
     * @param \Closure(self): void $callback
     */
    public function orWhereGroup(\Closure $callback): self
    {
        return $this->whereGroup($callback, WhereBoolean::Or);
    }

    /**
     * Add a raw WHERE expression.
     */
    public function whereRaw(string $sql, array $bindings = []): self
    {
        $this->wheres[] = new WhereClause(
            column: $sql,
            operator: Operator::Raw,
            value: null,
            boolean: WhereBoolean::And,
            bindings: $bindings,
        );
        return $this;
    }

    /**
     * Add a raw OR WHERE expression.
     */
    public function orWhereRaw(string $sql, array $bindings = []): self
    {
        $this->wheres[] = new WhereClause(
            column: $sql,
            operator: Operator::Raw,
            value: null,
            boolean: WhereBoolean::Or,
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
        $this->havings[] = new HavingClause($expression, $bindings, WhereBoolean::And);
        return $this;
    }

    /**
     * Add an OR HAVING clause.
     */
    public function orHaving(string $expression, array $bindings = []): self
    {
        $this->havings[] = new HavingClause($expression, $bindings, WhereBoolean::Or);
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

    // ── CTE ─────────────────────────────────────────────────────

    /**
     * Add a Common Table Expression (WITH clause) to this query.
     *
     * @param string                    $name      CTE name.
     * @param \Closure(self): self|void $callback  Receives a fresh QueryBuilder; the returned
     *                                             (or mutated) builder is used as the CTE body.
     * @param list<string>              $columns   Optional column aliases.
     * @param bool                      $recursive Whether to emit WITH RECURSIVE.
     */
    public function withCte(
        string $name,
        \Closure $callback,
        array $columns = [],
        bool $recursive = false,
    ): self {
        $this->cteBuilder ??= new CteBuilder();

        $inner = new self($this->manager, $this->resolvedName);
        $returned = $callback($inner);
        $cteQuery = ($returned instanceof self) ? $returned : $inner;

        $this->cteBuilder->addFromBuilder($name, $cteQuery, $columns, $recursive);

        return $this;
    }

    // ── RETURNING ───────────────────────────────────────────────

    /**
     * Specify columns to include in a RETURNING clause for INSERT/UPDATE/DELETE.
     * Only emitted when the grammar supports it (PostgreSQL, SQLite 3.35+).
     *
     * @param list<string> $columns Columns to return, or ['*'] for all.
     */
    public function returning(array $columns = ['*']): self
    {
        $this->returningColumns = $columns;
        return $this;
    }

    // ── Locking ─────────────────────────────────────────────────

    /**
     * Acquire an exclusive row lock: SELECT ... FOR UPDATE.
     */
    public function lockForUpdate(bool $noWait = false): self
    {
        $this->lockMode   = 'update';
        $this->lockNoWait = $noWait;
        return $this;
    }

    /**
     * Acquire a shared row lock: SELECT ... FOR SHARE / LOCK IN SHARE MODE.
     */
    public function sharedLock(bool $noWait = false): self
    {
        $this->lockMode   = 'share';
        $this->lockNoWait = $noWait;
        return $this;
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
        $ctePrefix   = $this->cteBuilder?->toSql() ?: null;
        $lockSuffix  = $this->lockMode !== null
            ? $this->grammar->compileLock($this->lockMode, $this->lockNoWait)
            : null;

        $result = QueryCompiler::compileSelect(
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
            ctePrefix: $ctePrefix,
            lockSuffix: ($lockSuffix !== '' ? $lockSuffix : null),
        );

        // Prepend CTE bindings before the main query bindings
        if ($this->cteBuilder !== null) {
            $result['bindings'] = [...$this->cteBuilder->getBindings(), ...$result['bindings']];
        }

        return $result;
    }

    /**
     * Get the compiled SQL string (for debugging).
     */
    public function toSql(): string
    {
        return $this->compile()['sql'];
    }

    /**
     * Get the compiled SQL with bindings replaced inline (for debugging only).
     *
     * @internal This method is intended for development use only.
     *           Do NOT use the output of this method for actual database queries.
     */
    public function toDebugSql(): string
    {
        $compiled = $this->compile();
        $sql = $compiled['sql'];

        foreach ($compiled['bindings'] as $value) {
            $pos = strpos($sql, '?');
            if ($pos === false) {
                break;
            }

            if ($value === null) {
                $replacement = 'NULL';
            } elseif (is_bool($value)) {
                $replacement = $value ? '1' : '0';
            } elseif (is_int($value) || is_float($value)) {
                $replacement = (string) $value;
            } else {
                // Basic escaping for debug display — NOT safe for production use.
                $replacement = "'" . str_replace(["\\", "'"], ["\\\\", "\\'"], (string) $value) . "'";
            }

            $sql = substr_replace($sql, $replacement, $pos, 1);
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
     * Uses an internal clone to avoid mutating $limitValue on the current builder.
     *
     * @return array<string, mixed>|null
     */
    public function first(): ?array
    {
        $result = (clone $this)->limit(1)->get();
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
     * Uses an internal clone to avoid mutating $limitValue on the current builder.
     */
    public function exists(): bool
    {
        return (clone $this)->limit(1)->get() !== [];
    }

    // ── Execution: Large-dataset traversal ─────────────────────

    /**
     * Process all rows in fixed-size batches, calling $callback per batch.
     * Memory-efficient for large tables.
     *
     * @param \Closure(list<array<string, mixed>>): bool|void $callback Return false to stop early.
     */
    public function chunk(int $size, \Closure $callback): void
    {
        $page = 1;

        do {
            $rows = (clone $this)->forPage($page, $size)->get();
            if ($rows === []) {
                break;
            }

            $result = $callback($rows);
            if ($result === false) {
                break;
            }

            $page++;
        } while (count($rows) === $size);
    }

    /**
     * Yield all rows one-by-one using a server-side cursor pattern.
     * Fetches rows in batches of $size internally to keep memory bounded.
     *
     * @param int $size Internal fetch batch size.
     *
     * @return \Generator<int, array<string, mixed>>
     */
    public function lazy(int $size = 1000): \Generator
    {
        $page = 1;

        do {
            $rows = (clone $this)->forPage($page, $size)->get();
            foreach ($rows as $row) {
                yield $row;
            }
            $page++;
        } while (count($rows) === $size);
    }

    /**
     * Process all rows in fixed-size batches ordered by a column ID, avoiding
     * the OFFSET cost of standard chunk(). Suitable for tables with a monotonically
     * increasing primary key.
     *
     * @param \Closure(list<array<string, mixed>>): bool|void $callback
     * @param string $column The cursor column (default: 'id').
     */
    public function chunkById(int $size, \Closure $callback, string $column = 'id'): void
    {
        $lastId = null;

        do {
            $qb = (clone $this)->orderBy($column)->limit($size);

            if ($lastId !== null) {
                $qb->where($column, '>', $lastId);
            }

            $rows = $qb->get();
            if ($rows === []) {
                break;
            }

            $result = $callback($rows);
            if ($result === false) {
                break;
            }

            $lastId = end($rows)[$column] ?? null;
        } while (count($rows) === $size && $lastId !== null);
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
     * When a RETURNING clause is set and the grammar supports it, returns the fetched rows.
     *
     * @param array<string, mixed> $values Column => value pairs for a single row.
     *
     * @return string|false|list<array<string, mixed>> Last insert ID, false, or returned rows.
     */
    public function insert(array $values): string|false|array
    {
        $columns = array_keys($values);
        $compiled = QueryCompiler::compileInsert(
            $this->fromTable,
            $columns,
            [array_values($values)],
            $this->grammar,
        );

        if ($this->returningColumns !== [] && $this->grammar->supportsReturning()) {
            $sql = $compiled['sql'] . ' ' . $this->grammar->compileReturning($this->returningColumns);
            return $this->executeReadWrite($sql, $compiled['bindings']);
        }

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

        if (!array_is_list($rows)) {
            throw new \InvalidArgumentException('insertMany() expects a list (sequential array) of rows.');
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
     * Insert a row only if it does not conflict with existing data.
     * Uses INSERT IGNORE (MySQL) or INSERT OR IGNORE (SQLite) or
     * INSERT ... ON CONFLICT DO NOTHING (PostgreSQL).
     *
     * @param array<string, mixed> $values
     */
    public function insertOrIgnore(array $values): int
    {
        $columns = array_keys($values);
        $compiled = QueryCompiler::compileInsertOrIgnore(
            $this->fromTable,
            $columns,
            [array_values($values)],
            $this->grammar,
        );

        return $this->executeWrite($compiled['sql'], $compiled['bindings']);
    }

    /**
     * Execute an UPDATE.
     * When a RETURNING clause is set and the grammar supports it, returns the fetched rows.
     *
     * @param array<string, mixed> $values Column => value pairs.
     *
     * @return int|list<array<string, mixed>> Affected rows, or returned rows.
     */
    public function update(array $values): int|array
    {
        $compiled = QueryCompiler::compileUpdate(
            $this->fromTable,
            $values,
            $this->wheres,
            $this->grammar,
        );

        if ($this->returningColumns !== [] && $this->grammar->supportsReturning()) {
            $sql = $compiled['sql'] . ' ' . $this->grammar->compileReturning($this->returningColumns);
            return $this->executeReadWrite($sql, $compiled['bindings']);
        }

        return $this->executeWrite($compiled['sql'], $compiled['bindings']);
    }

    /**
     * Execute a DELETE.
     * When a RETURNING clause is set and the grammar supports it, returns the fetched rows.
     *
     * @return int|list<array<string, mixed>> Affected rows, or returned rows.
     */
    public function delete(): int|array
    {
        $compiled = QueryCompiler::compileDelete(
            $this->fromTable,
            $this->wheres,
            $this->grammar,
        );

        if ($this->returningColumns !== [] && $this->grammar->supportsReturning()) {
            $sql = $compiled['sql'] . ' ' . $this->grammar->compileReturning($this->returningColumns);
            return $this->executeReadWrite($sql, $compiled['bindings']);
        }

        return $this->executeWrite($compiled['sql'], $compiled['bindings']);
    }

    /**
     * Truncate the table.
     */
    public function truncate(): void
    {
        $compiled = QueryCompiler::compileTruncate($this->fromTable, $this->grammar);
        $this->executeWrite($compiled['sql'], []);
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

    // ── Vector Search ───────────────────────────────────────────

    /**
     * Add an ORDER BY nearest-neighbors expression for vector similarity search.
     *
     * Requires pgvector on PostgreSQL or MySQL 9.x. Throws on SQLite.
     * The vector values are passed as bound parameters via OrderByClause bindings.
     *
     * @param string      $column  Vector column name.
     * @param list<float> $vector  Query vector.
     * @param int         $limit   Maximum number of neighbors to return.
     * @param string      $metric  Distance metric: 'l2', 'cosine', 'inner_product'.
     */
    public function nearestNeighbors(
        string $column,
        array $vector,
        int $limit = 10,
        string $metric = 'l2',
    ): self {
        $driver = $this->manager->connection($this->resolvedName)->getDriver();
        $expr   = VectorSearch::distance($column, $vector, $driver, $metric);

        $this->orders[] = new OrderByClause(
            column: $expr->toSql(),
            direction: SortDirection::Asc,
            bindings: $expr->getBindings(),
        );

        return $this->limit($limit);
    }

    // ── Transaction Support ─────────────────────────────────────

    /**
     * Execute a callback within a database transaction on the write connection.
     *
     * @template T
     *
     * @param \Closure(self): T $callback
     *
     * @return T
     */
    public function transaction(\Closure $callback): mixed
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
        $this->selectColumns    = ['*'];
        $this->distinct         = false;
        $this->joins            = [];
        $this->wheres           = [];
        $this->orders           = [];
        $this->groupBy          = null;
        $this->havings          = [];
        $this->limitValue       = null;
        $this->offsetValue      = null;
        $this->unions           = [];
        $this->returningColumns = [];
        $this->lockMode         = null;
        $this->lockNoWait       = false;
        $this->cteBuilder       = null;
        $this->resultCacheKey   = null;
        return $this;
    }

    // ── Static Cache Management ─────────────────────────────────

    /**
     * Clear the PDOStatement cache (all connections).
     */
    public static function clearStatementCache(): void
    {
        self::$stmtCache = null;
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
     * Execute a DML query on the write connection and fetch all returned rows
     * (used for RETURNING clauses).
     *
     * @param list<mixed> $bindings
     *
     * @return list<array<string, mixed>>
     */
    private function executeReadWrite(string $sql, array $bindings): array
    {
        $conn = $this->writeConnection();
        $stmt = $conn->pdo()->prepare($sql);
        $stmt->execute($bindings);
        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Prepare a statement with per-connection caching backed by WeakMap.
     *
     * WeakMap keys are the connection objects themselves, so entries are automatically
     * freed when a connection is destroyed — eliminating stale spl_object_id reuse (#3).
     * Per-connection cache is capped at STMT_CACHE_MAX entries via LRU eviction (#8).
     */
    private function prepareStatement(ConnectionInterface $conn, string $sql): PDOStatement
    {
        self::$stmtCache ??= new WeakMap();

        /** @var array<string, PDOStatement> $perConn */
        $perConn = self::$stmtCache[$conn] ?? [];

        if (isset($perConn[$sql])) {
            // Move to end (LRU: most recently used stays at back)
            $stmt = $perConn[$sql];
            unset($perConn[$sql]);
            $perConn[$sql] = $stmt;
            self::$stmtCache[$conn] = $perConn;
            return $stmt;
        }

        // Evict oldest entry when cap reached
        if (count($perConn) >= self::STMT_CACHE_MAX) {
            $oldest = array_key_first($perConn);
            unset($perConn[$oldest]);
        }

        $stmt = $conn->pdo()->prepare($sql);
        $perConn[$sql] = $stmt;
        self::$stmtCache[$conn] = $perConn;

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
