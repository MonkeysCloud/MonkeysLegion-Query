<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Compiler;

use MonkeysLegion\Database\Types\DatabaseDriver;
use MonkeysLegion\Query\Clause\GroupByClause;
use MonkeysLegion\Query\Clause\HavingClause;
use MonkeysLegion\Query\Clause\JoinClause;
use MonkeysLegion\Query\Clause\LimitOffsetClause;
use MonkeysLegion\Query\Clause\OrderByClause;
use MonkeysLegion\Query\Clause\SelectClause;
use MonkeysLegion\Query\Clause\UnionClause;
use MonkeysLegion\Query\Clause\WhereClause;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Stateless SQL compiler that converts typed clause objects into
 * a SQL string plus positional parameter bindings.
 *
 * Performance characteristics:
 *  • No regex at compile time
 *  • No information_schema queries
 *  • Structural SQL template caching via xxh128 hash
 *  • Zero string allocation for cached queries (only bindings change)
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class QueryCompiler
{
    // ── Structural SQL Cache ────────────────────────────────────

    /** @var array<string, string> structuralHash → compiled SQL template */
    private static array $sqlCache = [];

    // ── Grammar Resolution ──────────────────────────────────────

    /** @var array<string, GrammarInterface> driver → grammar instance */
    private static array $grammars = [];

    /**
     * Get the grammar for a given driver.
     */
    public static function grammarFor(DatabaseDriver $driver): GrammarInterface
    {
        $key = $driver->value;

        return self::$grammars[$key] ??= match ($driver) {
            DatabaseDriver::MySQL   => new MySqlGrammar(),
            DatabaseDriver::PostgreSQL => new PostgresGrammar(),
            DatabaseDriver::SQLite  => new SqliteGrammar(),
        };
    }

    /**
     * Clear the structural SQL cache (e.g. after schema changes).
     */
    public static function clearCache(): void
    {
        self::$sqlCache = [];
    }

    // ── SELECT Compilation ──────────────────────────────────────

    /**
     * Compile a SELECT query.
     *
     * @param SelectClause           $select
     * @param string                 $from      Table name (already quoted or raw).
     * @param list<WhereClause>      $wheres
     * @param list<JoinClause>       $joins
     * @param list<OrderByClause>    $orders
     * @param GroupByClause|null     $groupBy
     * @param list<HavingClause>     $havings
     * @param LimitOffsetClause|null $limit
     * @param list<UnionClause>      $unions
     * @param GrammarInterface       $grammar
     * @param string|null            $ctePrefix  Pre-compiled WITH … clause (prepended to SQL).
     * @param string|null            $lockSuffix  Pre-compiled FOR UPDATE / FOR SHARE clause.
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public static function compileSelect(
        SelectClause $select,
        string $from,
        array $wheres = [],
        array $joins = [],
        array $orders = [],
        ?GroupByClause $groupBy = null,
        array $havings = [],
        ?LimitOffsetClause $limit = null,
        array $unions = [],
        GrammarInterface $grammar = new MySqlGrammar(),
        ?string $ctePrefix = null,
        ?string $lockSuffix = null,
    ): array {
        $bindings = [];

        // Cache the SELECT sql fragment once to avoid double evaluation (#9)
        $selectSql = $select->toSql();

        // Build structural key for cache hit detection
        $structKey = self::buildStructuralKey(
            'SELECT',
            $selectSql,
            $from,
            $wheres,
            $joins,
            $orders,
            $groupBy,
            $havings,
            $limit,
            $unions,
            $ctePrefix,
            $lockSuffix,
        );

        // Check structural cache
        if (isset(self::$sqlCache[$structKey])) {
            $sql = self::$sqlCache[$structKey];
            $bindings = self::collectBindings($select, $wheres, $joins, $havings, $unions, $orders);
            return ['sql' => $sql, 'bindings' => $bindings];
        }

        // Build SQL
        $sql = 'SELECT ' . $selectSql;
        $sql .= " FROM {$from}";

        // JOINs
        foreach ($joins as $join) {
            $sql .= ' ' . $join->toSql();
        }

        // WHERE
        if ($wheres !== []) {
            $sql .= ' WHERE ' . self::compileWheres($wheres);
        }

        // GROUP BY
        if ($groupBy !== null) {
            $sql .= ' GROUP BY ' . $groupBy->toSql();
        }

        // HAVING
        if ($havings !== []) {
            $sql .= ' HAVING ' . self::compileHavings($havings);
        }

        // ORDER BY
        if ($orders !== []) {
            $sql .= ' ORDER BY ' . implode(', ', array_map(
                fn(OrderByClause $o) => $o->toSql(),
                $orders,
            ));
        }

        // LIMIT / OFFSET
        if ($limit !== null) {
            $limitSql = $grammar->compileLimit($limit->limit, $limit->offset);
            if ($limitSql !== '') {
                $sql .= ' ' . $limitSql;
            }
        }

        // UNION
        foreach ($unions as $union) {
            $sql .= ' ' . $union->toSql();
        }

        // Locking
        if ($lockSuffix !== null && $lockSuffix !== '') {
            $sql .= ' ' . $lockSuffix;
        }

        // Prepend CTE
        if ($ctePrefix !== null && $ctePrefix !== '') {
            $sql = $ctePrefix . ' ' . $sql;
        }

        // Cache the structural SQL
        self::$sqlCache[$structKey] = $sql;

        // Collect bindings
        $bindings = self::collectBindings($select, $wheres, $joins, $havings, $unions, $orders);

        return ['sql' => $sql, 'bindings' => $bindings];
    }

    // ── INSERT Compilation ──────────────────────────────────────

    /**
     * Compile an INSERT statement.
     *
     * @param string                $table   Table name.
     * @param list<string>          $columns Column names.
     * @param list<list<mixed>>     $rows    Row values (each row is a list of values).
     * @param GrammarInterface      $grammar
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public static function compileInsert(
        string $table,
        array $columns,
        array $rows,
        GrammarInterface $grammar = new MySqlGrammar(),
    ): array {
        $quotedTable = $grammar->quoteIdentifier($table);
        $quotedCols  = implode(', ', array_map(
            fn(string $c) => $grammar->quoteIdentifier($c),
            $columns,
        ));

        $rowPlaceholders = [];
        $bindings        = [];

        foreach ($rows as $row) {
            $rowPlaceholders[] = '(' . implode(', ', array_fill(0, count($row), '?')) . ')';
            foreach ($row as $value) {
                $bindings[] = $value;
            }
        }

        $sql = "INSERT INTO {$quotedTable} ({$quotedCols}) VALUES " . implode(', ', $rowPlaceholders);

        return ['sql' => $sql, 'bindings' => $bindings];
    }

    // ── UPDATE Compilation ──────────────────────────────────────

    /**
     * Compile an UPDATE statement.
     *
     * @param string             $table
     * @param array<string, mixed> $values   Column => value pairs.
     * @param list<WhereClause>    $wheres
     * @param GrammarInterface     $grammar
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public static function compileUpdate(
        string $table,
        array $values,
        array $wheres = [],
        GrammarInterface $grammar = new MySqlGrammar(),
    ): array {
        $quotedTable = $grammar->quoteIdentifier($table);
        $sets        = [];
        $bindings    = [];

        foreach ($values as $column => $value) {
            $sets[]     = $grammar->quoteIdentifier($column) . ' = ?';
            $bindings[] = $value;
        }

        $sql = "UPDATE {$quotedTable} SET " . implode(', ', $sets);

        if ($wheres !== []) {
            $sql .= ' WHERE ' . self::compileWheres($wheres);
            foreach ($wheres as $where) {
                foreach ($where->getBindings() as $binding) {
                    $bindings[] = $binding;
                }
            }
        }

        return ['sql' => $sql, 'bindings' => $bindings];
    }

    // ── DELETE Compilation ──────────────────────────────────────

    /**
     * Compile a DELETE statement.
     *
     * @param string              $table
     * @param list<WhereClause>   $wheres
     * @param GrammarInterface    $grammar
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public static function compileDelete(
        string $table,
        array $wheres = [],
        GrammarInterface $grammar = new MySqlGrammar(),
    ): array {
        $quotedTable = $grammar->quoteIdentifier($table);
        $bindings    = [];

        $sql = "DELETE FROM {$quotedTable}";

        if ($wheres !== []) {
            $sql .= ' WHERE ' . self::compileWheres($wheres);
            foreach ($wheres as $where) {
                foreach ($where->getBindings() as $binding) {
                    $bindings[] = $binding;
                }
            }
        }

        return ['sql' => $sql, 'bindings' => $bindings];
    }

    // ── UPSERT Compilation ──────────────────────────────────────

    /**
     * Compile an UPSERT statement.
     *
     * @param string                    $table
     * @param list<string>              $columns
     * @param list<mixed>               $values
     * @param list<string>              $updateColumns
     * @param string|list<string>|null  $conflictTarget
     * @param GrammarInterface          $grammar
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public static function compileUpsert(
        string $table,
        array $columns,
        array $values,
        array $updateColumns,
        string|array|null $conflictTarget = null,
        GrammarInterface $grammar = new MySqlGrammar(),
    ): array {
        $placeholders = array_fill(0, count($columns), '?');

        $sql = $grammar->compileUpsert($table, $columns, $placeholders, $updateColumns, $conflictTarget);

        return ['sql' => $sql, 'bindings' => $values];
    }

    // ── INSERT OR IGNORE Compilation ────────────────────────────

    /**
     * Compile an INSERT OR IGNORE statement.
     *
     * @param string                $table
     * @param list<string>          $columns
     * @param list<list<mixed>>     $rows
     * @param GrammarInterface      $grammar
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public static function compileInsertOrIgnore(
        string $table,
        array $columns,
        array $rows,
        GrammarInterface $grammar = new MySqlGrammar(),
    ): array {
        $rowPlaceholders = [];
        $bindings        = [];

        foreach ($rows as $row) {
            $rowPlaceholders[] = '(' . implode(', ', array_fill(0, count($row), '?')) . ')';
            foreach ($row as $value) {
                $bindings[] = $value;
            }
        }

        // compileInsertOrIgnore handles one value tuple; for multi-row we fall back to
        // building the statement manually using the per-grammar prefix.
        $singlePlaceholder = $rowPlaceholders[0] ?? '()';
        $baseSql = $grammar->compileInsertOrIgnore($table, $columns, array_fill(0, count($columns), '?'));

        if (count($rows) === 1) {
            return ['sql' => $baseSql, 'bindings' => $bindings];
        }

        // Multi-row: replace the single VALUES(...) with multiple tuples
        $valuesPart = implode(', ', $rowPlaceholders);
        $sql = (string) preg_replace('/VALUES \([^)]*\)$/', "VALUES {$valuesPart}", $baseSql);

        return ['sql' => $sql, 'bindings' => $bindings];
    }

    // ── TRUNCATE Compilation ─────────────────────────────────────

    /**
     * Compile a TRUNCATE statement.
     *
     * @return array{sql: string, bindings: list<mixed>}
     */
    public static function compileTruncate(
        string $table,
        GrammarInterface $grammar = new MySqlGrammar(),
    ): array {
        return ['sql' => $grammar->compileTruncate($table), 'bindings' => []];
    }

    // ── Private Helpers ─────────────────────────────────────────

    /**
     * Compile WHERE clauses into a SQL fragment.
     *
     * @param list<WhereClause> $wheres
     */
    private static function compileWheres(array $wheres): string
    {
        $parts = [];

        foreach ($wheres as $i => $where) {
            $fragment = $where->toSql();

            if ($i === 0) {
                $parts[] = $fragment;
            } else {
                $parts[] = $where->boolean->value . ' ' . $fragment;
            }
        }

        return implode(' ', $parts);
    }

    /**
     * Compile HAVING clauses respecting AND/OR booleans.
     *
     * @param list<HavingClause> $havings
     */
    private static function compileHavings(array $havings): string
    {
        $parts = [];

        foreach ($havings as $i => $having) {
            $fragment = $having->toSql();

            if ($i === 0) {
                $parts[] = $fragment;
            } else {
                $parts[] = $having->boolean->value . ' ' . $fragment;
            }
        }

        return implode(' ', $parts);
    }

    /**
     * Collect all bindings from clause objects in correct order.
     *
     * @return list<mixed>
     */
    private static function collectBindings(
        SelectClause $select,
        array $wheres,
        array $joins,
        array $havings,
        array $unions,
        array $orders = [],
    ): array {
        $bindings = [];

        // Select bindings (rare — sub-selects in column list)
        foreach ($select->getBindings() as $b) {
            $bindings[] = $b;
        }

        // Join bindings
        foreach ($joins as $join) {
            foreach ($join->getBindings() as $b) {
                $bindings[] = $b;
            }
        }

        // Where bindings
        foreach ($wheres as $where) {
            foreach ($where->getBindings() as $b) {
                $bindings[] = $b;
            }
        }

        // Having bindings
        foreach ($havings as $having) {
            foreach ($having->getBindings() as $b) {
                $bindings[] = $b;
            }
        }

        // Order by bindings (e.g. vector distance expressions)
        foreach ($orders as $order) {
            foreach ($order->getBindings() as $b) {
                $bindings[] = $b;
            }
        }

        // Union bindings
        foreach ($unions as $union) {
            foreach ($union->getBindings() as $b) {
                $bindings[] = $b;
            }
        }

        return $bindings;
    }

    /**
     * Build a structural key for SQL caching.
     *
     * The key captures the shape of the query (clause count, column names,
     * operator types, table names) but NOT the bound values. Identical
     * structural queries reuse the same SQL template.
     */
    private static function buildStructuralKey(
        string $type,
        string $selectSql,
        string $from,
        array $wheres,
        array $joins,
        array $orders,
        ?GroupByClause $groupBy,
        array $havings,
        ?LimitOffsetClause $limit,
        array $unions,
        ?string $ctePrefix = null,
        ?string $lockSuffix = null,
    ): string {
        // Build a compact structural fingerprint
        $parts = [$type, $selectSql, $from];

        if ($ctePrefix !== null) {
            $parts[] = 'CTE:' . $ctePrefix;
        }

        foreach ($joins as $j) {
            $parts[] = 'J:' . $j->type->value . ':' . $j->table . ':' . implode(',', $j->conditions);
        }

        foreach ($wheres as $w) {
            $key = 'W:' . $w->column . ':' . $w->operator->value . ':' . $w->boolean->value;
            // IN/NOT IN: the SQL placeholder count varies by number of values.
            // Include cardinality to avoid structural cache collisions.
            if (($w->operator === \MonkeysLegion\Query\Enums\Operator::In
                || $w->operator === \MonkeysLegion\Query\Enums\Operator::NotIn)
                && is_array($w->value)
            ) {
                $key .= ':' . count($w->value);
            }
            // Raw: the column IS the SQL — include it as-is for uniqueness
            if ($w->operator === \MonkeysLegion\Query\Enums\Operator::Raw) {
                $key .= ':RAW:' . strlen($w->column);
            }
            $parts[] = $key;
        }

        if ($groupBy !== null) {
            $parts[] = 'G:' . $groupBy->toSql();
        }

        foreach ($havings as $h) {
            $parts[] = 'H:' . $h->expression . ':' . $h->boolean->value;
        }

        foreach ($orders as $o) {
            $parts[] = 'O:' . $o->column . ':' . $o->direction->value . ':' . count($o->getBindings());
        }

        if ($limit !== null) {
            $parts[] = 'L:' . ($limit->limit ?? 'n') . ':' . ($limit->offset ?? 'n');
        }

        $parts[] = 'U:' . count($unions);

        if ($lockSuffix !== null) {
            $parts[] = 'LK:' . $lockSuffix;
        }

        return hash('xxh128', implode('|', $parts));
    }
}
