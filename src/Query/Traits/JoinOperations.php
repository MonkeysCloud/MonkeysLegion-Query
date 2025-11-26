<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

use MonkeysLegion\Query\JoinClauseBuilder;

/**
 * Provides JOIN clause operations for the query builder.
 * 
 * Implements methods for building SQL JOIN clauses with support for
 * inner, left, and right joins.
 * 
 * @property array $parts Query parts storage including 'joins' array
 */
trait JoinOperations
{
    public function join(
        string $table,
        string $alias,
        string $first,
        string $operator,
        string $second,
        string $type = 'INNER'
    ): static {
        $clause = strtoupper($type) . " JOIN $table AS $alias ON $first $operator $second";
        $this->parts['joins'][] = $clause;
        return $this;
    }

    /**
     * Adds an INNER JOIN clause.
     */
    public function innerJoin(string $table, string $alias, string $first, string $operator, string $second): static
    {
        return $this->join($table, $alias, $first, $operator, $second, 'INNER');
    }

    /**
     * Adds a LEFT JOIN clause.
     */
    public function leftJoin(string $table, string $alias, string $first, string $operator, string $second): static
    {
        return $this->join($table, $alias, $first, $operator, $second, 'LEFT');
    }

    /**
     * Adds a RIGHT JOIN clause.
     */
    public function rightJoin(string $table, string $alias, string $first, string $operator, string $second): static
    {
        return $this->join($table, $alias, $first, $operator, $second, 'RIGHT');
    }

    /**
     * Adds a CROSS JOIN clause.
     */
    public function crossJoin(string $table, ?string $alias = null): static
    {
        $tableExpr = $alias ? "$table AS $alias" : $table;
        $this->parts['joins'][] = "CROSS JOIN $tableExpr";
        return $this;
    }

    /**
     * Adds a FULL OUTER JOIN clause (PostgreSQL, some MySQL versions).
     */
    public function fullOuterJoin(string $table, string $alias, string $first, string $operator, string $second): static
    {
        return $this->join($table, $alias, $first, $operator, $second, 'FULL OUTER');
    }

    /**
     * Adds a LEFT OUTER JOIN clause (explicit OUTER keyword).
     */
    public function leftOuterJoin(string $table, string $alias, string $first, string $operator, string $second): static
    {
        return $this->join($table, $alias, $first, $operator, $second, 'LEFT OUTER');
    }

    /**
     * Adds a RIGHT OUTER JOIN clause (explicit OUTER keyword).
     */
    public function rightOuterJoin(string $table, string $alias, string $first, string $operator, string $second): static
    {
        return $this->join($table, $alias, $first, $operator, $second, 'RIGHT OUTER');
    }

    /**
     * Adds a JOIN with multiple conditions using a callback.
     *
     * Example:
     * ->joinOn('posts', 'p', function($join) {
     *     $join->on('users.id', '=', 'p.user_id')
     *          ->andOn('p.status', '=', 'published');
     * })
     */
    public function joinOn(string $table, string $alias, callable $callback, string $type = 'INNER'): static
    {
        $joinBuilder = new JoinClauseBuilder($table, $alias, $type);
        $callback($joinBuilder);

        $this->parts['joins'][] = $joinBuilder->toSql($this);
        return $this;
    }

    /**
     * Adds an INNER JOIN with multiple conditions.
     */
    public function innerJoinOn(string $table, string $alias, callable $callback): static
    {
        return $this->joinOn($table, $alias, $callback, 'INNER');
    }

    /**
     * Adds a LEFT JOIN with multiple conditions.
     */
    public function leftJoinOn(string $table, string $alias, callable $callback): static
    {
        return $this->joinOn($table, $alias, $callback, 'LEFT');
    }

    /**
     * Adds a RIGHT JOIN with multiple conditions.
     */
    public function rightJoinOn(string $table, string $alias, callable $callback): static
    {
        return $this->joinOn($table, $alias, $callback, 'RIGHT');
    }

    /**
     * Adds a raw JOIN clause.
     */
    public function joinRaw(string $sql, array $bindings = []): static
    {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $sql = preg_replace('/\?/', $placeholder, $sql, 1);
        }

        $this->parts['joins'][] = $sql;
        return $this;
    }

    /**
     * Joins to a subquery.
     *
     * @param string $subquery The subquery SQL
     * @param string $alias Alias for the subquery
     * @param string $first First column for join condition
     * @param string $operator Comparison operator
     * @param string $second Second column for join condition
     * @param string $type Join type (INNER, LEFT, RIGHT)
     * @param array $bindings Parameter bindings for subquery
     */
    public function joinSub(
        string $subquery,
        string $alias,
        string $first,
        string $operator,
        string $second,
        string $type = 'INNER',
        array $bindings = []
    ): static {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }

        $clause = strtoupper($type) . " JOIN ($subquery) AS $alias ON $first $operator $second";
        $this->parts['joins'][] = $clause;
        return $this;
    }

    /**
     * LEFT JOIN to a subquery.
     */
    public function leftJoinSub(
        string $subquery,
        string $alias,
        string $first,
        string $operator,
        string $second,
        array $bindings = []
    ): static {
        return $this->joinSub($subquery, $alias, $first, $operator, $second, 'LEFT', $bindings);
    }

    /**
     * RIGHT JOIN to a subquery.
     */
    public function rightJoinSub(
        string $subquery,
        string $alias,
        string $first,
        string $operator,
        string $second,
        array $bindings = []
    ): static {
        return $this->joinSub($subquery, $alias, $first, $operator, $second, 'RIGHT', $bindings);
    }

    /**
     * Joins to a subquery using QueryBuilder callback.
     */
    public function joinSubQuery(
        callable $callback,
        string $alias,
        string $first,
        string $operator,
        string $second,
        string $type = 'INNER'
    ): static {
        $subBuilder = new static($this->conn);
        $callback($subBuilder);

        $subquery = $subBuilder->toSql();

        // Merge parameters
        foreach ($subBuilder->params as $key => $value) {
            if (isset($this->params[$key])) {
                $newKey = $key . '_join' . $this->counter++;
                $subquery = str_replace($key, $newKey, $subquery);
                $this->params[$newKey] = $value;
            } else {
                $this->params[$key] = $value;
            }
        }

        $clause = strtoupper($type) . " JOIN ($subquery) AS $alias ON $first $operator $second";
        $this->parts['joins'][] = $clause;
        return $this;
    }

    /**
     * LEFT JOIN to a subquery using callback.
     */
    public function leftJoinSubQuery(
        callable $callback,
        string $alias,
        string $first,
        string $operator,
        string $second
    ): static {
        return $this->joinSubQuery($callback, $alias, $first, $operator, $second, 'LEFT');
    }

    /**
     * Adds a JOIN with USING clause (for columns with same name).
     *
     * Example: ->joinUsing('posts', 'p', 'user_id')
     * Results in: INNER JOIN posts AS p USING (user_id)
     */
    public function joinUsing(string $table, string $alias, string|array $columns, string $type = 'INNER'): static
    {
        $columnList = is_array($columns) ? implode(', ', $columns) : $columns;
        $clause = strtoupper($type) . " JOIN $table AS $alias USING ($columnList)";
        $this->parts['joins'][] = $clause;
        return $this;
    }

    /**
     * INNER JOIN with USING clause.
     */
    public function innerJoinUsing(string $table, string $alias, string|array $columns): static
    {
        return $this->joinUsing($table, $alias, $columns, 'INNER');
    }

    /**
     * LEFT JOIN with USING clause.
     */
    public function leftJoinUsing(string $table, string $alias, string|array $columns): static
    {
        return $this->joinUsing($table, $alias, $columns, 'LEFT');
    }

    /**
     * RIGHT JOIN with USING clause.
     */
    public function rightJoinUsing(string $table, string $alias, string|array $columns): static
    {
        return $this->joinUsing($table, $alias, $columns, 'RIGHT');
    }

    /**
     * Adds a NATURAL JOIN (automatically joins on columns with same name).
     * Use with caution - explicit joins are generally preferred.
     */
    public function naturalJoin(string $table, ?string $alias = null, string $type = 'INNER'): static
    {
        $tableExpr = $alias ? "$table AS $alias" : $table;
        $clause = "NATURAL " . strtoupper($type) . " JOIN $tableExpr";
        $this->parts['joins'][] = $clause;
        return $this;
    }

    /**
     * Adds a NATURAL LEFT JOIN.
     */
    public function naturalLeftJoin(string $table, ?string $alias = null): static
    {
        return $this->naturalJoin($table, $alias, 'LEFT');
    }

    /**
     * Adds a NATURAL RIGHT JOIN.
     */
    public function naturalRightJoin(string $table, ?string $alias = null): static
    {
        return $this->naturalJoin($table, $alias, 'RIGHT');
    }

    /**
     * Adds a LATERAL JOIN (PostgreSQL).
     * Allows subquery to reference columns from preceding FROM items.
     */
    public function joinLateral(
        string $subquery,
        string $alias,
        array $bindings = [],
        string $type = 'LEFT'
    ): static {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }

        $clause = strtoupper($type) . " JOIN LATERAL ($subquery) AS $alias ON true";
        $this->parts['joins'][] = $clause;
        return $this;
    }

    /**
     * LEFT JOIN LATERAL.
     */
    public function leftJoinLateral(string $subquery, string $alias, array $bindings = []): static
    {
        return $this->joinLateral($subquery, $alias, $bindings, 'LEFT');
    }

    /**
     * INNER JOIN LATERAL.
     */
    public function innerJoinLateral(string $subquery, string $alias, array $bindings = []): static
    {
        return $this->joinLateral($subquery, $alias, $bindings, 'INNER');
    }

    /**
     * Conditionally adds a join.
     */
    public function joinWhen(
        bool $condition,
        string $table,
        string $alias,
        string $first,
        string $operator,
        string $second,
        string $type = 'INNER'
    ): static {
        if ($condition) {
            return $this->join($table, $alias, $first, $operator, $second, $type);
        }
        return $this;
    }

    /**
     * Conditionally adds a LEFT JOIN.
     */
    public function leftJoinWhen(
        bool $condition,
        string $table,
        string $alias,
        string $first,
        string $operator,
        string $second
    ): static {
        return $this->joinWhen($condition, $table, $alias, $first, $operator, $second, 'LEFT');
    }

    /**
     * Gets all joins as an array.
     */
    public function getJoins(): array
    {
        return $this->parts['joins'] ?? [];
    }

    /**
     * Checks if any joins exist.
     */
    public function hasJoins(): bool
    {
        return !empty($this->parts['joins']);
    }

    /**
     * Removes all joins.
     */
    public function clearJoins(): static
    {
        $this->parts['joins'] = [];
        return $this;
    }

    /**
     * Counts the number of joins.
     */
    public function countJoins(): int
    {
        return count($this->parts['joins'] ?? []);
    }

    /**
     * Performs a self-join on the current table.
     *
     * @param string $alias Alias for the joined instance of the same table
     * @param string $first First column (from main table)
     * @param string $operator Comparison operator
     * @param string $second Second column (from aliased table)
     * @param string $type Join type
     */
    public function selfJoin(
        string $alias,
        string $first,
        string $operator,
        string $second,
        string $type = 'INNER'
    ): static {
        $table = $this->getTableName();

        if (!$table) {
            throw new \RuntimeException("Cannot perform self-join: no FROM table set");
        }

        return $this->join($table, $alias, $first, $operator, $second, $type);
    }

    /**
     * Performs a LEFT self-join.
     */
    public function leftSelfJoin(string $alias, string $first, string $operator, string $second): static
    {
        return $this->selfJoin($alias, $first, $operator, $second, 'LEFT');
    }
}
