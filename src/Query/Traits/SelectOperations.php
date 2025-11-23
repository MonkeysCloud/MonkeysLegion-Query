<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

/**
 * Provides SELECT statement operations for the query builder.
 * 
 * Implements methods for building SELECT queries, specifying columns,
 * and defining table sources.
 * 
 * @property array $parts Query parts storage including 'select', 'distinct', and 'from'
 */
trait SelectOperations
{
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

    /**
     * Adds a raw SELECT expression.
     *
     * @param string $expression Raw SQL expression
     * @param array $bindings Parameter bindings for the expression
     */
    public function selectRaw(string $expression, array $bindings = []): self
    {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $expression = preg_replace('/\?/', $placeholder, $expression, 1);
        }

        $this->parts['select'] = $expression;
        return $this;
    }

    /**
     * Adds a raw expression to existing SELECT.
     */
    public function addSelectRaw(string $expression, array $bindings = []): self
    {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $expression = preg_replace('/\?/', $placeholder, $expression, 1);
        }

        $existing = $this->parts['select'] === '*' ? [] : explode(', ', $this->parts['select']);
        $existing[] = $expression;
        $this->parts['select'] = implode(', ', $existing);
        return $this;
    }

    /**
     * Adds a subquery to the SELECT clause.
     *
     * @param string $subquery The subquery SQL
     * @param string $alias Alias for the subquery result
     * @param array $bindings Parameter bindings
     */
    public function selectSub(string $subquery, string $alias, array $bindings = []): self
    {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }

        $expression = "($subquery) AS $alias";

        if ($this->parts['select'] === '*') {
            $this->parts['select'] = $expression;
        } else {
            $this->parts['select'] .= ', ' . $expression;
        }

        return $this;
    }

    /**
     * Adds a subquery using a callback QueryBuilder.
     *
     * @param callable $callback Callback that receives a QueryBuilder instance
     * @param string $alias Alias for the subquery result
     */
    public function selectSubQuery(callable $callback, string $alias): self
    {
        $subBuilder = new self($this->conn);
        $callback($subBuilder);

        $subquery = $subBuilder->toSql();

        // Merge parameters from subquery
        foreach ($subBuilder->params as $key => $value) {
            if (isset($this->params[$key])) {
                // Avoid collision
                $newKey = $key . '_sub' . $this->counter++;
                $subquery = str_replace($key, $newKey, $subquery);
                $this->params[$newKey] = $value;
            } else {
                $this->params[$key] = $value;
            }
        }

        $expression = "($subquery) AS $alias";

        if ($this->parts['select'] === '*') {
            $this->parts['select'] = $expression;
        } else {
            $this->parts['select'] .= ', ' . $expression;
        }

        return $this;
    }

    /**
     * Selects a column with an explicit alias.
     *
     * @param string $column Column name or expression
     * @param string $alias Alias for the column
     */
    public function selectAs(string $column, string $alias): self
    {
        $expression = "$column AS $alias";

        if ($this->parts['select'] === '*') {
            $this->parts['select'] = $expression;
        } else {
            $this->parts['select'] .= ', ' . $expression;
        }

        return $this;
    }

    /**
     * Adds multiple columns with aliases.
     *
     * @param array $columns Associative array where key is column and value is alias
     * Example: ['user_id' => 'id', 'user_name' => 'name']
     */
    public function selectAliases(array $columns): self
    {
        $expressions = [];
        foreach ($columns as $column => $alias) {
            $expressions[] = "$column AS $alias";
        }

        if ($this->parts['select'] === '*') {
            $this->parts['select'] = implode(', ', $expressions);
        } else {
            $existing = explode(', ', $this->parts['select']);
            $this->parts['select'] = implode(', ', array_merge($existing, $expressions));
        }

        return $this;
    }

    /**
     * Adds a COUNT expression to SELECT.
     */
    public function selectCount(string $column = '*', string $alias = 'count'): self
    {
        return $this->selectAs("COUNT($column)", $alias);
    }

    /**
     * Adds a SUM expression to SELECT.
     */
    public function selectSum(string $column, string $alias = 'total'): self
    {
        return $this->selectAs("SUM($column)", $alias);
    }

    /**
     * Adds an AVG expression to SELECT.
     */
    public function selectAvg(string $column, string $alias = 'average'): self
    {
        return $this->selectAs("AVG($column)", $alias);
    }

    /**
     * Adds a MIN expression to SELECT.
     */
    public function selectMin(string $column, string $alias = 'minimum'): self
    {
        return $this->selectAs("MIN($column)", $alias);
    }

    /**
     * Adds a MAX expression to SELECT.
     */
    public function selectMax(string $column, string $alias = 'maximum'): self
    {
        return $this->selectAs("MAX($column)", $alias);
    }

    /**
     * Adds a CONCAT expression to SELECT.
     *
     * @param array $columns Columns to concatenate
     * @param string $alias Alias for the result
     * @param string|null $separator Optional separator
     */
    public function selectConcat(array $columns, string $alias, ?string $separator = null): self
    {
        if ($separator !== null) {
            $placeholder = $this->addParam($separator);
            $expression = 'CONCAT_WS(' . $placeholder . ', ' . implode(', ', $columns) . ')';
        } else {
            $expression = 'CONCAT(' . implode(', ', $columns) . ')';
        }

        return $this->selectAs($expression, $alias);
    }

    /**
     * Adds a COALESCE expression to SELECT.
     */
    public function selectCoalesce(array $columns, string $alias): self
    {
        $expression = 'COALESCE(' . implode(', ', $columns) . ')';
        return $this->selectAs($expression, $alias);
    }

    /**
     * Adds a CASE statement to SELECT.
     *
     * @param array $conditions Array of ['condition' => 'result'] pairs
     * @param mixed $else ELSE value (optional)
     * @param string $alias Alias for the CASE result
     *
     * Example:
     * ->selectCase([
     *     'status = "active"' => '"Active User"',
     *     'status = "pending"' => '"Pending User"'
     * ], '"Unknown"', 'user_status')
     */
    public function selectCase(array $conditions, mixed $else = null, string $alias = 'case_result'): self
    {
        $caseExpr = 'CASE';

        foreach ($conditions as $condition => $result) {
            $caseExpr .= " WHEN $condition THEN $result";
        }

        if ($else !== null) {
            $caseExpr .= " ELSE $else";
        }

        $caseExpr .= ' END';

        return $this->selectAs($caseExpr, $alias);
    }

    /**
     * Adds a simple CASE statement with bindings.
     *
     * @param string $column Column to evaluate
     * @param array $whenThen Array of [whenValue => thenValue]
     * @param mixed $else Default value
     * @param string $alias Result alias
     */
    public function selectCaseWhen(string $column, array $whenThen, mixed $else = null, string $alias = 'case_result'): self
    {
        $caseExpr = "CASE $column";

        foreach ($whenThen as $when => $then) {
            $whenPlaceholder = $this->addParam($when);
            $thenPlaceholder = $this->addParam($then);
            $caseExpr .= " WHEN $whenPlaceholder THEN $thenPlaceholder";
        }

        if ($else !== null) {
            $elsePlaceholder = $this->addParam($else);
            $caseExpr .= " ELSE $elsePlaceholder";
        }

        $caseExpr .= ' END';

        return $this->selectAs($caseExpr, $alias);
    }

    /**
     * Adds a raw FROM clause.
     */
    public function fromRaw(string $expression, array $bindings = []): self
    {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $expression = preg_replace('/\?/', $placeholder, $expression, 1);
        }

        $this->parts['from'] = $expression;
        return $this;
    }

    /**
     * Sets FROM to a subquery.
     *
     * @param string $subquery Subquery SQL
     * @param string $alias Alias for the derived table
     * @param array $bindings Parameter bindings
     */
    public function fromSub(string $subquery, string $alias, array $bindings = []): self
    {
        foreach ($bindings as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }

        $this->parts['from'] = "($subquery) AS $alias";
        return $this;
    }

    /**
     * Sets FROM to a subquery using QueryBuilder callback.
     */
    public function fromSubQuery(callable $callback, string $alias): self
    {
        $subBuilder = new self($this->conn);
        $callback($subBuilder);

        $subquery = $subBuilder->toSql();

        // Merge parameters
        foreach ($subBuilder->params as $key => $value) {
            if (isset($this->params[$key])) {
                $newKey = $key . '_from' . $this->counter++;
                $subquery = str_replace($key, $newKey, $subquery);
                $this->params[$newKey] = $value;
            } else {
                $this->params[$key] = $value;
            }
        }

        $this->parts['from'] = "($subquery) AS $alias";
        return $this;
    }

    /**
     * Adds additional tables to FROM (comma-separated).
     * For Cartesian products or when you need multiple tables.
     */
    public function addFrom(string $table, ?string $alias = null): self
    {
        $tableExpr = $alias ? "$table AS $alias" : $table;

        if (empty($this->parts['from'])) {
            $this->parts['from'] = $tableExpr;
        } else {
            $this->parts['from'] .= ', ' . $tableExpr;
        }

        return $this;
    }

    /**
     * Adds DISTINCT ON for specific columns (PostgreSQL).
     * For MySQL, this falls back to regular DISTINCT.
     *
     * @param string|array $columns Columns for DISTINCT ON
     */
    public function distinctOn(string|array $columns): self
    {
        $columnList = is_array($columns) ? implode(', ', $columns) : $columns;

        // Check if we're using PostgreSQL
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        if ($driver === 'pgsql') {
            $this->parts['distinct'] = "ON ($columnList)";
        } else {
            // Fallback to regular DISTINCT for other databases
            $this->parts['distinct'] = true;
        }

        return $this;
    }

    /**
     * Removes DISTINCT from the query.
     */
    public function removeDistinct(): self
    {
        $this->parts['distinct'] = false;
        return $this;
    }

    /**
     * Adds SQL_CALC_FOUND_ROWS modifier (MySQL).
     * Useful for pagination with total count.
     */
    public function calcFoundRows(): self
    {
        $this->parts['modifiers'] = ($this->parts['modifiers'] ?? '') . ' SQL_CALC_FOUND_ROWS';
        return $this;
    }

    /**
     * Adds HIGH_PRIORITY modifier (MySQL).
     */
    public function highPriority(): self
    {
        $this->parts['modifiers'] = ($this->parts['modifiers'] ?? '') . ' HIGH_PRIORITY';
        return $this;
    }

    /**
     * Adds SQL_SMALL_RESULT modifier (MySQL).
     */
    public function smallResult(): self
    {
        $this->parts['modifiers'] = ($this->parts['modifiers'] ?? '') . ' SQL_SMALL_RESULT';
        return $this;
    }

    /**
     * Adds SQL_BIG_RESULT modifier (MySQL).
     */
    public function bigResult(): self
    {
        $this->parts['modifiers'] = ($this->parts['modifiers'] ?? '') . ' SQL_BIG_RESULT';
        return $this;
    }

    /**
     * Adds SQL_BUFFER_RESULT modifier (MySQL).
     */
    public function bufferResult(): self
    {
        $this->parts['modifiers'] = ($this->parts['modifiers'] ?? '') . ' SQL_BUFFER_RESULT';
        return $this;
    }

    /**
     * Clears the SELECT clause (resets to default).
     */
    public function clearSelect(): self
    {
        $this->parts['select'] = '*';
        return $this;
    }

    /**
     * Checks if a specific column is in the SELECT.
     */
    public function hasColumn(string $column): bool
    {
        $select = $this->parts['select'];
        return str_contains($select, $column);
    }

    /**
     * Gets the current SELECT clause.
     */
    public function getSelect(): string
    {
        return $this->parts['select'];
    }

    /**
     * Sets SELECT to only return specific columns from a table.
     * Useful when you want table1.*, table2.id format.
     */
    public function selectTable(string $table, array $columns = ['*']): self
    {
        $prefixedColumns = array_map(
            fn($col) => $col === '*' ? "$table.*" : "$table.$col",
            $columns
        );

        return $this->addSelect($prefixedColumns);
    }

    /**
     * Selects a JSON field path.
     *
     * @param string $column JSON column name
     * @param string $path JSON path (e.g., '$.user.name')
     * @param string $alias Result alias
     */
    public function selectJson(string $column, string $path, string $alias): self
    {
        $pathPlaceholder = $this->addParam($path);
        $expression = "JSON_EXTRACT($column, $pathPlaceholder)";
        return $this->selectAs($expression, $alias);
    }

    /**
     * Selects and unquotes a JSON field path.
     */
    public function selectJsonUnquote(string $column, string $path, string $alias): self
    {
        $pathPlaceholder = $this->addParam($path);
        $expression = "JSON_UNQUOTE(JSON_EXTRACT($column, $pathPlaceholder))";
        return $this->selectAs($expression, $alias);
    }
}
