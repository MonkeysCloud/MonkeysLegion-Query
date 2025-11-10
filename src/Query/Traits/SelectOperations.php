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
}
