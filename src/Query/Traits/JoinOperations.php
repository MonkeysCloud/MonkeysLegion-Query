<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

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
}
