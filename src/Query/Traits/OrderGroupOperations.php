<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

/**
 * Provides ordering, grouping, and pagination operations for the query builder.
 * 
 * Implements methods for GROUP BY, HAVING, ORDER BY, LIMIT, OFFSET,
 * pagination, and UNION operations.
 * 
 * @property array $parts Query parts storage including 'groupBy', 'having', 'orderBy', 'limit', 'offset', 'unions'
 * @property array $params Query parameters
 * @property int $counter Parameter counter for generating unique placeholders
 */
trait OrderGroupOperations
{
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
}
