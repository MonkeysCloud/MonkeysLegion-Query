<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

/**
 * Provides WHERE clause operations for the query builder.
 * 
 * Implements methods for building SQL WHERE clauses with support for
 * conditions, grouping, and complex expressions.
 * 
 * @property array $parts Query parts storage including 'where' array
 * @property array $params Query parameters
 * @property int $counter Parameter counter for generating unique placeholders
 * @property \MonkeysLegion\Database\Contracts\ConnectionInterface $conn Database connection
 */
trait WhereOperations
{
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
     * Groups WHERE conditions with AND (explicit AND group).
     */
    public function andWhereGroup(callable $callback): self
    {
        $subBuilder = new self($this->conn);
        $callback($subBuilder);

        if (!empty($subBuilder->parts['where'])) {
            $clauses = [];
            foreach ($subBuilder->parts['where'] as $i => $w) {
                $prefix = $i && $w['type'] ? ' ' . $w['type'] . ' ' : '';
                $clauses[] = $prefix . $w['expr'];
            }

            // Always AND the group, even if it's the first condition — matches method name semantics
            $this->parts['where'][] = [
                'type' => 'AND',
                'expr' => '(' . implode('', $clauses) . ')',
            ];

            // Merge params from subBuilder with collision-safe renaming
            foreach ($subBuilder->params as $key => $value) {
                if (isset($this->params[$key])) {
                    $newKey = $key . '_g' . $this->counter++;
                    // Update the just-added expr to use the new param key
                    $lastIdx = count($this->parts['where']) - 1;
                    $this->parts['where'][$lastIdx]['expr'] =
                        str_replace($key, $newKey, $this->parts['where'][$lastIdx]['expr']);
                    $this->params[$newKey] = $value;
                } else {
                    $this->params[$key] = $value;
                }
            }

            // Keep counter monotonic
            if ($subBuilder->counter > $this->counter) {
                $this->counter = $subBuilder->counter;
            }
        }

        return $this;
    }

    /**
     * Adds a raw OR condition to the WHERE clause.
     */
    public function orWhereRaw(string $sql, array $params = []): self
    {
        foreach ($params as $value) {
            $placeholder = $this->addParam($value);
            $sql = preg_replace('/\?/', $placeholder, $sql, 1);
        }
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => $sql,
        ];
        return $this;
    }
}
