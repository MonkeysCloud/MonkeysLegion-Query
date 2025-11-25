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
    public function where(string $column, string $operator, mixed $value): static
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
    public function andWhere(string $column, string $operator, mixed $value): static
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type' => 'AND', 'expr' => "$column $operator $placeholder"];
        return $this;
    }

    /**
     * Adds an OR condition to the WHERE clause.
     */
    public function orWhere(string $column, string $operator, mixed $value): static
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type' => 'OR', 'expr' => "$column $operator $placeholder"];
        return $this;
    }

    /**
     * Adds a raw WHERE condition.
     */
    public function whereRaw(string $sql, array $params = []): static
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
    public function whereIn(string $column, array $values): static
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
    public function whereNotIn(string $column, array $values): static
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
    public function whereBetween(string $column, mixed $min, mixed $max): static
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
    public function whereNull(string $column): static
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
    public function whereNotNull(string $column): static
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
    public function orWhereNull(string $column): static
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
    public function orWhereNotNull(string $column): static
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
    public function whereExists(string $subquery, array $params = []): static
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
    public function whereNotExists(string $subquery, array $params = []): static
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
    public function whereLike(string $column, string $pattern): static
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
    public function orWhereLike(string $column, string $pattern): static
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
    public function whereGroup(callable $callback): static
    {
        $subBuilder = new static($this->conn);
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
    public function orWhereGroup(callable $callback): static
    {
        $subBuilder = new static($this->conn);
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
    public function andWhereGroup(callable $callback): static
    {
        $subBuilder = new static($this->conn);
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
    public function orWhereRaw(string $sql, array $params = []): static
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

    /**
     * Adds a WHERE NOT LIKE condition.
     */
    public function whereNotLike(string $column, string $pattern): static
    {
        $placeholder = $this->addParam($pattern);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column NOT LIKE $placeholder",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE NOT LIKE condition.
     */
    public function orWhereNotLike(string $column, string $pattern): static
    {
        $placeholder = $this->addParam($pattern);
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column NOT LIKE $placeholder",
        ];
        return $this;
    }

    /**
     * Adds a WHERE NOT BETWEEN condition.
     */
    public function whereNotBetween(string $column, mixed $min, mixed $max): static
    {
        $minPlaceholder = $this->addParam($min);
        $maxPlaceholder = $this->addParam($max);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column NOT BETWEEN $minPlaceholder AND $maxPlaceholder",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE IN condition.
     */
    public function orWhereIn(string $column, array $values): static
    {
        if (empty($values)) {
            $this->parts['where'][] = ['type' => 'OR', 'expr' => '1=0'];
            return $this;
        }

        $placeholders = array_map(fn($v) => $this->addParam($v), $values);
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column IN (" . implode(', ', $placeholders) . ")",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE NOT IN condition.
     */
    public function orWhereNotIn(string $column, array $values): static
    {
        if (empty($values)) {
            return $this;
        }

        $placeholders = array_map(fn($v) => $this->addParam($v), $values);
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column NOT IN (" . implode(', ', $placeholders) . ")",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE BETWEEN condition.
     */
    public function orWhereBetween(string $column, mixed $min, mixed $max): static
    {
        $minPlaceholder = $this->addParam($min);
        $maxPlaceholder = $this->addParam($max);
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column BETWEEN $minPlaceholder AND $maxPlaceholder",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE NOT BETWEEN condition.
     */
    public function orWhereNotBetween(string $column, mixed $min, mixed $max): static
    {
        $minPlaceholder = $this->addParam($min);
        $maxPlaceholder = $this->addParam($max);
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column NOT BETWEEN $minPlaceholder AND $maxPlaceholder",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE EXISTS condition.
     */
    public function orWhereExists(string $subquery, array $params = []): static
    {
        foreach ($params as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "EXISTS ($subquery)",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE NOT EXISTS condition.
     */
    public function orWhereNotExists(string $subquery, array $params = []): static
    {
        foreach ($params as $value) {
            $placeholder = $this->addParam($value);
            $subquery = preg_replace('/\?/', $placeholder, $subquery, 1);
        }
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "NOT EXISTS ($subquery)",
        ];
        return $this;
    }

    /**
     * Adds a WHERE condition comparing two columns.
     */
    public function whereColumn(string $column1, string $operator, string $column2): static
    {
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "$column1 $operator $column2",
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE condition comparing two columns.
     */
    public function orWhereColumn(string $column1, string $operator, string $column2): static
    {
        $this->parts['where'][] = [
            'type' => 'OR',
            'expr' => "$column1 $operator $column2",
        ];
        return $this;
    }

    /**
     * Adds a WHERE DATE() condition.
     */
    public function whereDate(string $column, string $operator, string $date): static
    {
        $placeholder = $this->addParam($date);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "DATE($column) $operator $placeholder",
        ];
        return $this;
    }

    /**
     * Adds a WHERE YEAR() condition.
     */
    public function whereYear(string $column, string $operator, int $year): static
    {
        $placeholder = $this->addParam($year);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "YEAR($column) $operator $placeholder",
        ];
        return $this;
    }

    /**
     * Adds a WHERE MONTH() condition.
     */
    public function whereMonth(string $column, string $operator, int $month): static
    {
        $placeholder = $this->addParam($month);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "MONTH($column) $operator $placeholder",
        ];
        return $this;
    }

    /**
     * Adds a WHERE DAY() condition.
     */
    public function whereDay(string $column, string $operator, int $day): static
    {
        $placeholder = $this->addParam($day);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "DAY($column) $operator $placeholder",
        ];
        return $this;
    }

    /**
     * Adds a WHERE TIME() condition.
     */
    public function whereTime(string $column, string $operator, string $time): static
    {
        $placeholder = $this->addParam($time);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "TIME($column) $operator $placeholder",
        ];
        return $this;
    }

    /**
     * Adds a WHERE JSON_CONTAINS() condition.
     */
    public function whereJsonContains(string $column, string $path, mixed $value): static
    {
        $placeholder = $this->addParam(json_encode($value));
        $pathPlaceholder = $this->addParam($path);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "JSON_CONTAINS($column, $placeholder, $pathPlaceholder)",
        ];
        return $this;
    }

    /**
     * Adds a WHERE JSON_LENGTH() condition.
     */
    public function whereJsonLength(string $column, string $operator, int $length, ?string $path = null): static
    {
        $placeholder = $this->addParam($length);
        $expr = $path
            ? "JSON_LENGTH($column, " . $this->addParam($path) . ") $operator $placeholder"
            : "JSON_LENGTH($column) $operator $placeholder";

        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => $expr,
        ];
        return $this;
    }

    /**
     * Adds a WHERE condition for JSON path extraction.
     */
    public function whereJsonExtract(string $column, string $path, string $operator, mixed $value): static
    {
        $pathPlaceholder = $this->addParam($path);
        $valuePlaceholder = $this->addParam($value);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "JSON_EXTRACT($column, $pathPlaceholder) $operator $valuePlaceholder",
        ];
        return $this;
    }

    /**
     * Adds a MATCH() AGAINST() full-text search condition.
     */
    public function whereFullText(string|array $columns, string $search, string $mode = 'NATURAL LANGUAGE'): static
    {
        $cols = is_array($columns) ? implode(', ', $columns) : $columns;
        $placeholder = $this->addParam($search);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "MATCH($cols) AGAINST($placeholder IN $mode MODE)",
        ];
        return $this;
    }

    /**
     * Adds a bitwise AND condition.
     */
    public function whereBitwise(string $column, int $value, string $operator = '&'): static
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "($column $operator $placeholder) > 0",
        ];
        return $this;
    }

    /**
     * Adds a FIND_IN_SET() condition.
     */
    public function whereFindInSet(string $value, string $column): static
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = [
            'type' => empty($this->parts['where']) ? '' : 'AND',
            'expr' => "FIND_IN_SET($placeholder, $column) > 0",
        ];
        return $this;
    }
}
