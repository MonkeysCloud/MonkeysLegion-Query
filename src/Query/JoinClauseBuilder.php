<?php

namespace MonkeysLegion\Query;

/**
 * Helper class for building complex join conditions.
 * Used internally by joinOn() methods.
 */
class JoinClauseBuilder
{
    private array $conditions = [];

    public function __construct(
        private string $table,
        private string $alias,
        private string $type
    ) {
    }

    /**
     * Adds an ON condition.
     */
    public function on(string $first, string $operator, string $second): static
    {
        $this->conditions[] = [
            'type' => empty($this->conditions) ? '' : 'AND',
            'expr' => "$first $operator $second"
        ];
        return $this;
    }

    /**
     * Adds an AND ON condition.
     */
    public function andOn(string $first, string $operator, string $second): static
    {
        $this->conditions[] = [
            'type' => 'AND',
            'expr' => "$first $operator $second"
        ];
        return $this;
    }

    /**
     * Adds an OR ON condition.
     */
    public function orOn(string $first, string $operator, string $second): static
    {
        $this->conditions[] = [
            'type' => 'OR',
            'expr' => "$first $operator $second"
        ];
        return $this;
    }

    /**
     * Adds a WHERE condition to the join (value binding).
     */
    public function where(string $column, string $operator, mixed $value, object $qb): static
    {
        $placeholder = $qb->addParam($value);
        $this->conditions[] = [
            'type' => empty($this->conditions) ? '' : 'AND',
            'expr' => "$column $operator $placeholder"
        ];
        return $this;
    }

    /**
     * Adds an OR WHERE condition to the join.
     */
    public function orWhere(string $column, string $operator, mixed $value, object $qb): static
    {
        $placeholder = $qb->addParam($value);
        $this->conditions[] = [
            'type' => 'OR',
            'expr' => "$column $operator $placeholder"
        ];
        return $this;
    }

    /**
     * Adds a raw condition.
     */
    public function onRaw(string $sql, array $bindings, object $qb): static
    {
        foreach ($bindings as $value) {
            $placeholder = $qb->addParam($value);
            $sql = preg_replace('/\?/', $placeholder, $sql, 1);
        }

        $this->conditions[] = [
            'type' => empty($this->conditions) ? '' : 'AND',
            'expr' => $sql
        ];
        return $this;
    }

    /**
     * Builds the JOIN SQL.
     */
    public function toSql(object $qb): string
    {
        if (empty($this->conditions)) {
            throw new \RuntimeException("Join must have at least one condition");
        }

        $conditionsSql = [];
        foreach ($this->conditions as $i => $condition) {
            $prefix = $i && $condition['type'] ? ' ' . $condition['type'] . ' ' : '';
            $conditionsSql[] = $prefix . $condition['expr'];
        }

        $onClause = implode('', $conditionsSql);
        return strtoupper($this->type) . " JOIN {$this->table} AS {$this->alias} ON {$onClause}";
    }
}
