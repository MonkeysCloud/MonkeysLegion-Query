<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;
use MonkeysLegion\Query\Enums\Operator;
use MonkeysLegion\Query\Enums\WhereBoolean;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable WHERE condition representation.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class WhereClause implements ExpressionInterface
{
    /**
     * @param string                                $column   Column name or expression.
     * @param Operator                              $operator SQL operator.
     * @param mixed                                 $value    Bound value(s).
     * @param WhereBoolean                          $boolean  AND / OR connector.
     * @param array<string, mixed>                  $bindings Pre-resolved bindings (for raw/sub-query).
     */
    public function __construct(
        public string $column,
        public Operator $operator,
        public mixed $value = null,
        public WhereBoolean $boolean = WhereBoolean::And,
        private array $bindings = [],
    ) {}

    public function toSql(): string
    {
        $col = $this->column;
        $op  = $this->operator;

        // IS NULL / IS NOT NULL — no value
        if (!$op->requiresValue()) {
            return "{$col} {$op->value}";
        }

        // IN / NOT IN — placeholder list
        if ($op === Operator::In || $op === Operator::NotIn) {
            $count = is_array($this->value) ? count($this->value) : 1;
            $placeholders = implode(', ', array_fill(0, $count, '?'));
            return "{$col} {$op->value} ({$placeholders})";
        }

        // BETWEEN / NOT BETWEEN — two placeholders
        if ($op === Operator::Between || $op === Operator::NotBetween) {
            return "{$col} {$op->value} ? AND ?";
        }

        // EXISTS / NOT EXISTS — sub-query is the value
        if ($op === Operator::Exists || $op === Operator::NotExists) {
            $subSql = $this->value instanceof ExpressionInterface
                ? $this->value->toSql()
                : (string) $this->value;
            return "{$op->value} ({$subSql})";
        }

        // Standard comparison
        return "{$col} {$op->value} ?";
    }

    public function getBindings(): array
    {
        if ($this->bindings !== []) {
            return $this->bindings;
        }

        $op = $this->operator;

        if (!$op->requiresValue()) {
            return [];
        }

        if ($op === Operator::Exists || $op === Operator::NotExists) {
            return $this->value instanceof ExpressionInterface
                ? $this->value->getBindings()
                : [];
        }

        if ($op === Operator::Between || $op === Operator::NotBetween) {
            return is_array($this->value) ? array_values($this->value) : [$this->value];
        }

        if ($op === Operator::In || $op === Operator::NotIn) {
            return is_array($this->value) ? array_values($this->value) : [$this->value];
        }

        return [$this->value];
    }
}
