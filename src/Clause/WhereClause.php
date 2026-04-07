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

    #[\Override]
    public function toSql(): string
    {
        $col = $this->column;
        $op  = $this->operator;

        // Raw SQL fragment — $column already contains the full expression
        if ($op === Operator::Raw) {
            return $col;
        }

        // Grouped sub-conditions — $value holds list<WhereClause>
        if ($op === Operator::Group) {
            return '(' . self::renderGroup((array) $this->value) . ')';
        }

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

    #[\Override]
    public function getBindings(): array
    {
        // Raw operator: bindings supplied via constructor (may be empty)
        if ($this->operator === Operator::Raw) {
            return $this->bindings;
        }

        // Group operator: collect from nested clauses
        if ($this->operator === Operator::Group) {
            $result = [];
            foreach ((array) $this->value as $nested) {
                /** @var self $nested */
                foreach ($nested->getBindings() as $b) {
                    $result[] = $b;
                }
            }
            return $result;
        }

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

    /**
     * Render a list of nested WhereClause objects as a space-separated SQL fragment
     * (without the outer parentheses, which the caller adds).
     *
     * @param list<self> $clauses
     */
    private static function renderGroup(array $clauses): string
    {
        $parts = [];
        foreach ($clauses as $i => $clause) {
            $fragment = $clause->toSql();
            if ($i === 0) {
                $parts[] = $fragment;
            } else {
                $parts[] = $clause->boolean->value . ' ' . $fragment;
            }
        }
        return implode(' ', $parts);
    }
}
