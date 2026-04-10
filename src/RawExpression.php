<?php
declare(strict_types=1);

namespace MonkeysLegion\Query;

use MonkeysLegion\Query\Contracts\ExpressionInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * A raw SQL expression with optional parameter bindings.
 * Use when the fluent API cannot express a specific SQL construct.
 *
 * Usage:
 *   $qb->selectRaw(new RawExpression('COUNT(*) AS total'))
 *   $qb->whereRaw(new RawExpression('age > ?', [18]))
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class RawExpression implements ExpressionInterface
{
    /**
     * @param string       $sql      Raw SQL fragment.
     * @param list<mixed>  $bindings Positional parameter values.
     */
    public function __construct(
        public string $sql,
        private array $bindings = [],
    ) {}

    #[\Override]
    public function toSql(): string
    {
        return $this->sql;
    }

    #[\Override]
    public function getBindings(): array
    {
        return $this->bindings;
    }
}
