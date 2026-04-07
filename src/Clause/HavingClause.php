<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable HAVING clause entry.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class HavingClause implements ExpressionInterface
{
    /**
     * @param string               $expression SQL expression (e.g. "COUNT(*) > ?").
     * @param array<string, mixed> $bindings   Bound values.
     */
    public function __construct(
        public string $expression,
        private array $bindings = [],
    ) {}

    public function toSql(): string
    {
        return $this->expression;
    }

    public function getBindings(): array
    {
        return $this->bindings;
    }
}
