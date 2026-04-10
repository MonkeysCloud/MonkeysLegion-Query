<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;
use MonkeysLegion\Query\Enums\SortDirection;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable ORDER BY clause entry.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class OrderByClause implements ExpressionInterface
{
    /**
     * @param string        $column   Column name or raw SQL expression.
     * @param SortDirection $direction
     * @param list<mixed>   $bindings Bound values (used for raw expressions like vector distances).
     */
    public function __construct(
        public string $column,
        public SortDirection $direction = SortDirection::Asc,
        private array $bindings = [],
    ) {}

    #[\Override]
    public function toSql(): string
    {
        return "{$this->column} {$this->direction->value}";
    }

    #[\Override]
    public function getBindings(): array
    {
        return $this->bindings;
    }
}
