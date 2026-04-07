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
    public function __construct(
        public string $column,
        public SortDirection $direction = SortDirection::Asc,
    ) {}

    public function toSql(): string
    {
        return "{$this->column} {$this->direction->value}";
    }

    public function getBindings(): array
    {
        return [];
    }
}
