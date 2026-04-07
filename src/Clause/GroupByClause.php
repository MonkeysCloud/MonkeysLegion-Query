<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable GROUP BY clause entry.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class GroupByClause implements ExpressionInterface
{
    /**
     * @param list<string> $columns Columns to group by.
     */
    public function __construct(
        public array $columns,
    ) {}

    public function toSql(): string
    {
        return implode(', ', $this->columns);
    }

    public function getBindings(): array
    {
        return [];
    }
}
