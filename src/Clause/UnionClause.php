<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable UNION clause.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class UnionClause implements ExpressionInterface
{
    /**
     * @param string               $sql      The UNION sub-query SQL.
     * @param array<string, mixed> $bindings Bindings for the sub-query.
     * @param bool                 $all      Whether to use UNION ALL.
     */
    public function __construct(
        public string $sql,
        private array $bindings = [],
        public bool $all = false,
    ) {}

    public function toSql(): string
    {
        return ($this->all ? 'UNION ALL ' : 'UNION ') . $this->sql;
    }

    public function getBindings(): array
    {
        return $this->bindings;
    }
}
