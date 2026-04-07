<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable LIMIT/OFFSET clause.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class LimitOffsetClause implements ExpressionInterface
{
    public function __construct(
        public ?int $limit = null,
        public ?int $offset = null,
    ) {}

    public function toSql(): string
    {
        $sql = '';

        if ($this->limit !== null) {
            $sql .= "LIMIT {$this->limit}";
        }

        if ($this->offset !== null) {
            $sql .= ($sql !== '' ? ' ' : '') . "OFFSET {$this->offset}";
        }

        return $sql;
    }

    public function getBindings(): array
    {
        return [];
    }
}
