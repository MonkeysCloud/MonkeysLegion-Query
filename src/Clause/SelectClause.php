<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable SELECT clause representation.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class SelectClause implements ExpressionInterface
{
    /**
     * @param list<string|ExpressionInterface> $columns  Column names or expressions.
     * @param bool                              $distinct Whether to apply DISTINCT.
     */
    public function __construct(
        public array $columns = ['*'],
        public bool $distinct = false,
    ) {}

    public function toSql(): string
    {
        $cols = [];
        foreach ($this->columns as $col) {
            $cols[] = $col instanceof ExpressionInterface ? $col->toSql() : $col;
        }

        return ($this->distinct ? 'DISTINCT ' : '') . implode(', ', $cols);
    }

    public function getBindings(): array
    {
        $bindings = [];

        foreach ($this->columns as $col) {
            if ($col instanceof ExpressionInterface) {
                $bindings = array_merge($bindings, $col->getBindings());
            }
        }

        return $bindings;
    }
}
