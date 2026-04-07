<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;
use MonkeysLegion\Query\Enums\JoinType;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable JOIN clause representation.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class JoinClause implements ExpressionInterface
{
    /**
     * @param JoinType                  $type       Join type (INNER, LEFT, RIGHT, CROSS).
     * @param string                    $table      Target table name.
     * @param string|null               $alias      Optional table alias.
     * @param list<string>              $conditions ON conditions as SQL fragments.
     * @param array<string, mixed>      $bindings   Parameter bindings for the ON conditions.
     */
    public function __construct(
        public JoinType $type,
        public string $table,
        public ?string $alias = null,
        public array $conditions = [],
        private array $bindings = [],
    ) {}

    #[\Override]
    public function toSql(): string
    {
        $sql = "{$this->type->value} JOIN {$this->table}";

        if ($this->alias !== null) {
            $sql .= " AS {$this->alias}";
        }

        if ($this->conditions !== []) {
            $sql .= ' ON ' . implode(' ', $this->conditions);
        }

        return $sql;
    }

    #[\Override]
    public function getBindings(): array
    {
        return $this->bindings;
    }
}
