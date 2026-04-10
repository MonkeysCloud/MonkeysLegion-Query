<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Query;

use MonkeysLegion\Query\Enums\JoinType;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Callback-based join condition builder.
 *
 * Usage:
 *   $qb->join('orders', fn(JoinBuilder $j) => $j
 *       ->on('users.id', '=', 'orders.user_id')
 *       ->andOn('orders.status', '=', 'active')
 *   );
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class JoinBuilder
{
    /** @var list<string> */
    private array $conditions = [];

    /** @var list<mixed> */
    private array $bindings = [];

    public function __construct(
        private readonly string $table,
        private readonly ?string $alias,
        private readonly JoinType $type,
    ) {}

    // ── Condition Methods ───────────────────────────────────────

    /**
     * Add an ON column-to-column condition.
     */
    public function on(string $first, string $operator, string $second): self
    {
        $prefix = $this->conditions === [] ? '' : 'AND ';
        $this->conditions[] = "{$prefix}{$first} {$operator} {$second}";
        return $this;
    }

    /**
     * Add an AND ON condition.
     */
    public function andOn(string $first, string $operator, string $second): self
    {
        $this->conditions[] = "AND {$first} {$operator} {$second}";
        return $this;
    }

    /**
     * Add an OR ON condition.
     */
    public function orOn(string $first, string $operator, string $second): self
    {
        $this->conditions[] = "OR {$first} {$operator} {$second}";
        return $this;
    }

    /**
     * Add an ON condition with a bound value.
     */
    public function where(string $column, string $operator, mixed $value): self
    {
        $prefix = $this->conditions === [] ? '' : 'AND ';
        $this->conditions[] = "{$prefix}{$column} {$operator} ?";
        $this->bindings[] = $value;
        return $this;
    }

    /**
     * Add a raw ON condition.
     */
    public function onRaw(string $sql, array $bindings = []): self
    {
        $prefix = $this->conditions === [] ? '' : 'AND ';
        $this->conditions[] = "{$prefix}{$sql}";
        foreach ($bindings as $b) {
            $this->bindings[] = $b;
        }
        return $this;
    }

    // ── Compilation ─────────────────────────────────────────────

    /**
     * Get the compiled ON conditions.
     *
     * @return list<string>
     */
    public function getConditions(): array
    {
        return $this->conditions;
    }

    /**
     * Get parameter bindings from join conditions.
     *
     * @return list<mixed>
     */
    public function getBindings(): array
    {
        return $this->bindings;
    }

    public function getTable(): string
    {
        return $this->table;
    }

    public function getAlias(): ?string
    {
        return $this->alias;
    }

    public function getType(): JoinType
    {
        return $this->type;
    }
}
