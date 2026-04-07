<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Query;

use MonkeysLegion\Query\Contracts\ExpressionInterface;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Common Table Expression (CTE) builder.
 *
 * Supports standard and recursive CTEs. Compiles to:
 *   WITH [RECURSIVE] name AS (...) SELECT ...
 *
 * Compatible: MySQL 8.0+, MariaDB 10.2+, PostgreSQL 8.4+, SQLite 3.8.3+
 *
 * Usage:
 *   $qb->withCte('active_users', fn($q) => $q
 *       ->from('users')
 *       ->where('status', '=', 'active')
 *   )->from('active_users')->get();
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class CteBuilder implements ExpressionInterface
{
    /**
     * @var list<array{
     *     name: string,
     *     columns: list<string>,
     *     sql: string,
     *     bindings: list<mixed>,
     *     recursive: bool
     * }>
     */
    private array $ctes = [];

    /**
     * Add a CTE definition.
     *
     * @param string       $name      CTE name.
     * @param string       $sql       CTE body SQL.
     * @param list<mixed>  $bindings  Bound values.
     * @param list<string> $columns   Optional column aliases.
     * @param bool         $recursive Whether the CTE is recursive.
     */
    public function add(
        string $name,
        string $sql,
        array $bindings = [],
        array $columns = [],
        bool $recursive = false,
    ): self {
        $this->ctes[] = [
            'name'      => $name,
            'columns'   => $columns,
            'sql'       => $sql,
            'bindings'  => $bindings,
            'recursive' => $recursive,
        ];

        return $this;
    }

    /**
     * Add a CTE from a QueryBuilder (compiles it).
     */
    public function addFromBuilder(
        string $name,
        QueryBuilder $builder,
        array $columns = [],
        bool $recursive = false,
    ): self {
        $compiled = $builder->compile();
        return $this->add($name, $compiled['sql'], $compiled['bindings'], $columns, $recursive);
    }

    /**
     * Whether this builder has any CTEs defined.
     */
    public function hasCtes(): bool
    {
        return $this->ctes !== [];
    }

    /**
     * Whether any CTE is recursive.
     */
    public function isRecursive(): bool
    {
        foreach ($this->ctes as $cte) {
            if ($cte['recursive']) {
                return true;
            }
        }
        return false;
    }

    public function toSql(): string
    {
        if ($this->ctes === []) {
            return '';
        }

        $recursive = $this->isRecursive() ? 'RECURSIVE ' : '';
        $parts = [];

        foreach ($this->ctes as $cte) {
            $def = $cte['name'];
            if ($cte['columns'] !== []) {
                $def .= '(' . implode(', ', $cte['columns']) . ')';
            }
            $def .= ' AS (' . $cte['sql'] . ')';
            $parts[] = $def;
        }

        return 'WITH ' . $recursive . implode(', ', $parts);
    }

    public function getBindings(): array
    {
        $bindings = [];
        foreach ($this->ctes as $cte) {
            foreach ($cte['bindings'] as $b) {
                $bindings[] = $b;
            }
        }
        return $bindings;
    }
}
