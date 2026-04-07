<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Clause;

use MonkeysLegion\Query\Contracts\ExpressionInterface;
use MonkeysLegion\Query\Enums\WhereBoolean;

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
     * @param WhereBoolean         $boolean    AND / OR connector for multiple HAVING clauses.
     */
    public function __construct(
        public string $expression,
        private array $bindings = [],
        public WhereBoolean $boolean = WhereBoolean::And,
    ) {}

    #[\Override]
    public function toSql(): string
    {
        return $this->expression;
    }

    #[\Override]
    public function getBindings(): array
    {
        return $this->bindings;
    }
}
