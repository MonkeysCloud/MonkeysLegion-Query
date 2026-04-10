<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Attributes;

use Attribute;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Marks a repository method as a query scope.
 *
 * Global scopes (isGlobal: true) are automatically applied to
 * every query on the repository. Per-query scopes can be applied
 * manually via ->scope('methodName').
 *
 * Usage:
 *   #[Scope]
 *   public function active(QueryBuilder $qb): QueryBuilder {
 *       return $qb->where('status', '=', 'active');
 *   }
 *
 *   #[Scope(isGlobal: true)]
 *   public function notDeleted(QueryBuilder $qb): QueryBuilder {
 *       return $qb->whereNull('deleted_at');
 *   }
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
#[Attribute(Attribute::TARGET_METHOD)]
final readonly class Scope
{
    public function __construct(
        /** Whether this scope is applied globally to all queries. */
        public bool $isGlobal = false,

        /** Optional name override (defaults to method name). */
        public ?string $name = null,
    ) {}
}
