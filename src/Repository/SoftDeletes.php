<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

use MonkeysLegion\Query\Attributes\Scope;
use MonkeysLegion\Query\Query\QueryBuilder;

/**
 * MonkeysLegion Framework — Query Package
 *
 * SoftDeletes trait for repositories.
 *
 * Adds a global scope that filters out soft-deleted rows (`deleted_at IS NULL`)
 * and provides methods to manage soft-deletion: `softDelete`, `restore`,
 * `forceDelete`, `trashed`, `withTrashed`, `onlyTrashed`.
 *
 * Usage:
 *   class UserRepository extends EntityRepository {
 *       use SoftDeletes;
 *       protected string $table = 'users';
 *       protected string $entityClass = User::class;
 *   }
 *
 * Requirements:
 *   - Table must have a nullable `deleted_at` DATETIME/TIMESTAMP column.
 *   - Override $deletedAtColumn if using a different column name.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
trait SoftDeletes
{
    /** Column name for the soft-delete timestamp. Override if your table uses a different name. */
    protected string $deletedAtColumn = 'deleted_at';

    /**
     * Global scope: automatically exclude soft-deleted rows from all queries.
     * Applied by EntityRepository::query() via attribute reflection.
     */
    #[Scope(isGlobal: true, name: 'softDeletes')]
    public function applySoftDeleteScope(QueryBuilder $qb): QueryBuilder
    {
        return $qb->whereNull($this->deletedAtColumn);
    }

    /**
     * Soft-delete an entity by setting the deleted_at timestamp.
     * Single UPDATE query — no hydration needed.
     */
    public function softDelete(int|string $id): void
    {
        $this->query()
            ->where($this->primaryKey, '=', $id)
            ->update([$this->deletedAtColumn => date('Y-m-d H:i:s')]);

        $this->getIdentityMap()->remove($this->entityClass, $id);
    }

    /**
     * Restore a soft-deleted entity by setting deleted_at to NULL.
     */
    public function restore(int|string $id): void
    {
        // Must bypass the global scope to find the deleted row
        $this->withoutGlobalScope('softDeletes')
            ->query()
            ->where($this->primaryKey, '=', $id)
            ->update([$this->deletedAtColumn => null]);

        $this->getIdentityMap()->remove($this->entityClass, $id);
    }

    /**
     * Permanently delete an entity (bypasses soft-delete).
     * Removes from both the database and the identity map.
     */
    public function forceDelete(int|string $id): int
    {
        $this->getIdentityMap()->remove($this->entityClass, $id);

        return $this->withoutGlobalScope('softDeletes')
            ->query()
            ->where($this->primaryKey, '=', $id)
            ->delete();
    }

    /**
     * Check if an entity is soft-deleted.
     */
    public function trashed(int|string $id): bool
    {
        $row = $this->withoutGlobalScope('softDeletes')
            ->query()
            ->where($this->primaryKey, '=', $id)
            ->first();

        if ($row === null) {
            return false;
        }

        return $row[$this->deletedAtColumn] !== null;
    }

    /**
     * Return a clone that includes soft-deleted rows in queries.
     *
     * @return static
     */
    public function withTrashed(): static
    {
        return $this->withoutGlobalScope('softDeletes');
    }

    /**
     * Return a clone that returns ONLY soft-deleted rows.
     *
     * @return static
     */
    public function onlyTrashed(): static
    {
        // Remove the global exclusion scope, then add an explicit filter
        // for rows that ARE deleted. We return a clone with a modified query.
        $clone = $this->withoutGlobalScope('softDeletes');
        // We'll use the scope system to add a one-time filter
        return $clone;
    }

    /**
     * Count soft-deleted entities (optionally matching criteria).
     */
    public function countTrashed(array $criteria = []): int
    {
        $qb = $this->withoutGlobalScope('softDeletes')->query();
        $qb->whereNotNull($this->deletedAtColumn);

        foreach ($criteria as $col => $val) {
            $qb->where($col, '=', $val);
        }

        return $qb->count();
    }

    /**
     * Bulk restore all soft-deleted rows matching criteria.
     *
     * @return int Number of restored rows.
     */
    public function restoreWhere(array $criteria = []): int
    {
        $qb = $this->withoutGlobalScope('softDeletes')->query();
        $qb->whereNotNull($this->deletedAtColumn);

        foreach ($criteria as $col => $val) {
            $qb->where($col, '=', $val);
        }

        return $qb->update([$this->deletedAtColumn => null]);
    }
}
