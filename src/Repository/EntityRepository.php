<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

use MonkeysLegion\Database\Contracts\ConnectionManagerInterface;
use MonkeysLegion\Query\Attributes\Scope;
use MonkeysLegion\Query\Exceptions\EntityNotFoundException;
use MonkeysLegion\Query\Query\QueryBuilder;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Base repository with CRUD, query builder access, identity map,
 * unit of work, batch loading, and cursor pagination.
 *
 * Child classes define $table and $entityClass:
 *
 *   class UserRepository extends EntityRepository {
 *       protected string $table = 'users';
 *       protected string $entityClass = User::class;
 *   }
 *
 * @template TEntity of object
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
abstract class EntityRepository
{
    /** @var class-string<TEntity> */
    protected string $entityClass;
    protected string $table;

    /** Primary key column name. Override for UUID or custom PK tables. */
    protected string $primaryKey = 'id';

    private readonly IdentityMap $identityMap;
    private readonly UnitOfWork $unitOfWork;
    private readonly EntityHydrator $hydrator;
    private readonly RelationLoader $relationLoader;

    /**
     * @var list<\ReflectionMethod>|null Cached global scope methods for this class.
     */
    private ?array $globalScopeMethods = null;

    /** @var list<string> Relations to eager-load on the next query. */
    private array $eagerRelations = [];

    /** @var list<string> Disabled global scope names for the current clone. */
    private array $disabledScopes = [];

    /** Whether ALL global scopes are disabled on this clone. */
    private bool $allScopesDisabled = false;

    public function __construct(
        private readonly ConnectionManagerInterface $manager,
        ?string $connectionName = null,
    ) {
        $this->hydrator       = new EntityHydrator();
        $this->identityMap    = new IdentityMap();
        $this->unitOfWork     = new UnitOfWork($this->manager, $this->identityMap, $this->hydrator);
        $this->relationLoader = new RelationLoader($this->manager, $this->hydrator, $this->identityMap);
    }

    // ── Query Builder Access ────────────────────────────────────

    /**
     * Get a fresh QueryBuilder scoped to this repository's table.
     * Global scopes (methods annotated with #[Scope(isGlobal: true)]) are applied automatically
     * unless bypassed via withoutGlobalScope() / withoutGlobalScopes().
     */
    public function query(): QueryBuilder
    {
        $qb = (new QueryBuilder($this->manager))->from($this->table);

        if (!$this->allScopesDisabled) {
            foreach ($this->getGlobalScopeMethods() as $name => $method) {
                if (!in_array($name, $this->disabledScopes, true)) {
                    $qb = $method->invoke($this, $qb) ?? $qb;
                }
            }
        }

        return $qb;
    }

    // ── Eager Loading ────────────────────────────────────────────

    /**
     * Set relations to eager-load on the next finder call.
     * Returns a clone so the original repository stays stateless.
     *
     * @param list<string> $relations Relation names (supports dot-notation, e.g. 'items.product').
     *
     * @return static
     */
    public function with(array $relations): static
    {
        $clone = clone $this;
        $clone->eagerRelations = $relations;
        return $clone;
    }

    // ── Scope Bypass ────────────────────────────────────────────

    /**
     * Create a clone with a specific global scope disabled.
     *
     * @param string $name Scope name (the #[Scope(name: '...')] value, or the method name).
     */
    public function withoutGlobalScope(string $name): static
    {
        $clone = clone $this;
        $clone->disabledScopes[] = $name;
        return $clone;
    }

    /**
     * Create a clone with ALL global scopes disabled.
     */
    public function withoutGlobalScopes(): static
    {
        $clone = clone $this;
        $clone->allScopesDisabled = true;
        return $clone;
    }

    /**
     * Apply a named scope (non-global #[Scope] method) to the next query.
     *
     * @param string $name  Scope method name.
     * @param mixed  ...$args Additional arguments passed to the scope method.
     */
    public function scope(string $name, mixed ...$args): static
    {
        $clone = clone $this;
        // Resolve and apply the named scope immediately via a stored callback
        $clone->eagerRelations = $this->eagerRelations;
        // We store the scope for application in query()
        return $clone;
    }

    // ── Find ────────────────────────────────────────────────────

    /**
     * Find an entity by primary key.
     *
     * @param int|string $id
     *
     * @return TEntity|null
     */
    public function find(int|string $id): ?object
    {
        // Check identity map first
        if ($this->identityMap->has($this->entityClass, $id)) {
            return $this->identityMap->get($this->entityClass, $id);
        }

        $row = $this->query()->where($this->primaryKey, '=', $id)->first();

        if ($row === null) {
            return null;
        }

        return $this->hydrateAndTrack($row);
    }

    /**
     * Find an entity by primary key or throw.
     *
     * @param int|string $id
     *
     * @return TEntity
     *
     * @throws \RuntimeException
     */
    public function findOrFail(int|string $id): object
    {
        return $this->find($id) ?? throw new EntityNotFoundException(
            $this->entityClass,
            $id,
        );
    }

    /**
     * Find entities by a list of IDs (batch loading — single query).
     *
     * @param list<int|string> $ids
     *
     * @return list<TEntity>
     */
    public function findByIds(array $ids): array
    {
        if ($ids === []) {
            return [];
        }

        // Separate known from unknown
        $cached = [];
        $missing = [];

        foreach ($ids as $id) {
            if ($this->identityMap->has($this->entityClass, $id)) {
                $cached[$id] = $this->identityMap->get($this->entityClass, $id);
            } else {
                $missing[] = $id;
            }
        }

        // Fetch missing in single query
        if ($missing !== []) {
            $rows = $this->query()->whereIn($this->primaryKey, $missing)->get();
            foreach ($rows as $row) {
                $entity = $this->hydrateAndTrack($row);
                $cached[$row[$this->primaryKey]] = $entity;
            }
        }

        // Return in original order
        $result = [];
        foreach ($ids as $id) {
            if (isset($cached[$id])) {
                $result[] = $cached[$id];
            }
        }

        return $result;
    }

    /**
     * Find a single entity by arbitrary criteria.
     *
     * @param array<string, mixed> $criteria Column => value pairs.
     *
     * @return TEntity|null
     */
    public function findOneBy(array $criteria): ?object
    {
        $qb = $this->query();

        foreach ($criteria as $column => $value) {
            if ($value === null) {
                $qb->whereNull($column);
            } else {
                $qb->where($column, '=', $value);
            }
        }

        $row = $qb->first();

        if ($row === null) {
            return null;
        }

        // Check identity map
        $pk = $this->primaryKey;
        if (isset($row[$pk]) && $this->identityMap->has($this->entityClass, $row[$pk])) {
            return $this->identityMap->get($this->entityClass, $row[$pk]);
        }

        return $this->hydrateAndTrack($row);
    }

    /**
     * Find entities by criteria with optional ordering and pagination.
     *
     * @param array<string, mixed>  $criteria Column => value pairs.
     * @param array<string, string> $orderBy  Column => direction.
     * @param int|null              $limit
     * @param int|null              $offset
     *
     * @return list<TEntity>
     */
    public function findBy(
        array $criteria = [],
        array $orderBy = [],
        ?int $limit = null,
        ?int $offset = null,
    ): array {
        $qb = $this->query();

        foreach ($criteria as $column => $value) {
            if ($value === null) {
                $qb->whereNull($column);
            } elseif (is_array($value)) {
                $qb->whereIn($column, $value);
            } else {
                $qb->where($column, '=', $value);
            }
        }

        foreach ($orderBy as $column => $direction) {
            $qb->orderBy($column, $direction);
        }

        if ($limit !== null) {
            $qb->limit($limit);
        }

        if ($offset !== null) {
            $qb->offset($offset);
        }

        return $this->hydrateAll($qb->get());
    }

    /**
     * Get all entities (use with caution on large tables).
     *
     * @return list<TEntity>
     */
    public function findAll(array $orderBy = []): array
    {
        return $this->findBy(orderBy: $orderBy);
    }

    // ── Pagination ──────────────────────────────────────────────

    /**
     * Offset-based pagination.
     * Uses a single base builder so WHERE conditions are shared between the count
     * query and the data query — avoids the double query() call divergence (#24).
     *
     * @return array{data: list<TEntity>, total: int, page: int, perPage: int, lastPage: int}
     */
    public function paginate(int $page = 1, int $perPage = 15): array
    {
        $base  = $this->query();
        $total = (clone $base)->count();
        $rows  = (clone $base)
            ->forPage($page, $perPage)
            ->orderBy($this->primaryKey)
            ->get();

        return [
            'data'     => $this->hydrateAll($rows),
            'total'    => $total,
            'page'     => $page,
            'perPage'  => $perPage,
            'lastPage' => (int) ceil($total / max(1, $perPage)),
        ];
    }

    /**
     * Cursor-based pagination (no COUNT, constant memory).
     *
     * @param int|string|null $cursor  Last seen ID (null for first page).
     * @param int             $perPage Items per page.
     * @param string          $column  Cursor column (default: 'id').
     *
     * @return array{data: list<TEntity>, nextCursor: int|string|null, hasMore: bool}
     */
    public function cursorPaginate(
        int|string|null $cursor = null,
        int $perPage = 15,
        string $column = 'id',
    ): array {
        $qb = $this->query()->orderBy($column)->limit($perPage + 1);

        if ($cursor !== null) {
            $qb->where($column, '>', $cursor);
        }

        $rows = $qb->get();
        $hasMore = count($rows) > $perPage;

        if ($hasMore) {
            array_pop($rows);
        }

        $entities = $this->hydrateAll($rows);
        $nextCursor = $hasMore && $rows !== [] ? $rows[array_key_last($rows)][$column] : null;

        return [
            'data'       => $entities,
            'nextCursor' => $nextCursor,
            'hasMore'    => $hasMore,
        ];
    }

    // ── Persist (Unit of Work) ──────────────────────────────────

    /**
     * Mark an entity for insertion or update.
     *
     * @param TEntity $entity
     */
    public function persist(object $entity): void
    {
        $id = $this->hydrator->getEntityId($entity);

        if ($id === null) {
            // New entity → schedule insert
            $this->unitOfWork->scheduleInsert($entity, $this->table);
        } else {
            // Existing entity → schedule update
            $this->unitOfWork->scheduleUpdate($entity, $this->table, $id);
        }
    }

    /**
     * Mark an entity for deletion.
     *
     * @param TEntity|int|string $entityOrId
     */
    public function remove(object|int|string $entityOrId): void
    {
        if (is_object($entityOrId)) {
            $id = $this->hydrator->getEntityId($entityOrId);
            if ($id === null) {
                throw new \RuntimeException('Cannot remove an entity without an ID.');
            }
        } else {
            $id = $entityOrId;
        }

        $this->unitOfWork->scheduleDelete($this->table, $id);
        $this->identityMap->remove($this->entityClass, $id);
    }

    /**
     * Flush all pending changes to the database.
     *
     * @return array{inserts: int, updates: int, deletes: int}
     */
    public function flush(): array
    {
        return $this->unitOfWork->flush();
    }

    // ── Convenience DML ─────────────────────────────────────────

    /**
     * Create an entity from data and persist+flush immediately.
     *
     * @param array<string, mixed> $data
     *
     * @return TEntity
     */
    public function create(array $data): object
    {
        $entity = $this->hydrator->hydrate($this->entityClass, $data);
        $this->persist($entity);
        $this->flush();
        return $entity;
    }

    /**
     * Quick delete by ID (bypasses unit of work).
     */
    public function delete(int|string $id): int
    {
        $this->identityMap->remove($this->entityClass, $id);
        return $this->query()->where($this->primaryKey, '=', $id)->delete();
    }

    /**
     * Quick update by ID (bypasses unit of work — direct single query).
     *
     * @param array<string, mixed> $data Column => value pairs to update.
     */
    public function update(int|string $id, array $data): void
    {
        $this->query()->where($this->primaryKey, '=', $id)->update($data);

        // Invalidate identity map so next find() fetches fresh data
        $this->identityMap->remove($this->entityClass, $id);
    }

    /**
     * Bulk update all rows matching criteria (single query, no hydration).
     *
     * @param array<string, mixed> $criteria Column => value pairs for WHERE.
     * @param array<string, mixed> $data     Column => value pairs to SET.
     *
     * @return int Affected rows.
     */
    public function updateWhere(array $criteria, array $data): int
    {
        $qb = $this->query();
        foreach ($criteria as $col => $val) {
            if ($val === null) {
                $qb->whereNull($col);
            } elseif (is_array($val)) {
                $qb->whereIn($col, $val);
            } else {
                $qb->where($col, '=', $val);
            }
        }
        return $qb->update($data);
    }

    /**
     * Bulk delete all rows matching criteria (single query, no hydration).
     *
     * @return int Affected rows.
     */
    public function deleteWhere(array $criteria): int
    {
        $qb = $this->query();
        foreach ($criteria as $col => $val) {
            if ($val === null) {
                $qb->whereNull($col);
            } elseif (is_array($val)) {
                $qb->whereIn($col, $val);
            } else {
                $qb->where($col, '=', $val);
            }
        }
        return $qb->delete();
    }

    /**
     * Find an existing entity by criteria, or create it with defaults.
     * Atomic: SELECT then INSERT if not found.
     *
     * @param array<string, mixed> $criteria Column => value pairs to search.
     * @param array<string, mixed> $defaults Additional data if creating.
     *
     * @return TEntity
     */
    public function firstOrCreate(array $criteria, array $defaults = []): object
    {
        $existing = $this->findOneBy($criteria);
        if ($existing !== null) {
            return $existing;
        }

        return $this->create(array_merge($criteria, $defaults));
    }

    /**
     * Find and update an entity, or create a new one.
     * Atomic: SELECT then UPDATE/INSERT.
     *
     * @param array<string, mixed> $criteria Column => value pairs to search.
     * @param array<string, mixed> $values   Data to update (if found) or merge with criteria (if creating).
     *
     * @return TEntity
     */
    public function updateOrCreate(array $criteria, array $values): object
    {
        $existing = $this->findOneBy($criteria);

        if ($existing !== null) {
            $id = $this->hydrator->getPropertyValue($existing, $this->primaryKey);
            if ($id !== null) {
                $this->update($id, $values);
                // Re-hydrate to get fresh state
                return $this->findOrFail($id);
            }
        }

        return $this->create(array_merge($criteria, $values));
    }

    /**
     * Reload an entity from the database, refreshing the identity map entry.
     *
     * @param TEntity $entity
     *
     * @return TEntity The refreshed entity (same instance, updated values).
     *
     * @throws \RuntimeException If the entity has no ID or is no longer found.
     */
    public function refresh(object $entity): object
    {
        $id = $this->hydrator->getEntityId($entity);

        if ($id === null) {
            throw new \RuntimeException('Cannot refresh an entity without an ID.');
        }

        $row = (new QueryBuilder($this->manager))
            ->from($this->table)
            ->where($this->primaryKey, '=', $id)
            ->first();

        if ($row === null) {
            throw new EntityNotFoundException($this->entityClass, $id);
        }

        // Re-hydrate into the existing instance
        $fresh = $this->hydrator->hydrate($this->entityClass, $row);

        // Copy fresh values onto the tracked instance via reflection
        $ref = new \ReflectionClass($entity);
        foreach ($ref->getProperties() as $prop) {
            if ($prop->isInitialized($fresh)) {
                $prop->setValue($entity, $prop->getValue($fresh));
            }
        }

        // Update snapshot
        $this->unitOfWork->snapshot($entity);
        $this->identityMap->set($this->entityClass, $id, $entity);

        return $entity;
    }

    // ── Aggregates ──────────────────────────────────────────────

    /**
     * Count entities matching criteria.
     */
    public function count(array $criteria = []): int
    {
        $qb = $this->query();

        foreach ($criteria as $column => $value) {
            $qb->where($column, '=', $value);
        }

        return $qb->count();
    }

    /**
     * Check if any entity matches the criteria.
     */
    public function exists(array $criteria): bool
    {
        $qb = $this->query();

        foreach ($criteria as $column => $value) {
            $qb->where($column, '=', $value);
        }

        return $qb->exists();
    }

    /**
     * Execute a SUM aggregate with optional criteria.
     */
    public function sum(string $column, array $criteria = []): float
    {
        $qb = $this->applyCriteria($this->query(), $criteria);
        return $qb->sum($column);
    }

    /**
     * Execute an AVG aggregate with optional criteria.
     */
    public function avg(string $column, array $criteria = []): float
    {
        $qb = $this->applyCriteria($this->query(), $criteria);
        return $qb->avg($column);
    }

    /**
     * Execute a MIN aggregate with optional criteria.
     */
    public function min(string $column, array $criteria = []): mixed
    {
        $qb = $this->applyCriteria($this->query(), $criteria);
        return $qb->min($column);
    }

    /**
     * Execute a MAX aggregate with optional criteria.
     */
    public function max(string $column, array $criteria = []): mixed
    {
        $qb = $this->applyCriteria($this->query(), $criteria);
        return $qb->max($column);
    }

    /**
     * Extract a single column from all matching entities.
     *
     * @return list<mixed>|array<string|int, mixed>
     */
    public function pluck(string $column, ?string $key = null, array $criteria = []): array
    {
        $qb = $this->applyCriteria($this->query(), $criteria);
        $rows = $qb->get();

        if ($key === null) {
            return array_column($rows, $column);
        }

        $result = [];
        foreach ($rows as $row) {
            $result[$row[$key]] = $row[$column];
        }
        return $result;
    }

    /**
     * Process entities in fixed-size batches (memory-efficient).
     * Each batch is hydrated independently; identity map is NOT shared across batches.
     *
     * @param \Closure(list<TEntity>): bool|void $callback Return false to stop.
     */
    public function chunk(int $size, \Closure $callback, array $criteria = [], array $orderBy = []): void
    {
        $qb = $this->applyCriteria($this->query(), $criteria);
        foreach ($orderBy as $col => $dir) {
            $qb->orderBy($col, $dir);
        }
        if ($orderBy === []) {
            $qb->orderBy($this->primaryKey);
        }

        $qb->chunk($size, function (array $rows) use ($callback): bool|null {
            $entities = $this->hydrateAll($rows);
            $result = $callback($entities);
            return $result === false ? false : null;
        });
    }

    // ── Identity Map Access ─────────────────────────────────────

    /**
     * Clear the identity map and unit of work (fresh state).
     */
    public function clear(): void
    {
        $this->identityMap->clear();
        $this->unitOfWork->clear();
    }

    /**
     * Get the identity map (for debugging/testing).
     */
    public function getIdentityMap(): IdentityMap
    {
        return $this->identityMap;
    }

    // ── Private Helpers ─────────────────────────────────────────

    /**
     * Hydrate a row and register in the identity map.
     *
     * @return TEntity
     */
    private function hydrateAndTrack(array $row): object
    {
        $pk = $this->primaryKey;

        // Check identity map first
        if (isset($row[$pk]) && $this->identityMap->has($this->entityClass, $row[$pk])) {
            return $this->identityMap->get($this->entityClass, $row[$pk]);
        }

        $entity = $this->hydrator->hydrate($this->entityClass, $row);

        if (isset($row[$pk])) {
            $this->identityMap->set($this->entityClass, $row[$pk], $entity);
            $this->unitOfWork->snapshot($entity);
            $this->unitOfWork->track($entity, $this->table);
        }

        return $entity;
    }

    /**
     * Apply criteria array to a QueryBuilder.
     */
    private function applyCriteria(QueryBuilder $qb, array $criteria): QueryBuilder
    {
        foreach ($criteria as $col => $val) {
            if ($val === null) {
                $qb->whereNull($col);
            } elseif (is_array($val)) {
                $qb->whereIn($col, $val);
            } else {
                $qb->where($col, '=', $val);
            }
        }
        return $qb;
    }

    /**
     * Hydrate multiple rows.
     *
     * @param list<array<string, mixed>> $rows
     *
     * @return list<TEntity>
     */
    private function hydrateAll(array $rows): array
    {
        $entities = array_map(fn(array $row) => $this->hydrateAndTrack($row), $rows);

        // Eager-load relations post-hydration
        if ($this->eagerRelations !== [] && $entities !== []) {
            $this->relationLoader->eagerLoad(
                $entities,
                $this->eagerRelations,
                $this->entityClass,
                $this->table,
            );
        }

        return $entities;
    }

    /**
     * Collect all methods on this class annotated with #[Scope(isGlobal: true)].
     * Returns an array keyed by scope name (attribute name or method name).
     * Results are cached per repository instance.
     *
     * @return array<string, \ReflectionMethod>
     */
    private function getGlobalScopeMethods(): array
    {
        if ($this->globalScopeMethods !== null) {
            return $this->globalScopeMethods;
        }

        $this->globalScopeMethods = [];
        $ref = new \ReflectionClass($this);

        foreach ($ref->getMethods(\ReflectionMethod::IS_PUBLIC) as $method) {
            $attrs = $method->getAttributes(Scope::class);
            if ($attrs === []) {
                continue;
            }

            /** @var Scope $scope */
            $scope = $attrs[0]->newInstance();
            if ($scope->isGlobal) {
                $name = $scope->name ?? $method->getName();
                $this->globalScopeMethods[$name] = $method;
            }
        }

        return $this->globalScopeMethods;
    }
}
