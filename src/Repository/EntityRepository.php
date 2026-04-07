<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

use MonkeysLegion\Database\Contracts\ConnectionManagerInterface;
use MonkeysLegion\Query\Attributes\Scope;
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

    private readonly IdentityMap $identityMap;
    private readonly UnitOfWork $unitOfWork;
    private readonly EntityHydrator $hydrator;

    /**
     * @var list<\ReflectionMethod>|null Cached global scope methods for this class.
     */
    private ?array $globalScopeMethods = null;

    public function __construct(
        private readonly ConnectionManagerInterface $manager,
        ?string $connectionName = null,
    ) {
        $this->hydrator    = new EntityHydrator();
        $this->identityMap = new IdentityMap();
        $this->unitOfWork  = new UnitOfWork($this->manager, $this->identityMap, $this->hydrator);
    }

    // ── Query Builder Access ────────────────────────────────────

    /**
     * Get a fresh QueryBuilder scoped to this repository's table.
     * Global scopes (methods annotated with #[Scope(isGlobal: true)]) are applied automatically.
     */
    public function query(): QueryBuilder
    {
        $qb = (new QueryBuilder($this->manager))->from($this->table);

        foreach ($this->getGlobalScopeMethods() as $method) {
            $qb = $method->invoke($this, $qb) ?? $qb;
        }

        return $qb;
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

        $row = $this->query()->where('id', '=', $id)->first();

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
        return $this->find($id) ?? throw new \RuntimeException(
            "Entity {$this->entityClass} with id '{$id}' not found.",
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
            $rows = $this->query()->whereIn('id', $missing)->get();
            foreach ($rows as $row) {
                $entity = $this->hydrateAndTrack($row);
                $cached[$row['id']] = $entity;
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
        if (isset($row['id']) && $this->identityMap->has($this->entityClass, $row['id'])) {
            return $this->identityMap->get($this->entityClass, $row['id']);
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
            ->orderBy('id')
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
        return $this->query()->where('id', '=', $id)->delete();
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
            ->where('id', '=', $id)
            ->first();

        if ($row === null) {
            throw new \RuntimeException(
                "Entity {$this->entityClass} with id '{$id}' no longer exists.",
            );
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
        // Check identity map first
        if (isset($row['id']) && $this->identityMap->has($this->entityClass, $row['id'])) {
            return $this->identityMap->get($this->entityClass, $row['id']);
        }

        $entity = $this->hydrator->hydrate($this->entityClass, $row);

        if (isset($row['id'])) {
            $this->identityMap->set($this->entityClass, $row['id'], $entity);
            $this->unitOfWork->snapshot($entity);
        }

        return $entity;
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
        return array_map(fn(array $row) => $this->hydrateAndTrack($row), $rows);
    }

    /**
     * Collect all methods on this class annotated with #[Scope(isGlobal: true)].
     * Results are cached per repository class.
     *
     * @return list<\ReflectionMethod>
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
                $this->globalScopeMethods[] = $method;
            }
        }

        return $this->globalScopeMethods;
    }
}
