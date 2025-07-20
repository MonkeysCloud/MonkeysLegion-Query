<?php

declare(strict_types=1);

namespace MonkeysLegion\Repository;

use MonkeysLegion\Entity\Hydrator;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Entity\Attributes\Field;
use ReflectionClass;

/**
 * Base repository with common CRUD and query methods.
 *
 * Child classes should define:
 *   protected string $table;
 *   protected string $entityClass;
 */
abstract class EntityRepository
{
    protected string $table;
    protected string $entityClass;

    public function __construct(protected QueryBuilder $qb) {}

    /**
     * Fetch all entities matching optional criteria.
     *
     * @param array<string,mixed> $criteria
     * @return object[]
     * @throws \ReflectionException
     */
    public function findAll(array $criteria = []): array
    {
        $qb = clone $this->qb;
        $qb->select()->from($this->table);
        foreach ($criteria as $column => $value) {
            $qb->andWhere($column, '=', $value);
        }
        $rows = $qb->fetchAll();
        return array_map(
            fn($r) => Hydrator::hydrate($this->entityClass, $r),
            $rows
        );
    }

    /**
     * Find a single entity by primary key.
     *
     * @param int $id The primary key of the entity to find.
     * @return object|null The found entity or null if not found.
     * @throws \ReflectionException
     */
    public function find(int $id): ?object
    {
        $qb = clone $this->qb;
        $entity = $qb->select()
            ->from($this->table)
            ->where('id', '=', $id)
            ->fetch($this->entityClass);
        return $entity ?: null;
    }

    /**
     * Fetch entities by criteria, with optional ordering and pagination.
     *
     * @param array<string,mixed> $criteria
     * @param array<string,string> $orderBy  column => direction
     * @return object[]
     * @throws \ReflectionException
     */
    public function findBy(
        array $criteria,
        array $orderBy = [],
        int|null $limit = null,
        int|null $offset = null
    ): array {
        $qb = clone $this->qb;
        $qb->select()->from($this->table);
        foreach ($criteria as $column => $value) {
            $qb->andWhere($column, '=', $value);
        }
        foreach ($orderBy as $column => $dir) {
            $qb->orderBy($column, $dir);
        }
        if ($limit !== null)  $qb->limit($limit);
        if ($offset !== null) $qb->offset($offset);
        return $qb->fetchAll($this->entityClass);
    }

    /**
     * Find a single entity by arbitrary criteria.
     *
     * @param array<string,mixed> $criteria
     * @return object|null The found entity or null if not found.
     */
    public function findOneBy(array $criteria): ?object
    {
        $results = $this->findBy($criteria, [], 1);
        return $results[0] ?? null;
    }

    /**
     * Count entities matching criteria.
     *
     * @param array<string,mixed> $criteria
     * @return int The number of entities matching the criteria.
     * @throws \ReflectionException
     */
    public function count(array $criteria = []): int
    {
        $qb = clone $this->qb;
        $qb->select('COUNT(*) AS count')->from($this->table);
        foreach ($criteria as $column => $value) {
            $qb->andWhere($column, '=', $value);
        }
        $row = $qb->fetch();
        return $row?->count ?? 0;
    }

    /**
     * Persist a new or existing entity. Returns insert ID or affected rows.
     *
     * @param object $entity The entity instance to save.
     * @return int The ID of the saved entity or number of affected rows.
     */
    public function save(object $entity): int
    {
        $data = $this->extractFields($entity);
        $qb   = clone $this->qb;

        if (!empty($data['id'])) {
            $id = $data['id'];
            unset($data['id']);
            return $qb->update($this->table, $data)
                ->where('id', '=', $id)
                ->execute();
        }

        $id = $qb->insert($this->table, $data);

        // reflectively set id if property exists
        if (property_exists($entity, 'id')) {
            $ref = new \ReflectionProperty($entity, 'id');
            $ref->setAccessible(true);
            $ref->setValue($entity, $id);
        }

        return $id;
    }

    /**
     * Delete entity by primary key.
     *
     * @param int $id The primary key of the entity to delete.
     * @return int The number of affected rows (should be 1 for successful delete).
     */
    public function delete(int $id): int
    {
        $qb = clone $this->qb;
        return $qb->delete($this->table)
            ->where('id', '=', $id)
            ->execute();
    }

    /**
     * Extract only properties annotated with #[Field] for persistence,
     * skipping uninitialized id on new entities.
     *
     * @param object $entity The entity instance to extract fields from.
     * @return array<string, mixed> An associative array of field names and their values.
     */
    private function extractFields(object $entity): array
    {
        $data = [];
        $ref  = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            // Only include properties marked with #[Field]
            if (! $prop->getAttributes(Field::class)) {
                continue;
            }

            // Skip uninitialized id on insert
            if ($prop->getName() === 'id' && ! $prop->isInitialized($entity)) {
                continue;
            }

            if ($prop->isPrivate() || $prop->isProtected()) {
                $prop->setAccessible(true);
            }

            $data[$prop->getName()] = $prop->getValue($entity);
        }

        return $data;
    }
}
