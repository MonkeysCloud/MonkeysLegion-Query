<?php
declare(strict_types=1);

namespace MonkeysLegion\Repository;

use MonkeysLegion\Query\QueryBuilder;

/**
 * Base a repository with common CRUD and query methods.
 *
 * Child classes should be set:
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
     * @return object[]
     */
    public function findAll(array $criteria = []): array
    {
        $qb = clone $this->qb;
        $qb->select()->from($this->table);
        foreach ($criteria as $column => $value) {
            $qb->andWhere($column, '=', $value);
        }
        return $qb->fetchAll($this->entityClass);
    }

    /**
     * Find a single entity by primary key.
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
     * @param array<string,mixed> $criteria
     * @param array<string,string> $orderBy  column => direction
     * @return object[]
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
     */
    public function findOneBy(array $criteria): ?object
    {
        $results = $this->findBy($criteria, [], 1);
        return $results[0] ?? null;
    }

    /**
     * Count entities matching criteria.
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
     */
    public function save(object $entity): int
    {
        $data = get_object_vars($entity);
        $qb   = clone $this->qb;

        if (!empty($data['id'])) {
            $id = $data['id'];
            unset($data['id']);
            return $qb->update($this->table, $data)
                ->where('id', '=', $id)
                ->execute();
        }

        return $qb->insert($this->table, $data);
    }

    /**
     * Delete entity by primary key.
     */
    public function delete(int $id): int
    {
        $qb = clone $this->qb;
        return $qb->delete($this->table)
            ->where('id', '=', $id)
            ->execute();
    }
}
