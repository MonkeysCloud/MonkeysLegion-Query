<?php
declare(strict_types=1);

namespace MonkeysLegion\Repository;

use InvalidArgumentException;
use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Entity\Attributes\OneToOne;
use MonkeysLegion\Entity\Hydrator;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Entity\Attributes\Field;
use ReflectionClass;
use ReflectionProperty;

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
        $data = $this->extractPersistableData($entity);

        // ---- Normalize PHP values → DB scalars ---------------------------------
        foreach ($data as $key => $value) {
            if ($value instanceof \DateTimeInterface) {
                $data[$key] = $value->format('Y-m-d H:i:s');
            } elseif (is_bool($value)) {
                $data[$key] = $value ? 1 : 0;
            } elseif (is_float($value)) {
                $data[$key] = (string)$value;            // keep precision for DECIMAL
            } elseif (is_array($value)) {
                $data[$key] = json_encode($value, JSON_UNESCAPED_UNICODE | JSON_UNESCAPED_SLASHES);
            }
        }

        $pdo = $this->qb->pdo();   // QueryBuilder should expose the PDO. If your class

        // UPDATE path
        if (!empty($data['id'])) {
            $id = (int)$data['id'];
            unset($data['id']);

            if (empty($data)) {
                return 0; // nothing to update
            }

            // build: UPDATE `table` SET `col` = :col, ... WHERE id = :_id
            $setParts = [];
            foreach ($data as $col => $_) {
                $setParts[] = "`{$col}` = :{$col}";
            }
            $sql = "UPDATE `{$this->table}` SET " . implode(', ', $setParts) . " WHERE `id` = :_id";

            $stmt = $pdo->prepare($sql);

            // bind SET values
            foreach ($data as $col => $val) {
                $stmt->bindValue(":{$col}", $val);
            }
            // bind WHERE id
            $stmt->bindValue(":_id", $id, \PDO::PARAM_INT);

            $stmt->execute();
            return $stmt->rowCount();
        }

        // INSERT path
        // build: INSERT INTO `table` (`c1`,`c2`,...) VALUES (:c1,:c2,...)
        $cols        = array_keys($data);
        $placeholders = array_map(fn($c) => ':' . $c, $cols);

        $sql = sprintf(
            "INSERT INTO `%s` (%s) VALUES (%s)",
            $this->table,
            implode(',', array_map(fn($c) => "`{$c}`", $cols)),
            implode(',', $placeholders)
        );

        $stmt = $pdo->prepare($sql);
        foreach ($data as $col => $val) {
            $stmt->bindValue(":{$col}", $val);
        }
        $stmt->execute();

        $id = (int)$pdo->lastInsertId();

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
     * Extract persistable data including Field properties and ManyToOne relationships.
     *
     * @param object $entity The entity instance to extract data from.
     * @return array<string, mixed> An associative array of column names and their values.
     */
    private function extractPersistableData(object $entity): array
    {
        $data = [];
        $ref  = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            // Handle Field attributes
            if ($prop->getAttributes(Field::class)) {
                // Skip uninitialized id on insert
                if ($prop->getName() === 'id' && ! $prop->isInitialized($entity)) {
                    continue;
                }

                if ($prop->isPrivate() || $prop->isProtected()) {
                    $prop->setAccessible(true);
                }

                $data[$prop->getName()] = $prop->getValue($entity);
            }

            // Handle ManyToOne relationships
            $manyToOneAttrs = $prop->getAttributes(ManyToOne::class);
            if ($manyToOneAttrs) {
                if ($prop->isPrivate() || $prop->isProtected()) {
                    $prop->setAccessible(true);
                }

                $relatedEntity = $prop->getValue($entity);
                $columnName = $this->getRelationColumnName($prop->getName());

                if ($relatedEntity === null) {
                    $data[$columnName] = null;
                } else {
                    // Get the ID of the related entity
                    $idProp = new ReflectionProperty($relatedEntity, 'id');
                    $idProp->setAccessible(true);
                    $data[$columnName] = $idProp->getValue($relatedEntity);
                }
            }

            // Handle OneToOne relationships (owning side)
            $oneToOneAttrs = $prop->getAttributes(OneToOne::class);
            if ($oneToOneAttrs) {
                /** @var OneToOne $attr */
                $attr = $oneToOneAttrs[0]->newInstance();

                // Only persist if this is the owning side (no mappedBy)
                if ($attr->mappedBy === null) {
                    if ($prop->isPrivate() || $prop->isProtected()) {
                        $prop->setAccessible(true);
                    }

                    $relatedEntity = $prop->getValue($entity);
                    $columnName = $this->getRelationColumnName($prop->getName());

                    if ($relatedEntity === null) {
                        $data[$columnName] = null;
                    } else {
                        // Get the ID of the related entity
                        $idProp = new ReflectionProperty($relatedEntity, 'id');
                        $idProp->setAccessible(true);
                        $data[$columnName] = $idProp->getValue($relatedEntity);
                    }
                }
            }
        }

        return $data;
    }

    /**
     * Get the database column name for a relation property.
     * Converts camelCase to snake_case and appends _id.
     *
     * @param string $propertyName The property name (e.g., 'media', 'parentCategory')
     * @return string The column name (e.g., 'media_id', 'parent_category_id')
     */
    private function getRelationColumnName(string $propertyName): string
    {
        // Convert camelCase to snake_case
        $snakeCase = strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $propertyName));
        return $snakeCase . '_id';
    }

    /**
     * Extract only properties annotated with #[Field] for persistence,
     * skipping uninitialized id on new entities.
     *
     * @deprecated Use extractPersistableData() instead
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

    /**
     * Find all entities where $relationProp (ManyToMany) contains $relatedId
     *
     * @param string       $relationProp  name of the property on the Entity marked #[ManyToMany]
     * @param int|string   $relatedId     the ID in the inverse table
     * @return object[]                   array of hydrated entities
     * @throws \ReflectionException
     */
    public function findByRelation(string $relationProp, int|string $relatedId): array
    {
        $rclass = new ReflectionClass($this->entityClass);

        if (! $rclass->hasProperty($relationProp)) {
            throw new \InvalidArgumentException("No property $relationProp on {$this->entityClass}");
        }

        $prop  = $rclass->getProperty($relationProp);
        $attrs = $prop->getAttributes(ManyToMany::class);

        if (! $attrs) {
            throw new \InvalidArgumentException("$relationProp is not a ManyToMany relation");
        }

        /** @var ManyToMany $relMeta */
        $relMeta      = $attrs[0]->newInstance();
        $jt           = $relMeta->joinTable;
        $owningSide   = true;

        // 1) if this side has no joinTable, walk to the other side
        if (! $jt) {
            $owningSide = false;
            $otherProp  = $relMeta->mappedBy
                ?? $relMeta->inversedBy
                ?? throw new \InvalidArgumentException(
                    "Relation $relationProp has no joinTable and no mappedBy/inversedBy"
                );

            $targetClass = $relMeta->targetEntity;
            $tclass      = new ReflectionClass($targetClass);

            if (! $tclass->hasProperty($otherProp)) {
                throw new \InvalidArgumentException("No property $otherProp on $targetClass");
            }

            $tprop  = $tclass->getProperty($otherProp);
            $tattrs = $tprop->getAttributes(ManyToMany::class);

            if (! $tattrs) {
                throw new \InvalidArgumentException("$otherProp is not a ManyToMany on $targetClass");
            }

            /** @var ManyToMany $ownerMeta */
            $ownerMeta = $tattrs[0]->newInstance();

            if (! $ownerMeta->joinTable) {
                throw new \InvalidArgumentException(
                    "Neither $relationProp nor $otherProp carry joinTable metadata"
                );
            }

            $jt = $ownerMeta->joinTable;
        }

        // 2) decide which columns to use
        if ($owningSide) {
            // this side declared the joinTable
            $joinCol    = $jt->joinColumn;     // FK → THIS entity ID
            $inverseCol = $jt->inverseColumn;  // FK → RELATED entity ID

            $joinFirst  = "j.$joinCol";
            $joinSecond = "e.id";
            $whereCol   = $inverseCol;
        } else {
            // joinTable came from the OTHER side, so swap
            $joinCol    = $jt->joinColumn;     // FK → OTHER entity ID
            $inverseCol = $jt->inverseColumn;  // FK → THIS entity ID

            $joinFirst  = "j.$inverseCol";
            $joinSecond = "e.id";
            $whereCol   = $joinCol;
        }

        // 3) build & run
        $alias = 'e';
        $qb    = clone $this->qb;

        return $qb
            ->select(["{$alias}.*"])
            ->from($this->table, $alias)
            ->join($jt->name, 'j', $joinFirst, '=', $joinSecond)
            ->where("j.$whereCol", '=', $relatedId)
            ->fetchAll($this->entityClass);
    }

    /**
     * Attach a many-to-many relation.
     *
     * @param object $entity The owning entity instance (must have an ->id).
     * @param string $relationProp The property name on the entity that's #[ManyToMany].
     * @param int|string $value The ID of the related record to attach.
     * @return int                      The number of affected rows (should be 1).
     * @throws \ReflectionException
     */
    public function attachRelation(object $entity, string $relationProp, int|string $value): int
    {
        // [0] = joinTable, [1] = ownCol, [2] = invCol
        [$table, $ownCol, $invCol] = $this->getJoinTableMeta($relationProp);

        // retrieve the ID of this entity
        $ref = new ReflectionProperty($entity::class, 'id');
        $ref->setAccessible(true);
        $ownId = $ref->getValue($entity);

        if (! $ownId) {
            throw new InvalidArgumentException("Entity must have an ID before attaching relations");
        }

        return $this->qb->insert($table, [
            $ownCol => $ownId,
            $invCol => $value,
        ]);
    }

    /**
     * Detach a many-to-many relation.
     *
     * @param object $entity
     * @param string $relationProp
     * @param int|string $value
     * @return int       Rows deleted (usually 1).
     * @throws \ReflectionException
     */
    public function detachRelation(object $entity, string $relationProp, int|string $value): int
    {
        [$table, $ownCol, $invCol] = $this->getJoinTableMeta($relationProp);

        $ref = new ReflectionProperty($entity::class, 'id');
        $ref->setAccessible(true);
        $ownId = $ref->getValue($entity);

        return $this->qb
            ->delete($table)
            ->where($ownCol, '=', $ownId)
            ->andWhere($invCol, '=', $value)
            ->execute();
    }

    /**
     * Internal helper to read #[ManyToMany(..., joinTable: new JoinTable(...))] metadata.
     *
     * @param string $relationProp
     * @return array{0:string,1:string,2:string}  [ join_table, join_column, inverse_column ]
     * @throws \ReflectionException
     */
    private function getJoinTableMeta(string $relationProp): array
    {
        $ownClass = new ReflectionClass($this->entityClass);

        if (! $ownClass->hasProperty($relationProp)) {
            throw new InvalidArgumentException("Property {$relationProp} not found on {$this->entityClass}");
        }

        $prop  = $ownClass->getProperty($relationProp);
        $attrs = $prop->getAttributes(ManyToMany::class);

        if (! $attrs) {
            throw new InvalidArgumentException("{$relationProp} is not a ManyToMany relation");
        }

        /** @var ManyToMany $meta */
        $meta = $attrs[0]->newInstance();
        $jt   = $meta->joinTable;
        $owning = true;

        // if no joinTable here, follow mappedBy/inversedBy to other side
        if (! $jt) {
            $owning = false;
            $otherProp = $meta->mappedBy ?? $meta->inversedBy
                ?? throw new InvalidArgumentException(
                    "Relation {$relationProp} has no joinTable or mappedBy/inversedBy"
                );

            $otherClass = new ReflectionClass($meta->targetEntity);
            if (! $otherClass->hasProperty($otherProp)) {
                throw new InvalidArgumentException("Property {$otherProp} not found on {$meta->targetEntity}");
            }

            $otherAttrs = $otherClass->getProperty($otherProp)
                ->getAttributes(ManyToMany::class);

            if (! $otherAttrs) {
                throw new InvalidArgumentException("{$otherProp} is not a ManyToMany relation on {$meta->targetEntity}");
            }

            /** @var ManyToMany $ownerMeta */
            $ownerMeta = $otherAttrs[0]->newInstance();
            $jt = $ownerMeta->joinTable
                ?? throw new InvalidArgumentException(
                    "Neither {$relationProp} nor {$otherProp} carry joinTable metadata"
                );
        }

        // determine which column belongs to this entity (ownCol) vs. the related (invCol)
        if ($owning) {
            $ownCol = $jt->joinColumn;
            $invCol = $jt->inverseColumn;
        } else {
            // swap columns when using other side's joinTable
            $ownCol = $jt->inverseColumn;
            $invCol = $jt->joinColumn;
        }

        return [$jt->name, $ownCol, $invCol];
    }

}