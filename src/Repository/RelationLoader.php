<?php

declare(strict_types=1);

namespace MonkeysLegion\Repository;

use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Entity\Attributes\OneToOne;
use ReflectionClass;
use ReflectionProperty;

class RelationLoader extends EntityHelper
{
    /**
     * Load all relationships for an entity.
     *
     * @param object $entity The entity to load relationships for
     * @param array<string, mixed> $loadedClasses Track which entity classes have been loaded to prevent infinite recursion
     * @throws \ReflectionException
     */
    protected function loadRelations(object $entity, array $loadedClasses = []): void
    {
        $entityClass = get_class($entity);
        $this->tables[$this->tableOf($entityClass)] = $entityClass;

        $entityId = $this->getEntityId($entity);
        if ($entityId === null) {
            error_log("Skipping hydration: entity {$entityClass} has no ID");
            return;
        }

        // Initialize depth to 0 for root entities if not set
        if (!$this->context->hasInstance($entityClass, $entityId)) {
            $this->context->registerInstance($entityClass, $entityId, $entity);
            $this->context->setDepth($entity, 0);
        }

        $currEntityDepth = $this->context->getDepth($entity);

        if ($currEntityDepth >= $this->context->maxDepth) {
            return;
        }

        $ref = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            // Handle ManyToOne relationships
            if ($manyToOneAttrs = $prop->getAttributes(ManyToOne::class)) {
                /** @var ManyToOne $attr */
                $attr = $manyToOneAttrs[0]->newInstance();
                $this->loadManyToOne($entity, $prop, $attr);
            }

            // Handle OneToOne relationships (owning side)
            if ($oneToOneAttrs = $prop->getAttributes(OneToOne::class)) {
                $attr = $oneToOneAttrs[0]->newInstance();

                if (!$attr->mappedBy) {
                    // Owning side → use FK
                    $this->loadOneToOne($entity, $prop, $attr);
                } else {
                    // Inverse side → need a different loader
                    $this->loadOneToOneInverse($entity, $prop, $attr);
                }
            }

            // Handle OneToMany relationships
            if ($oneToManyAttrs = $prop->getAttributes(OneToMany::class)) {
                /** @var OneToMany $attr */
                $attr = $oneToManyAttrs[0]->newInstance();
                $this->loadOneToMany($entity, $prop, $attr);
            }

            // Handle ManyToMany relationships
            if ($manyToManyAttrs = $prop->getAttributes(ManyToMany::class)) {
                /** @var ManyToMany $attr */
                $attr = $manyToManyAttrs[0]->newInstance();
                $this->loadManyToManyWithGuard($entity, $prop, $attr, $loadedClasses);
            }
        }
    }

    /**
     * Load relations with guard against circular references
     *
     * @param object $entity
     * @param array<string, mixed> $loadedEntities Already loaded entities in this query
     */
    protected function loadRelationsWithGuard(object $entity, array &$loadedEntities = []): void
    {
        $entityClass = get_class($entity);
        $entityId = $this->getEntityId($entity);
        if ($entityId === null) {
            error_log("Skipping hydration: entity {$entityClass} has no ID");
            return;
        }

        // Initialize depth to 0 for root entities if not already registered
        if (!$this->context->hasInstance($entityClass, $entityId)) {
            $this->context->registerInstance($entityClass, $entityId, $entity);
            $this->context->setDepth($entity, 0);
        }

        $currEntityDepth = $this->context->getDepth($entity);

        if ($currEntityDepth >= $this->context->maxDepth) {
            return;
        }

        $ref = new ReflectionClass($entity);

        foreach ($ref->getProperties() as $prop) {
            try {
                $oneToManyAttrs = null;
                $manyToManyAttrs = null;

                // ManyToOne
                if ($manyToOneAttrs = $prop->getAttributes(ManyToOne::class)) {
                    /** @var ManyToOne $attr */
                    $attr = $manyToOneAttrs[0]->newInstance();
                    $this->loadManyToOneWithGuard($entity, $prop, $attr, $loadedEntities);
                }

                // OneToOne owning side
                if ($oneToOneAttrs = $prop->getAttributes(OneToOne::class)) {
                    /** @var OneToOne $attr */
                    $attr = $oneToOneAttrs[0]->newInstance();
                    if (!$attr->mappedBy) {
                        $this->loadOneToOneWithGuard($entity, $prop, $attr, $loadedEntities);
                    } else {
                        $this->loadOneToOneInverse($entity, $prop, $attr); // <- correct loader
                    }
                }

                // OneToMany
                if ($oneToManyAttrs = $prop->getAttributes(OneToMany::class)) {
                    /** @var OneToMany $attr */
                    $attr = $oneToManyAttrs[0]->newInstance();
                    $this->loadOneToManyWithGuard($entity, $prop, $attr, $loadedEntities);
                }

                // ManyToMany
                if ($manyToManyAttrs = $prop->getAttributes(ManyToMany::class)) {
                    /** @var ManyToMany $attr */
                    $attr = $manyToManyAttrs[0]->newInstance();
                    $this->loadManyToManyWithGuard($entity, $prop, $attr, $loadedEntities);
                }
            } catch (\Exception $e) {
                error_log("Failed to load relation {$prop->getName()}: " . $e->getMessage());
                $prop->setAccessible(true);
                if ($oneToManyAttrs || $manyToManyAttrs) {
                    $prop->setValue($entity, []);
                } else {
                    $prop->setValue($entity, null);
                }
            }
        }
    }

    protected function loadOneToOneInverse(object $entity, ReflectionProperty $prop, OneToOne $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);
        if ($currEntityDepth > $this->context->maxDepth) return;

        $ownId = $this->getEntityId($entity);
        if (!$ownId) {
            $prop->setValue($entity, null);
            return;
        }
        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);

        // mappedBy tells us which property on target points back here
        $mappedBy = $attr->mappedBy;

        $inversedBy = $prop->getName();
        $fkColumn = $this->getRelationColumnName($inversedBy); // your helper for FK name
        $fkValue = $this->getEntityPropValue($entity, $fkColumn);

        if ($fkValue === null) {
            $prop->setAccessible(true);
            $prop->setValue($entity, null);
            return;
        }

        if ($this->context->hasInstance($targetClass, $fkValue)) {
            $prop->setAccessible(true);
            $prop->setValue($entity, $this->context->getInstance($targetClass, $fkValue));
            return;
        }

        $qb = clone $this->qb;
        $row = $qb->select(['t.*'])
            ->from($targetTable, 't')
            ->where("t.id", '=', $fkValue)
            ->fetch($targetClass);
        if ($row) {
            $prop->setAccessible(true);
            $prop->setValue($entity, $row);

            // Set the inverse side
            $inverseProp = $mappedBy ?: $inversedBy;
            if ($inverseProp && property_exists($row, $inverseProp)) {
                $row->$inverseProp = $entity;
            }

            $this->context->registerInstance($targetClass, $fkValue, $row);
        }
    }

    /**
     * Load ManyToOne with guard
     * 
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param ManyToOne $attr
     * @param array<string, mixed> $loadedEntities Already loaded entities in this query
     */
    protected function loadManyToOneWithGuard(object $entity, ReflectionProperty $prop, ManyToOne $attr, array &$loadedEntities): void
    {
        $currEntityDepth = $this->context->getDepth($entity);
        if ($currEntityDepth >= $this->context->maxDepth) return;

        $prop->setAccessible(true);

        // Resolve FK column on THIS table for this relation property
        $fkColumn = $this->getRelationColumnName($prop->getName());

        // Current entity PK
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, null);
            return;
        }

        // Try to read FK directly from the entity if it truly has that field initialized
        $fkValue = $this->getEntityPropValue($entity, $fkColumn);

        // If FK not present on the object, fetch it minimally by THIS row's ID
        if ($fkValue === null) {
            $qb = clone $this->qb;
            $row = $qb->select([$fkColumn])
                ->from($this->table)
                ->where('id', '=', $entityId)
                ->fetch();

            $fkValue = $this->getRowValue($row, $fkColumn);
        }

        // Normalize FK to string|null
        $fkValue = $this->normalizeFk($fkValue);

        if ($fkValue === null) {
            $prop->setValue($entity, null);
            return;
        }

        // Reuse from context or from the already-loaded map
        $targetClass = $attr->targetEntity;
        $relatedKey  = $this->context->key($targetClass, $fkValue);

        if ($this->context->hasInstance($targetClass, $fkValue)) {
            $instance = $this->context->getInstance($targetClass, $fkValue);
            if (!$instance) {
                $prop->setValue($entity, null);
                return;
            }
            $prop->setValue($entity, $instance);
            $newDepth = $currEntityDepth + 1;
            $existingDepth = $this->context->getDepth($instance);
            if ($newDepth < $existingDepth) $this->context->setDepth($instance, $newDepth);
            return;
        }

        if (isset($loadedEntities[$relatedKey])) {
            $prop->setValue($entity, $loadedEntities[$relatedKey]);
            return;
        }

        // Fetch related entity by its PK
        $targetTable = $this->tableOf($targetClass);
        $qb2 = clone $this->qb;
        $relatedEntity = $qb2->select()
            ->from($targetTable)
            ->where('id', '=', $fkValue)
            ->fetch($targetClass);

        if ($relatedEntity) {
            $newDepth = $currEntityDepth + 1;
            $this->context->registerInstance($targetClass, $fkValue, $relatedEntity);
            $this->context->setDepth($relatedEntity, $newDepth);
            $loadedEntities[$relatedKey] = $relatedEntity;

            $prop->setValue($entity, $relatedEntity);

            if ($newDepth < $this->context->maxDepth) {
                $this->loadRelationsWithGuard($relatedEntity, $loadedEntities);
            }
        } else {
            // Broken FK → null out the relation (alternatively: log/throw)
            error_log("Warning: Related entity {$targetClass} with ID {$fkValue} not found for ManyToOne");
            $prop->setValue($entity, null);
        }
    }

    /**
     * Load OneToOne with guard
     * 
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param OneToOne $attr
     * @param array<string, mixed> $loadedEntities Already loaded entities in this query
     */
    protected function loadOneToOneWithGuard(object $entity, ReflectionProperty $prop, OneToOne $attr, array &$loadedEntities): void
    {
        $currEntityDepth = $this->context->getDepth($entity);
        if ($currEntityDepth > $this->context->maxDepth) return;

        // Same as ManyToOne for owning side
        $this->loadManyToOneWithGuard($entity, $prop, new ManyToOne(
            targetEntity: $attr->targetEntity,
            inversedBy: $attr->inversedBy,
            nullable: $attr->nullable
        ), $loadedEntities);
    }

    /**
     * Load OneToMany with guard
     * 
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param OneToMany $attr
     * @param array<string, mixed> $loadedEntities Already loaded entities in this query
     */
    protected function loadOneToManyWithGuard(
        object $entity,
        ReflectionProperty $prop,
        OneToMany $attr,
        array &$loadedEntities
    ): void {
        $currEntityDepth = $this->context->getDepth($entity);
        if ($currEntityDepth >= $this->context->maxDepth) return;

        $prop->setAccessible(true);
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, []);
            return;
        }

        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);

        if (!$attr->mappedBy) {
            $prop->setValue($entity, []);
            return;
        }

        // FK lives on the TARGET table
        $fkColumn = $this->getRelationColumnName($attr->mappedBy, $targetTable);

        $qb = clone $this->qb;
        $rows = $qb->select()
            ->from($targetTable)
            ->where($fkColumn, '=', $entityId)
            ->fetchAll($targetClass);

        $newDepth = $currEntityDepth + 1;
        $dedup = [];

        foreach ($rows as $row) {
            $rowId = $this->getEntityId($row);
            if (!$rowId) continue;

            $key = $this->context->key($targetClass, $rowId);

            if ($this->context->hasInstance($targetClass, $rowId)) {
                $instance = $this->context->getInstance($targetClass, $rowId);
                $dedup[] = $instance;
                $loadedEntities[$key] = $instance;
            } else {
                $this->context->registerInstance($targetClass, $rowId, $row);
                $this->context->setDepth($row, $newDepth);
                $dedup[] = $row;
                $loadedEntities[$key] = $row;
                $this->loadRelationsWithGuard($row, $loadedEntities);
            }
        }

        $prop->setValue($entity, $dedup);
    }

    /**
     * Load ManyToMany with guard
     * 
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param ManyToMany $attr
     * @param array<string, mixed> $loadedEntities Already loaded entities in this query
     */
    protected function loadManyToManyWithGuard(
        object             $entity,
        ReflectionProperty $prop,
        ManyToMany         $attr,
        array              &$loadedEntities,
    ): void {
        $currEntityDepth = $this->context->getDepth($entity);
        if ($currEntityDepth >= $this->context->maxDepth) return;

        $prop->setAccessible(true);

        $ownId = $this->getEntityId($entity);
        if (!$ownId) {
            $prop->setValue($entity, []);
            return;
        }

        try {
            [$jt, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName(), $entity);
        } catch (\Exception $e) {
            $prop->setValue($entity, []);
            return;
        }

        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);

        $qb = clone $this->qb;
        $rows = $qb->select(['t.*'])
            ->from($targetTable, 't')
            ->join($jt, 'j', "j.$invCol", '=', 't.id')
            ->where("j.$ownCol", '=', $ownId)
            ->fetchAll($targetClass);

        $newDepth = $currEntityDepth + 1;
        $dedup = [];

        foreach ($rows as $row) {
            $rowId = $this->getEntityId($row);
            if (!$rowId) continue;

            $key = $this->context->key($targetClass, $rowId);

            if ($this->context->hasInstance($targetClass, $rowId)) {
                $instance = $this->context->getInstance($targetClass, $rowId);
                $dedup[] = $instance;
                $loadedEntities[$key] = $instance;
            } else {
                $this->context->registerInstance($targetClass, $rowId, $row);
                $this->context->setDepth($row, $newDepth);
                $dedup[] = $row;
                $loadedEntities[$key] = $row;
            }
        }

        $prop->setValue($entity, $dedup);
    }

    /**
     * Load a ManyToOne relationship.
     */
    protected function loadManyToOne(object $entity, ReflectionProperty $prop, ManyToOne $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);

        if ($currEntityDepth >= $this->context->maxDepth) return;

        $prop->setAccessible(true);

        // Determine FK column
        $fkColumn = $this->getRelationColumnName($prop->getName());

        // Get this entity's ID
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, null);
            return;
        }

        // Try to read FK directly from the entity if it truly has that field initialized
        $fkValue = $this->getEntityPropValue($entity, $fkColumn);

        // If FK not present on the object, fetch it minimally by THIS row's ID
        if ($fkValue === null) {
            $qb = clone $this->qb;
            $targetTable = $this->tableOf(get_class($entity));
            $row = $qb->select([$fkColumn])
                ->from($targetTable)
                ->where('id', '=', $entityId)
                ->fetch();
            $fkValue = $this->getRowValue($row, $fkColumn);
        }

        // Normalize FK to string|null
        $fkValue = $this->normalizeFk($fkValue);

        if ($fkValue === null) {
            $prop->setValue($entity, null);
            return;
        }

        // Check if the target entity is already loaded in the context
        if ($this->context->hasInstance($attr->targetEntity, $fkValue)) {
            $instance = $this->context->getInstance($attr->targetEntity, $fkValue);
            if (!$instance) {
                $prop->setValue($entity, null);
                return;
            }
            $prop->setValue($entity, $instance);

            // Fix: Don't increment instance depth, set it relative to current entity
            $newDepth = $currEntityDepth + 1;
            $existingDepth = $this->context->getDepth($instance);

            // Only update depth if the new path is shallower
            if ($newDepth < $existingDepth) {
                $this->context->setDepth($instance, $newDepth);
            }
            return;
        }

        // Fetch the related entity from its table
        $targetTable = $this->tableOf($attr->targetEntity);
        $qb2 = clone $this->qb;
        $relatedEntity = $qb2->select()
            ->from($targetTable)
            ->where('id', '=', $fkValue)
            ->fetch($attr->targetEntity);

        if ($relatedEntity) {
            // Register this entity in the context with proper depth
            $newDepth = $currEntityDepth + 1;
            $this->context->registerInstance($attr->targetEntity, $fkValue, $relatedEntity);
            $this->context->setDepth($relatedEntity, $newDepth);

            // Recursively load relations for this entity if we're not at max depth
            if ($newDepth < $this->context->maxDepth) {
                $this->loadRelations($relatedEntity);
            }
        }

        // Set the property value
        $prop->setValue($entity, $relatedEntity ?: null);
    }

    /**
     * Load a OneToOne relationship.
     */
    protected function loadOneToOne(object $entity, ReflectionProperty $prop, OneToOne $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);
        if ($currEntityDepth > $this->context->maxDepth) return;

        // Same as ManyToOne for owning side
        $this->loadManyToOne($entity, $prop, new ManyToOne(
            targetEntity: $attr->targetEntity,
            inversedBy: $attr->inversedBy,
            nullable: $attr->nullable
        ));
    }

    /**
     * Load a OneToMany relationship.
     */
    protected function loadOneToMany(object $entity, ReflectionProperty $prop, OneToMany $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);

        if ($currEntityDepth >= $this->context->maxDepth) {
            $prop->setAccessible(true);
            $prop->setValue($entity, []);
            return;
        }

        $prop->setAccessible(true);
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $prop->setValue($entity, []);
            return;
        }

        $targetTable = $this->tableOf($attr->targetEntity);

        if (!$attr->mappedBy) {
            $prop->setValue($entity, []);
            return;
        }
        $fkColumn = $this->getRelationColumnName($attr->mappedBy, $targetTable);

        $qb = clone $this->qb;
        $related = $qb->select()
            ->from($targetTable)
            ->where($fkColumn, '=', $entityId)
            ->fetchAll($attr->targetEntity);

        $prop->setValue($entity, $related);

        // Set proper depth for related entities and load their relations
        $newDepth = $currEntityDepth + 1;
        if ($newDepth < $this->context->maxDepth) {
            foreach ($related as $rel) {
                $relId = $this->getEntityId($rel);
                if ($relId === null) continue;
                $this->context->registerInstance($attr->targetEntity, $relId, $rel);
                $this->context->setDepth($rel, $newDepth);

                // Only load deeper relations if we're not at max depth
                if ($newDepth < $this->context->maxDepth) {
                    $this->loadRelations($rel);
                }
            }
        }
    }
}
