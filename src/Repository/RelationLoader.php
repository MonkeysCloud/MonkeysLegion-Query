<?php

declare(strict_types=1);

namespace MonkeysLegion\Repository;

use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Entity\Attributes\OneToOne;
use ReflectionClass;
use ReflectionProperty;

abstract class RelationLoader extends EntityHelper
{
    /**
     * Check if an entity can have deeper relations loaded
     *
     * @param object $entity
     * @return bool
     */
    private function canGoDeeper(object $entity): bool
    {
        return $this->context->getDepth($entity) < ($this->context->maxDepth + 1);
    }

    /**
     * Load all relationships for an entity.
     *
     * @param object $entity The entity to load relationships for
     * @param array<string, mixed> $loadedClasses Track which entity classes have been loaded to prevent infinite recursion
     * @throws \ReflectionException
     */
    protected function loadRelations(object $entity, array $loadedClasses = []): void
    {
        if (!$this->canGoDeeper($entity)) {
            return;
        }

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

        $ref = self::reflect($entity);

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
                $this->loadManyToManyWithGuard($entity, $prop, $attr);
            }
        }
    }

    /**
     * Load relations with guard against circular references
     *
     * @param object $entity
     */
    protected function loadRelationsWithGuard(object $entity): void
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

        // Early return if we've reached max depth
        if (!$this->canGoDeeper($entity)) {
            return;
        }

        $ref = self::reflect($entity);

        foreach ($ref->getProperties() as $prop) {
            $isCollectionRelation = false;
            try {
                // ManyToOne
                if ($manyToOneAttrs = $prop->getAttributes(ManyToOne::class)) {
                    /** @var ManyToOne $attr */
                    $attr = $manyToOneAttrs[0]->newInstance();
                    $this->loadManyToOneWithGuard($entity, $prop, $attr);
                }

                // OneToOne owning side
                if ($oneToOneAttrs = $prop->getAttributes(OneToOne::class)) {
                    /** @var OneToOne $attr */
                    $attr = $oneToOneAttrs[0]->newInstance();
                    if (!$attr->mappedBy) {
                        $this->loadOneToOneWithGuard($entity, $prop, $attr);
                    } else {
                        $this->loadOneToOneInverse($entity, $prop, $attr); // <- correct loader
                    }
                }

                // OneToMany
                if ($oneToManyAttrs = $prop->getAttributes(OneToMany::class)) {
                    $isCollectionRelation = true;
                    /** @var OneToMany $attr */
                    $attr = $oneToManyAttrs[0]->newInstance();
                    $this->loadOneToManyWithGuard($entity, $prop, $attr);
                }

                // ManyToMany
                if ($manyToManyAttrs = $prop->getAttributes(ManyToMany::class)) {
                    $isCollectionRelation = true;
                    /** @var ManyToMany $attr */
                    $attr = $manyToManyAttrs[0]->newInstance();
                    $this->loadManyToManyWithGuard($entity, $prop, $attr);
                }
            } catch (\Exception $e) {
                error_log("Failed to load relation {$prop->getName()}: " . $e->getMessage());
                $this->safeSetProperty($prop, $entity, $isCollectionRelation ? [] : null);
            }
        }
    }

    protected function loadOneToOneInverse(object $entity, ReflectionProperty $prop, OneToOne $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);

        // Early check for max depth
        if (!$this->canGoDeeper($entity)) {
            $this->safeSetProperty($prop, $entity, null);
            return;
        }

        $ownId = $this->getEntityId($entity);
        if (!$ownId) {
            $this->safeSetProperty($prop, $entity, null);
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
            $this->safeSetProperty($prop, $entity, null);
            return;
        }

        if ($this->context->hasInstance($targetClass, $fkValue)) {
            $instance = $this->context->getInstance($targetClass, $fkValue);
            $this->safeSetProperty($prop, $entity, $instance);

            // Check if we need to load relations for the existing instance
            // This is important if this instance was registered but its relations weren't loaded
            if ($instance && $this->canGoDeeper($instance)) {
                $this->loadRelationsWithGuard($instance);
            }
            return;
        }

        // Single query (previously duplicated)
        $qb = clone $this->qb;
        $row = $qb->select(['t.*'])
            ->from($targetTable, 't')
            ->where("t.id", '=', $fkValue)
            ->fetch($targetClass);
        if ($row) {
            $this->safeSetProperty($prop, $entity, $row);

            // Set the inverse side
            $inverseProp = $mappedBy ?: $inversedBy;
            if ($inverseProp && property_exists($row, $inverseProp)) {
                $row->$inverseProp = $entity;
            }

            // Calculate the new depth - one level deeper than current entity
            $newDepth = $currEntityDepth + 1;

            if (!$this->context->hasInstance($targetClass, $fkValue)) {
                $this->context->registerInstance($targetClass, $fkValue, $row);
                $this->context->setDepth($row, $newDepth);
            } else {
                // Only update if the new path is shorter
                $existingDepth = $this->context->getDepth($row);
                if ($newDepth < $existingDepth) {
                    $this->context->setDepth($row, $newDepth);
                }
            }

            // Only load relations if we're not at max depth
            if ($this->canGoDeeper($row)) {
                $this->loadRelationsWithGuard($row);
            }
        }
    }

    /**
     * Load ManyToOne with guard
     *
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param ManyToOne $attr
     */
    protected function loadManyToOneWithGuard(object $entity, ReflectionProperty $prop, ManyToOne $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);

        // Early check for max depth - consistent with other relationships
        if (!$this->canGoDeeper($entity)) {
            $this->safeSetProperty($prop, $entity, null);
            return;
        }

        // Resolve FK column on THIS table for this relation property
        $fkColumn = $this->getRelationColumnName($prop->getName());


        // Current entity PK
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $this->safeSetProperty($prop, $entity, null);
            return;
        }

        // Try to read FK directly from the entity if it truly has that field initialized
        $fkValue = $this->getEntityPropValue($entity, $fkColumn);

        // If FK not present on the object, fetch it minimally by THIS row's ID
        if ($fkValue === null) {
            $qb = clone $this->qb;
            $row = $qb->select([$fkColumn])
                ->from($this->tableOf(get_class($entity)))
                ->where('id', '=', $entityId)
                ->fetch();

            $fkValue = $this->getRowValue($row, $fkColumn);
        }

        // Normalize FK to string|null
        $fkValue = $this->normalizeFk($fkValue);

        if ($fkValue === null) {
            $this->safeSetProperty($prop, $entity, null);
            return;
        }

        // Reuse from context or from the already-loaded map
        $targetClass = $attr->targetEntity;

        if ($this->context->hasInstance($targetClass, $fkValue)) {
            $instance = $this->context->getInstance($targetClass, $fkValue);
            if (!$instance) {
                $this->safeSetProperty($prop, $entity, null);
                return;
            }
            $this->safeSetProperty($prop, $entity, $instance);

            // Only update depth if new path is shorter
            $newDepth = $currEntityDepth + 1;
            $existingDepth = $this->context->getDepth($instance);
            if ($newDepth < $existingDepth) {
                $this->context->setDepth($instance, $newDepth);

                // Important: If we found a shorter path to this entity,
                // we might need to load more relations now
                if ($this->canGoDeeper($instance)) {
                    $this->loadRelationsWithGuard($instance);
                }
            }
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

            // Register the entity in our context with the proper depth
            if (!$this->context->hasInstance($targetClass, $fkValue)) {
                $this->context->registerInstance($targetClass, $fkValue, $relatedEntity);
                $this->context->setDepth($relatedEntity, $newDepth);
            } else {
                // Only update depth if new path is shorter
                $existingDepth = $this->context->getDepth($relatedEntity);
                if ($newDepth < $existingDepth) {
                    $this->context->setDepth($relatedEntity, $newDepth);
                }
            }

            $this->safeSetProperty($prop, $entity, $relatedEntity);

            // Only load deeper relations if we're not at max depth
            if ($this->canGoDeeper($relatedEntity)) {
                $this->loadRelationsWithGuard($relatedEntity);
            }
        } else {
            // Broken FK → null out the relation (alternatively: log/throw)
            error_log("Warning: Related entity {$targetClass} with ID {$fkValue} not found for ManyToOne");
            $this->safeSetProperty($prop, $entity, null);
        }
    }

    /**
     * Load OneToOne with guard
     *
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param OneToOne $attr
     */
    protected function loadOneToOneWithGuard(object $entity, ReflectionProperty $prop, OneToOne $attr): void
    {
        // Same as ManyToOne for owning side
        $this->loadManyToOneWithGuard($entity, $prop, new ManyToOne(
            targetEntity: $attr->targetEntity,
            inversedBy: $attr->inversedBy,
            nullable: $attr->nullable
        ));
    }

    /**
     * Load OneToMany with guard
     *
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param OneToMany $attr
     */
    protected function loadOneToManyWithGuard(
        object $entity,
        ReflectionProperty $prop,
        OneToMany $attr,
    ): void {
        $currEntityDepth = $this->context->getDepth($entity);

        // Early check for max depth - using the helper method
        if (!$this->canGoDeeper($entity)) {
            $this->safeSetProperty($prop, $entity, []);
            return;
        }

        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $this->safeSetProperty($prop, $entity, []);
            return;
        }

        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);

        if (!$attr->mappedBy) {
            $this->safeSetProperty($prop, $entity, []);
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
            if (!$rowId) {
                continue;
            }

            if ($this->context->hasInstance($targetClass, $rowId)) {
                $instance = $this->context->getInstance($targetClass, $rowId);
                if ($instance === null) {
                    continue;
                }
                $dedup[] = $instance;

                // Update depth if new path is shorter
                $existingDepth = $this->context->getDepth($instance);
                if ($newDepth < $existingDepth) {
                    $this->context->setDepth($instance, $newDepth);

                    // If we found a shorter path, load more relations if possible
                    if ($this->canGoDeeper($instance)) {
                        $this->loadRelationsWithGuard($instance);
                    }
                }
            } else {
                $this->context->registerInstance($targetClass, $rowId, $row);
                $this->context->setDepth($row, $newDepth);
                $dedup[] = $row;

                // Only load relations if we're not at max depth
                if ($this->canGoDeeper($row)) {
                    $this->loadRelationsWithGuard($row);
                }
            }
        }

        $this->safeSetProperty($prop, $entity, $dedup);
    }

    /**
     * Load ManyToMany with guard
     *
     * @param object $entity
     * @param ReflectionProperty $prop
     * @param ManyToMany $attr
     */
    protected function loadManyToManyWithGuard(
        object $entity,
        ReflectionProperty $prop,
        ManyToMany $attr,
    ): void {
        $currEntityDepth = $this->context->getDepth($entity);

        // Early check for max depth - using the helper method for consistency
        if (!$this->canGoDeeper($entity)) {
            $this->safeSetProperty($prop, $entity, []);
            return;
        }

        $ownId = $this->getEntityId($entity);
        if (!$ownId) {
            $this->safeSetProperty($prop, $entity, []);
            return;
        }

        try {
            [$jt, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName(), $entity);
        } catch (\Exception $e) {
            $this->safeSetProperty($prop, $entity, []);
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
            if (!$rowId) {
                continue;
            }

            if ($this->context->hasInstance($targetClass, $rowId)) {
                $instance = $this->context->getInstance($targetClass, $rowId);
                if ($instance === null) {
                    continue;
                }
                $dedup[] = $instance;

                // Update depth if new path is shorter
                $existingDepth = $this->context->getDepth($instance);
                if ($newDepth < $existingDepth) {
                    $this->context->setDepth($instance, $newDepth);

                    // If we found a shorter path, load more relations if possible
                    if ($this->canGoDeeper($instance)) {
                        $this->loadRelationsWithGuard($instance);
                    }
                }
            } else {
                $this->context->registerInstance($targetClass, $rowId, $row);
                $this->context->setDepth($row, $newDepth);
                $dedup[] = $row;

                // Only load relations if we're not at max depth
                if ($this->canGoDeeper($row)) {
                    $this->loadRelationsWithGuard($row);
                }
            }
        }

        $this->safeSetProperty($prop, $entity, $dedup);
    }

    /**
     * Load a ManyToOne relationship.
     */
    protected function loadManyToOne(object $entity, ReflectionProperty $prop, ManyToOne $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);

        // Early check for max depth using helper
        if (!$this->canGoDeeper($entity)) {
            $this->safeSetProperty($prop, $entity, null);
            return;
        }

        // Resolve FK column on THIS table for this relation property
        $fkColumn = $this->getRelationColumnName($prop->getName());

        // Current entity PK
        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $this->safeSetProperty($prop, $entity, null);
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
            $this->safeSetProperty($prop, $entity, null);
            return;
        }

        // Check if the target entity is already loaded in the context
        if ($this->context->hasInstance($attr->targetEntity, $fkValue)) {
            $instance = $this->context->getInstance($attr->targetEntity, $fkValue);
            if (!$instance) {
                $this->safeSetProperty($prop, $entity, null);
                return;
            }
            $this->safeSetProperty($prop, $entity, $instance);

            // Fix: Don't increment instance depth, set it relative to current entity
            $newDepth = $currEntityDepth + 1;
            $existingDepth = $this->context->getDepth($instance);

            // Only update depth if the new path is shallower
            if ($newDepth < $existingDepth) {
                $this->context->setDepth($instance, $newDepth);

                // If we found a shorter path, we might need to load more relations
                if ($this->canGoDeeper($instance)) {
                    $this->loadRelations($instance);
                }
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
            if ($this->canGoDeeper($relatedEntity)) {
                $this->loadRelations($relatedEntity);
            }
        }

        // Set the property value
        $this->safeSetProperty($prop, $entity, $relatedEntity ?: null);
    }

    /**
     * Load a OneToMany relationship.
     */
    protected function loadOneToMany(object $entity, ReflectionProperty $prop, OneToMany $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);

        // Early check using helper
        if (!$this->canGoDeeper($entity)) {
            $this->safeSetProperty($prop, $entity, []);
            return;
        }

        $entityId = $this->getEntityId($entity);
        if (!$entityId) {
            $this->safeSetProperty($prop, $entity, []);
            return;
        }

        $targetTable = $this->tableOf($attr->targetEntity);

        if (!$attr->mappedBy) {
            $this->safeSetProperty($prop, $entity, []);
            return;
        }
        $fkColumn = $this->getRelationColumnName($attr->mappedBy, $targetTable);

        $qb = clone $this->qb;
        $related = $qb->select()
            ->from($targetTable)
            ->where($fkColumn, '=', $entityId)
            ->fetchAll($attr->targetEntity);

        $this->safeSetProperty($prop, $entity, $related);

        // Set proper depth for related entities and load their relations
        $newDepth = $currEntityDepth + 1;
        foreach ($related as $rel) {
            $relId = $this->getEntityId($rel);
            if ($relId === null) {
                continue;
            }

            // Check if we already have this instance
            if ($this->context->hasInstance($attr->targetEntity, $relId)) {
                $instance = $this->context->getInstance($attr->targetEntity, $relId);
                if ($instance === null) {
                    continue;
                }
                $existingDepth = $this->context->getDepth($instance);

                // If we found a shorter path, update depth and reload relations
                if ($newDepth < $existingDepth) {
                    $this->context->setDepth($instance, $newDepth);
                    if ($this->canGoDeeper($instance)) {
                        $this->loadRelations($instance);
                    }
                }
            } else {
                $this->context->registerInstance($attr->targetEntity, $relId, $rel);
                $this->context->setDepth($rel, $newDepth);

                // Only load deeper relations if we're not at max depth
                if ($this->canGoDeeper($rel)) {
                    $this->loadRelations($rel);
                }
            }
        }
    }

    /**
     * Load a OneToOne relationship.
     */
    protected function loadOneToOne(object $entity, ReflectionProperty $prop, OneToOne $attr): void
    {
        $currEntityDepth = $this->context->getDepth($entity);
        if ($currEntityDepth >= $this->context->maxDepth) {
            return;
        }

        // Same as ManyToOne for owning side
        $this->loadManyToOne($entity, $prop, new ManyToOne(
            targetEntity: $attr->targetEntity,
            inversedBy: $attr->inversedBy,
            nullable: $attr->nullable
        ));
    }

    /**
     * Check if a property type allows null values
     */
    protected function isPropertyNullable(ReflectionProperty $prop): bool
    {
        $type = $prop->getType();
        if (!$type) {
            return true; // No type hint = nullable
        }

        if ($type instanceof \ReflectionUnionType) {
            foreach ($type->getTypes() as $unionType) {
                if ($unionType instanceof \ReflectionNamedType && $unionType->getName() === 'null') {
                    return true;
                }
            }
            return false;
        }

        if ($type instanceof \ReflectionNamedType) {
            return $type->allowsNull();
        }

        return true; // Default to nullable for safety
    }

    /**
     * Safely set a property value, respecting nullability constraints
     */
    protected function safeSetProperty(ReflectionProperty $prop, object $entity, mixed $value): void
    {

        if ($value === null && !$this->isPropertyNullable($prop)) {
            // Don't set non-nullable properties to null - leave them uninitialized
            return;
        }

        $prop->setValue($entity, $value);
    }

    // ════════════════════════════════════════════════════════════════════════
    //  BATCH RELATION LOADING — eliminates N+1 for findBy / findAll
    // ════════════════════════════════════════════════════════════════════════

    /**
     * Load all relations for a collection of entities using batched queries.
     *
     * Instead of N queries per relation (one per entity), this issues a single
     * WHERE … IN (…) query per relation type and distributes results back.
     *
     * @param object[] $entities  Hydrated root entities (same class)
     * @param string[]|null $eagerLoad  Relation property names to load (null = all)
     */
    protected function batchLoadRelations(array $entities, ?array $eagerLoad = null): void
    {
        if (empty($entities)) {
            return;
        }

        // Register root entities in context
        foreach ($entities as $entity) {
            $entityId = $this->getEntityId($entity);
            if ($entityId === null) {
                continue;
            }
            $entityClass = get_class($entity);
            if (!$this->context->hasInstance($entityClass, $entityId)) {
                $this->context->registerInstance($entityClass, $entityId, $entity);
                $this->context->setDepth($entity, 0);
            }
        }

        $ref = self::reflect($entities[0]);

        foreach ($ref->getProperties() as $prop) {
            // When eagerLoad is specified, skip properties not in the list
            if ($eagerLoad !== null && !in_array($prop->getName(), $eagerLoad, true)) {
                continue;
            }

            try {
                // ManyToOne — batch by FK
                if ($manyToOneAttrs = $prop->getAttributes(ManyToOne::class)) {
                    $attr = $manyToOneAttrs[0]->newInstance();
                    $this->batchLoadManyToOne($entities, $prop, $attr);
                }

                // OneToOne owning side — same as ManyToOne
                if ($oneToOneAttrs = $prop->getAttributes(OneToOne::class)) {
                    $attr = $oneToOneAttrs[0]->newInstance();
                    if (!$attr->mappedBy) {
                        $this->batchLoadManyToOne($entities, $prop, new ManyToOne(
                            targetEntity: $attr->targetEntity,
                            inversedBy: $attr->inversedBy,
                            nullable: $attr->nullable
                        ));
                    } else {
                        // Inverse side: fall back to per-entity loading (rare)
                        foreach ($entities as $entity) {
                            if ($this->canGoDeeper($entity)) {
                                $this->loadOneToOneInverse($entity, $prop, $attr);
                            }
                        }
                    }
                }

                // OneToMany — batch by parent IDs
                if ($oneToManyAttrs = $prop->getAttributes(OneToMany::class)) {
                    $attr = $oneToManyAttrs[0]->newInstance();
                    $this->batchLoadOneToMany($entities, $prop, $attr);
                }

                // ManyToMany — batch via join table
                if ($manyToManyAttrs = $prop->getAttributes(ManyToMany::class)) {
                    $attr = $manyToManyAttrs[0]->newInstance();
                    $this->batchLoadManyToMany($entities, $prop, $attr);
                }
            } catch (\Exception $e) {
                error_log("Batch relation load failed for {$prop->getName()}: " . $e->getMessage());
                // Fallback: set default value
                $isCollection = $prop->getAttributes(OneToMany::class) || $prop->getAttributes(ManyToMany::class);
                foreach ($entities as $entity) {
                    $this->safeSetProperty($prop, $entity, $isCollection ? [] : null);
                }
            }
        }
    }

    /**
     * Batch-load ManyToOne/OneToOne(owning) relations.
     *
     * Collects all FK values from $entities, loads target entities with a
     * single WHERE id IN (...) query, then assigns them back.
     *
     * If an entity does not expose the FK column as a hydrated property
     * (e.g. `?Author $author` without `author_id`), the FK is fetched
     * from the owning table in a single batched query — matching the
     * fallback behaviour of the non-batch loader.
     */
    private function batchLoadManyToOne(array $entities, ReflectionProperty $prop, ManyToOne $attr): void
    {
        $fkColumn    = $this->getRelationColumnName($prop->getName());
        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);
        $owningTable = $this->tableOf(get_class($entities[0]));

        $qi = fn(string $id): string => $this->qb->quoteIdentifier($id);

        // 1) Collect FK values — track entities that don't expose the FK property
        $fkValues          = [];         // unique FK values to load
        $entityFkMap       = [];         // spl_object_id => normalised FK
        $entitiesMissingFk = [];         // spl_object_id => entity (FK not on the object)
        $idsNeedingLookup  = [];         // owning-table IDs whose FK must be fetched from DB

        foreach ($entities as $entity) {
            if (!$this->canGoDeeper($entity)) {
                $this->safeSetProperty($prop, $entity, null);
                continue;
            }

            $oid     = spl_object_id($entity);
            $fkValue = $this->getEntityPropValue($entity, $fkColumn);
            $fkValue = $this->normalizeFk($fkValue);

            if ($fkValue !== null) {
                $entityFkMap[$oid] = $fkValue;
                if (!$this->context->hasInstance($targetClass, $fkValue)) {
                    $fkValues[$fkValue] = true;
                }
            } else {
                // FK not present on the hydrated entity — queue for DB lookup
                $entitiesMissingFk[$oid] = $entity;
                $idValue = $this->getEntityId($entity);
                if ($idValue !== null) {
                    $idsNeedingLookup[] = $idValue;
                }
            }
        }

        // 1b) Batch-fetch missing FK values from the owning table
        if (!empty($idsNeedingLookup)) {
            $placeholders = implode(',', array_fill(0, count($idsNeedingLookup), '?'));
            $pdo  = $this->qb->pdo();
            $sql  = "SELECT {$qi('id')}, {$qi($fkColumn)} FROM {$qi($owningTable)} WHERE {$qi('id')} IN ({$placeholders})";
            $stmt = $pdo->prepare($sql);
            $stmt->execute(array_values($idsNeedingLookup));
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            $fkById = [];
            foreach ($rows as $row) {
                $rowId = $row['id'] ?? null;
                $rowFk = $this->normalizeFk($row[$fkColumn] ?? null);
                if ($rowId !== null && $rowFk !== null) {
                    $fkById[(string) $rowId] = $rowFk;
                }
            }

            foreach ($entitiesMissingFk as $oid => $entity) {
                $idKey = $this->getEntityId($entity);
                if ($idKey === null || !isset($fkById[$idKey])) {
                    continue;
                }
                $fk = $fkById[$idKey];
                $entityFkMap[$oid] = $fk;
                if (!$this->context->hasInstance($targetClass, $fk)) {
                    $fkValues[$fk] = true;
                }
            }
        }

        // 2) Batch-fetch missing target entities
        $uniqueFks = array_keys($fkValues);
        if (!empty($uniqueFks)) {
            $placeholders = implode(',', array_fill(0, count($uniqueFks), '?'));
            $pdo  = $this->qb->pdo();
            $sql  = "SELECT * FROM {$qi($targetTable)} WHERE {$qi('id')} IN ({$placeholders})";
            $stmt = $pdo->prepare($sql);
            $stmt->execute(array_values($uniqueFks));
            $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

            foreach ($rows as $row) {
                $relatedEntity = \MonkeysLegion\Entity\Hydrator::hydrate($targetClass, $row);
                $relatedId = (string) ($row['id'] ?? '');
                if ($relatedId !== '' && !$this->context->hasInstance($targetClass, $relatedId)) {
                    $this->context->registerInstance($targetClass, $relatedId, $relatedEntity);
                    $this->context->setDepth($relatedEntity, 1);
                }
            }
        }

        // 3) Assign from context
        foreach ($entities as $entity) {
            if (!$this->canGoDeeper($entity)) {
                continue; // already set to null above
            }
            $oid     = spl_object_id($entity);
            $fkValue = $entityFkMap[$oid] ?? null;
            if ($fkValue === null) {
                $this->safeSetProperty($prop, $entity, null);
                continue;
            }
            $related = $this->context->getInstance($targetClass, $fkValue);
            $this->safeSetProperty($prop, $entity, $related);
        }

        // 4) Recursively load deeper relations for newly loaded entities
        foreach ($uniqueFks as $fk) {
            $instance = $this->context->getInstance($targetClass, $fk);
            if ($instance && $this->canGoDeeper($instance)) {
                $this->loadRelationsWithGuard($instance);
            }
        }
    }

    /**
     * Batch-load OneToMany relations.
     *
     * Issues a single SELECT … WHERE fk_col IN (...) to load all children,
     * then groups and distributes them by parent ID.
     */
    private function batchLoadOneToMany(array $entities, ReflectionProperty $prop, OneToMany $attr): void
    {
        if (!$attr->mappedBy) {
            foreach ($entities as $entity) {
                $this->safeSetProperty($prop, $entity, []);
            }
            return;
        }

        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);
        $fkColumn    = $this->getRelationColumnName($attr->mappedBy, $targetTable);

        // Collect parent IDs
        $parentIds = [];
        foreach ($entities as $entity) {
            if (!$this->canGoDeeper($entity)) {
                $this->safeSetProperty($prop, $entity, []);
                continue;
            }
            $eid = $this->getEntityId($entity);
            if ($eid !== null) {
                $parentIds[$eid] = true;
            }
        }

        if (empty($parentIds)) {
            return;
        }

        $uniqueParentIds = array_keys($parentIds);
        $placeholders = implode(',', array_fill(0, count($uniqueParentIds), '?'));
        $pdo = $this->qb->pdo();
        $qi  = fn(string $id): string => $this->qb->quoteIdentifier($id);
        $sql = "SELECT * FROM {$qi($targetTable)} WHERE {$qi($fkColumn)} IN ({$placeholders})";
        $stmt = $pdo->prepare($sql);
        $stmt->execute(array_values($uniqueParentIds));
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        // Group children by FK value
        $grouped = [];
        foreach ($rows as $row) {
            $childEntity = \MonkeysLegion\Entity\Hydrator::hydrate($targetClass, $row);
            $childId = (string) ($row['id'] ?? '');
            $parentFk = (string) ($row[$fkColumn] ?? '');

            if ($childId !== '' && !$this->context->hasInstance($targetClass, $childId)) {
                $this->context->registerInstance($targetClass, $childId, $childEntity);
                $this->context->setDepth($childEntity, 1);
            } elseif ($childId !== '') {
                $childEntity = $this->context->getInstance($targetClass, $childId) ?? $childEntity;
            }

            $grouped[$parentFk][] = $childEntity;
        }

        // Assign grouped children to parent entities
        foreach ($entities as $entity) {
            $eid = $this->getEntityId($entity);
            $children = $grouped[$eid] ?? [];
            $this->safeSetProperty($prop, $entity, $children);
        }

        // Recursively load deeper relations for children
        foreach ($rows as $row) {
            $childId = (string) ($row['id'] ?? '');
            if ($childId !== '') {
                $instance = $this->context->getInstance($targetClass, $childId);
                if ($instance && $this->canGoDeeper($instance)) {
                    $this->loadRelationsWithGuard($instance);
                }
            }
        }
    }

    /**
     * Batch-load ManyToMany relations.
     *
     * Joins through the pivot table with a single WHERE own_col IN (...)
     * and distributes results by owning entity ID.
     */
    private function batchLoadManyToMany(array $entities, ReflectionProperty $prop, ManyToMany $attr): void
    {
        // Collect entity IDs
        $entityIds = [];
        foreach ($entities as $entity) {
            if (!$this->canGoDeeper($entity)) {
                $this->safeSetProperty($prop, $entity, []);
                continue;
            }
            $eid = $this->getEntityId($entity);
            if ($eid !== null) {
                $entityIds[$eid] = true;
            }
        }

        if (empty($entityIds)) {
            return;
        }

        try {
            [$jt, $ownCol, $invCol] = $this->getJoinTableMeta($prop->getName(), $entities[0]);
        } catch (\Exception $e) {
            foreach ($entities as $entity) {
                $this->safeSetProperty($prop, $entity, []);
            }
            return;
        }

        $targetClass = $attr->targetEntity;
        $targetTable = $this->tableOf($targetClass);

        $uniqueIds = array_keys($entityIds);
        $placeholders = implode(',', array_fill(0, count($uniqueIds), '?'));
        $pdo = $this->qb->pdo();
        $qi  = fn(string $id): string => $this->qb->quoteIdentifier($id);

        // Single join query fetches all related records for all parent entities
        $sql = "SELECT t.*, j.{$qi($ownCol)} AS __pivot_owner_id
                FROM {$qi($targetTable)} t
                JOIN {$qi($jt)} j ON j.{$qi($invCol)} = t.{$qi('id')}
                WHERE j.{$qi($ownCol)} IN ({$placeholders})";
        $stmt = $pdo->prepare($sql);
        $stmt->execute(array_values($uniqueIds));
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        // Group by owning entity
        $grouped = [];
        foreach ($rows as $row) {
            $ownerId = (string) ($row['__pivot_owner_id'] ?? '');
            unset($row['__pivot_owner_id']);

            $childEntity = \MonkeysLegion\Entity\Hydrator::hydrate($targetClass, $row);
            $childId = (string) ($row['id'] ?? '');

            if ($childId !== '' && !$this->context->hasInstance($targetClass, $childId)) {
                $this->context->registerInstance($targetClass, $childId, $childEntity);
                $this->context->setDepth($childEntity, 1);
            } elseif ($childId !== '') {
                $childEntity = $this->context->getInstance($targetClass, $childId) ?? $childEntity;
            }

            $grouped[$ownerId][] = $childEntity;
        }

        // Assign
        foreach ($entities as $entity) {
            $eid = $this->getEntityId($entity);
            $this->safeSetProperty($prop, $entity, $grouped[$eid] ?? []);
        }

        // Recursively load deeper relations for related entities
        foreach ($rows as $row) {
            $childId = (string) ($row['id'] ?? '');
            if ($childId !== '') {
                $instance = $this->context->getInstance($targetClass, $childId);
                if ($instance && $this->canGoDeeper($instance)) {
                    $this->loadRelationsWithGuard($instance);
                }
            }
        }
    }
}
