<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

use MonkeysLegion\Database\Contracts\ConnectionManagerInterface;
use MonkeysLegion\Query\Query\QueryBuilder;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Unit of Work that collects entity changes and flushes them
 * in a single transaction. Inserts, updates, and deletes are
 * batched for optimal performance.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class UnitOfWork
{
    /** @var list<array{entity: object, table: string, data: array<string, mixed>}> */
    private array $pendingInserts = [];

    /** @var list<array{entity: object, table: string, data: array<string, mixed>, id: string|int}> */
    private array $pendingUpdates = [];

    /** @var list<array{table: string, id: string|int}> */
    private array $pendingDeletes = [];

    /** @var array<int, array<string, mixed>> objectId → original snapshot */
    private array $snapshots = [];

    public function __construct(
        private readonly ConnectionManagerInterface $manager,
        private readonly IdentityMap $identityMap,
        private readonly EntityHydrator $hydrator,
    ) {}

    /**
     * Schedule an entity for insertion.
     */
    public function scheduleInsert(object $entity, string $table): void
    {
        $data = $this->hydrator->dehydrate($entity);
        $this->pendingInserts[] = ['entity' => $entity, 'table' => $table, 'data' => $data];
    }

    /**
     * Schedule an entity for update (only changed fields).
     */
    public function scheduleUpdate(object $entity, string $table, string|int $id, string $primaryKey = 'id'): void
    {
        $objectId = spl_object_id($entity);
        $currentData = $this->hydrator->dehydrate($entity);
        $originalData = $this->snapshots[$objectId] ?? [];

        // Compute delta
        $changed = [];
        foreach ($currentData as $key => $value) {
            if ($key === $primaryKey) {
                continue;
            }
            $original = $originalData[$key] ?? null;

            if ($value instanceof \DateTimeInterface && $original instanceof \DateTimeInterface) {
                if ($value->format('Y-m-d H:i:s') !== $original->format('Y-m-d H:i:s')) {
                    $changed[$key] = $value;
                }
            } elseif ($value !== $original) {
                $changed[$key] = $value;
            }
        }

        if ($changed !== []) {
            $this->pendingUpdates[] = ['entity' => $entity, 'table' => $table, 'data' => $changed, 'id' => $id, 'primaryKey' => $primaryKey];
        }
    }

    /**
     * Schedule an entity for deletion.
     */
    public function scheduleDelete(string $table, string|int $id, string $primaryKey = 'id'): void
    {
        $this->pendingDeletes[] = ['table' => $table, 'id' => $id, 'primaryKey' => $primaryKey];
    }

    /**
     * Take a snapshot of an entity's current state (for change detection).
     */
    public function snapshot(object $entity): void
    {
        $objectId = spl_object_id($entity);
        $this->snapshots[$objectId] = $this->hydrator->dehydrate($entity);
    }

    /**
     * Flush all pending changes in a single transaction.
     *
     * @return array{inserts: int, updates: int, deletes: int}
     */
    public function flush(): array
    {
        // Auto-detect dirty entities before flushing
        $this->computeChangeSets();

        $conn = $this->manager->write();
        $counts = ['inserts' => 0, 'updates' => 0, 'deletes' => 0];

        $conn->transaction(function () use (&$counts): void {
            /** @var array<string, QueryBuilder> $builders */
            $builders = [];
            $getBuilder = function (string $table) use (&$builders): QueryBuilder {
                if (!isset($builders[$table])) {
                    $builders[$table] = (new QueryBuilder($this->manager))->from($table);
                }
                $builders[$table]->reset()->from($table);
                return $builders[$table];
            };

            // Group inserts by table for batch optimization
            $grouped = [];
            foreach ($this->pendingInserts as $item) {
                $grouped[$item['table']][] = $item;
            }

            foreach ($grouped as $table => $items) {
                // Split into explicit-ID rows (can batch) vs auto-increment (must insert individually)
                $explicitId = [];
                $autoIncrement = [];
                foreach ($items as $item) {
                    if (isset($item['data']['id']) && $item['data']['id'] !== '' && $item['data']['id'] !== null) {
                        $explicitId[] = $item;
                    } else {
                        $autoIncrement[] = $item;
                    }
                }

                // Batch insert rows with explicit IDs
                if (count($explicitId) > 1) {
                    $rows = array_map(fn($item) => $item['data'], $explicitId);
                    $qb = $getBuilder($table);
                    $qb->insertMany($rows);

                    foreach ($explicitId as $item) {
                        $entityId = (string) $item['data']['id'];
                        $this->identityMap->set(get_class($item['entity']), $entityId, $item['entity']);
                        $this->snapshot($item['entity']);
                        $counts['inserts']++;
                    }
                } elseif (count($explicitId) === 1) {
                    // Single explicit-ID insert
                    $item = $explicitId[0];
                    $qb = $getBuilder($item['table']);
                    $qb->insert($item['data']);
                    $entityId = (string) $item['data']['id'];
                    $this->identityMap->set(get_class($item['entity']), $entityId, $item['entity']);
                    $this->snapshot($item['entity']);
                    $counts['inserts']++;
                }

                // Insert auto-increment rows individually to capture generated IDs
                foreach ($autoIncrement as $item) {
                    $qb = $getBuilder($item['table']);
                    $id = $qb->insert($item['data']);

                    if ($id !== false && !is_array($id)) {
                        // Preserve type: numeric IDs get cast to int, string IDs stay as-is
                        $typedId = is_numeric($id) ? (int) $id : $id;
                        $this->hydrator->setPropertyValue($item['entity'], 'id', $typedId);
                        $entityId = (string) $id;
                    } else {
                        $entityId = (string) ($item['data']['id'] ?? '');
                    }

                    if ($entityId !== '') {
                        $this->identityMap->set(get_class($item['entity']), $entityId, $item['entity']);
                        $this->snapshot($item['entity']);
                    }

                    $counts['inserts']++;
                }
            }

            // Updates
            foreach ($this->pendingUpdates as $item) {
                $pk = $item['primaryKey'] ?? 'id';
                $getBuilder($item['table'])
                    ->where($pk, '=', $item['id'])
                    ->update($item['data']);

                $this->snapshot($item['entity']);
                $counts['updates']++;
            }

            // Deletes
            foreach ($this->pendingDeletes as $item) {
                $pk = $item['primaryKey'] ?? 'id';
                $getBuilder($item['table'])
                    ->where($pk, '=', $item['id'])
                    ->delete();

                $counts['deletes']++;
            }
        });

        // Clear pending
        $this->pendingInserts = [];
        $this->pendingUpdates = [];
        $this->pendingDeletes = [];

        return $counts;
    }

    /**
     * Check if there are any pending changes.
     */
    public function hasPendingChanges(): bool
    {
        return $this->pendingInserts !== []
            || $this->pendingUpdates !== []
            || $this->pendingDeletes !== [];
    }

    /**
     * Discard all pending changes.
     */
    public function clear(): void
    {
        $this->pendingInserts = [];
        $this->pendingUpdates = [];
        $this->pendingDeletes = [];
        $this->snapshots = [];
    }

    /**
     * Scan all snapshotted entities for changes and auto-schedule updates.
     * Called automatically by flush() — no need for explicit persist() on tracked entities.
     */
    public function computeChangeSets(): void
    {
        foreach ($this->snapshots as $objectId => $originalData) {
            // Find the entity via identity map reverse lookup
            $entity = $this->findEntityByObjectId($objectId);
            if ($entity === null) {
                continue; // Entity was GC'd or removed
            }

            $primaryKey = $this->resolvePrimaryKeyForEntity($entity);
            $currentData = $this->hydrator->dehydrate($entity);
            $id = $currentData[$primaryKey] ?? null;
            if ($id === null) {
                continue;
            }

            // Check if already scheduled
            foreach ($this->pendingUpdates as $pending) {
                if ($pending['entity'] === $entity) {
                    continue 2;
                }
            }

            // Compute delta
            $changed = [];
            foreach ($currentData as $key => $value) {
                if ($key === $primaryKey) {
                    continue;
                }

                $original = $originalData[$key] ?? null;
                if ($value instanceof \DateTimeInterface && $original instanceof \DateTimeInterface) {
                    if ($value->format('Y-m-d H:i:s') !== $original->format('Y-m-d H:i:s')) {
                        $changed[$key] = $value;
                    }
                } elseif ($value !== $original) {
                    $changed[$key] = $value;
                }
            }

            if ($changed !== []) {
                $table = $this->resolveTableForEntity($entity);
                if ($table !== null) {
                    $this->pendingUpdates[] = [
                        'entity' => $entity,
                        'table' => $table,
                        'data' => $changed,
                        'id' => $id,
                        'primaryKey' => $primaryKey,
                    ];
                }
            }
        }
    }

    /**
     * Get the count of all pending operations.
     *
     * @return array{inserts: int, updates: int, deletes: int}
     */
    public function getPendingCount(): array
    {
        return [
            'inserts' => count($this->pendingInserts),
            'updates' => count($this->pendingUpdates),
            'deletes' => count($this->pendingDeletes),
        ];
    }

    /** @var array<int, array{entity: object, table: string, primaryKey: string}> objectId → entity+table+pk for reverse lookup */
    private array $trackedEntities = [];

    /**
     * Register an entity for auto-dirty detection.
     */
    public function track(object $entity, string $table, string $primaryKey = 'id'): void
    {
        $this->trackedEntities[spl_object_id($entity)] = ['entity' => $entity, 'table' => $table, 'primaryKey' => $primaryKey];
    }

    private function findEntityByObjectId(int $objectId): ?object
    {
        return $this->trackedEntities[$objectId]['entity'] ?? null;
    }

    private function resolveTableForEntity(object $entity): ?string
    {
        return $this->trackedEntities[spl_object_id($entity)]['table'] ?? null;
    }

    private function resolvePrimaryKeyForEntity(object $entity): string
    {
        return $this->trackedEntities[spl_object_id($entity)]['primaryKey'] ?? 'id';
    }
}
