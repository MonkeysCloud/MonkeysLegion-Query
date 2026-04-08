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
    public function scheduleUpdate(object $entity, string $table, string|int $id): void
    {
        $objectId = spl_object_id($entity);
        $currentData = $this->hydrator->dehydrate($entity);
        $originalData = $this->snapshots[$objectId] ?? [];

        // Compute delta
        $changed = [];
        foreach ($currentData as $key => $value) {
            if ($key === 'id') {
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
            $this->pendingUpdates[] = ['entity' => $entity, 'table' => $table, 'data' => $changed, 'id' => $id];
        }
    }

    /**
     * Schedule an entity for deletion.
     */
    public function scheduleDelete(string $table, string|int $id): void
    {
        $this->pendingDeletes[] = ['table' => $table, 'id' => $id];
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
                if (count($items) === 1) {
                    // Single insert — use standard insert
                    $item = $items[0];
                    $qb = $getBuilder($item['table']);
                    $id = $qb->insert($item['data']);

                    if ($id !== false && !is_array($id)) {
                        $this->hydrator->setPropertyValue($item['entity'], 'id', (int) $id);
                        $entityId = (string) $id;
                    } else {
                        $entityId = (string) ($item['data']['id'] ?? '');
                    }

                    if ($entityId !== '') {
                        $this->identityMap->set(get_class($item['entity']), $entityId, $item['entity']);
                        $this->snapshot($item['entity']);
                    }

                    $counts['inserts']++;
                } else {
                    // Batch insert — use insertMany for N rows in 1 query
                    $rows = array_map(fn($item) => $item['data'], $items);
                    $qb = $getBuilder($table);
                    $qb->insertMany($rows);

                    // For batch inserts, we can't reliably get individual IDs
                    // from all DB drivers, so we re-query if needed
                    foreach ($items as $item) {
                        $entityId = (string) ($item['data']['id'] ?? '');
                        if ($entityId !== '') {
                            $this->identityMap->set(get_class($item['entity']), $entityId, $item['entity']);
                            $this->snapshot($item['entity']);
                        }
                        $counts['inserts']++;
                    }
                }
            }

            // Updates
            foreach ($this->pendingUpdates as $item) {
                $getBuilder($item['table'])
                    ->where('id', '=', $item['id'])
                    ->update($item['data']);

                $this->snapshot($item['entity']);
                $counts['updates']++;
            }

            // Deletes
            foreach ($this->pendingDeletes as $item) {
                $getBuilder($item['table'])
                    ->where('id', '=', $item['id'])
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

            $currentData = $this->hydrator->dehydrate($entity);
            $id = $currentData['id'] ?? null;
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
                if ($key === 'id') {
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
                // Resolve table from pending inserts or use entity class name
                $table = $this->resolveTableForEntity($entity);
                if ($table !== null) {
                    $this->pendingUpdates[] = [
                        'entity' => $entity,
                        'table' => $table,
                        'data' => $changed,
                        'id' => $id,
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

    /** @var array<int, array{entity: object, table: string}> objectId → entity+table for reverse lookup */
    private array $trackedEntities = [];

    /**
     * Register an entity for auto-dirty detection.
     */
    public function track(object $entity, string $table): void
    {
        $this->trackedEntities[spl_object_id($entity)] = ['entity' => $entity, 'table' => $table];
    }

    private function findEntityByObjectId(int $objectId): ?object
    {
        return $this->trackedEntities[$objectId]['entity'] ?? null;
    }

    private function resolveTableForEntity(object $entity): ?string
    {
        return $this->trackedEntities[spl_object_id($entity)]['table'] ?? null;
    }
}
