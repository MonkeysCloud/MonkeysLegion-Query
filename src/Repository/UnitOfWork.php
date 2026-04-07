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
        $conn = $this->manager->write();
        $counts = ['inserts' => 0, 'updates' => 0, 'deletes' => 0];

        $conn->transaction(function () use (&$counts): void {
            // Reuse one QueryBuilder per table to avoid repeated instantiation (#10)
            /** @var array<string, QueryBuilder> $builders */
            $builders = [];
            $getBuilder = function (string $table) use (&$builders): QueryBuilder {
                if (!isset($builders[$table])) {
                    $builders[$table] = (new QueryBuilder($this->manager))->from($table);
                }
                // Reset clauses but keep the FROM table
                $builders[$table]->reset()->from($table);
                return $builders[$table];
            };

            // Inserts
            foreach ($this->pendingInserts as $item) {
                $qb = $getBuilder($item['table']);
                $id = $qb->insert($item['data']);

                // Set the ID on the entity if auto-generated
                if ($id !== false && !is_array($id)) {
                    $this->hydrator->setPropertyValue($item['entity'], 'id', (int) $id);
                    $entityId = (string) $id;
                } else {
                    $entityId = (string) ($item['data']['id'] ?? '');
                }

                // Register in identity map + snapshot
                if ($entityId !== '') {
                    $this->identityMap->set(get_class($item['entity']), $entityId, $item['entity']);
                    $this->snapshot($item['entity']);
                }

                $counts['inserts']++;
            }

            // Updates
            foreach ($this->pendingUpdates as $item) {
                $getBuilder($item['table'])
                    ->where('id', '=', $item['id'])
                    ->update($item['data']);

                // Refresh snapshot
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
}
