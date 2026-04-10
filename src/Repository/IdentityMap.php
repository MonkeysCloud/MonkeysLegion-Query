<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

use WeakMap;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Identity map that tracks hydrated entities by class+id.
 * Ensures the same database row always maps to the same object
 * instance within a unit of work scope.
 *
 * Uses SplObjectStorage internally for entity→id reverse lookup,
 * and a nested array for class:id→entity forward lookup.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class IdentityMap
{
    /** @var array<string, array<string, object>> class → [id → entity] */
    private array $entities = [];

    /** @var WeakMap<object, string> entity → "class:id" for reverse lookup */
    private WeakMap $reverseMap;

    public function __construct()
    {
        $this->reverseMap = new WeakMap();
    }

    /**
     * Register an entity in the identity map.
     */
    public function set(string $class, string|int $id, object $entity): void
    {
        $key = (string) $id;
        $this->entities[$class][$key] = $entity;
        $this->reverseMap[$entity] = $class . ':' . $key;
    }

    /**
     * Retrieve an entity from the identity map.
     */
    public function get(string $class, string|int $id): ?object
    {
        return $this->entities[$class][(string) $id] ?? null;
    }

    /**
     * Check if an entity exists in the identity map.
     */
    public function has(string $class, string|int $id): bool
    {
        return isset($this->entities[$class][(string) $id]);
    }

    /**
     * Remove an entity from the identity map.
     */
    public function remove(string $class, string|int $id): void
    {
        $key = (string) $id;
        if (isset($this->entities[$class][$key])) {
            $entity = $this->entities[$class][$key];
            unset($this->reverseMap[$entity]);
            unset($this->entities[$class][$key]);
        }
    }

    /**
     * Get the identity map key for an entity (if tracked).
     *
     * @return array{class: string, id: string}|null
     */
    public function identify(object $entity): ?array
    {
        if (!isset($this->reverseMap[$entity])) {
            return null;
        }

        $key = $this->reverseMap[$entity];
        [$class, $id] = explode(':', $key, 2);
        return ['class' => $class, 'id' => $id];
    }

    /**
     * Get all tracked entities for a given class.
     *
     * @return array<string, object>
     */
    public function allOfClass(string $class): array
    {
        return $this->entities[$class] ?? [];
    }

    /**
     * Total number of tracked entities across all classes.
     */
    public function count(): int
    {
        return array_sum(array_map('count', $this->entities));
    }

    /**
     * Clear all tracked entities.
     */
    public function clear(): void
    {
        $this->entities = [];
        $this->reverseMap = new WeakMap();
    }
}
