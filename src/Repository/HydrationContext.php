<?php

declare(strict_types=1);

namespace MonkeysLegion\Repository;

/**
 * HydrationContext keeps track of loaded entities to maintain object identity
 * and prevent infinite recursion during relation hydration.
 */
final class HydrationContext
{
    /**
     * Map of loaded entity instances by class and ID
     * @var array<string, array<int, object>>
     */
    public array $instances = [];

    /** @var array<string, array<int, array>> */
    public array $rows = [];

    /**
     * Track recursion depth for each path
     * @var array<string, int>
     */
    public array $pathCounts = [];

    /**
     * Maximum recursion depth for relation loading
     */
    public int $maxDepth;

    private \SplObjectStorage $meta;

    /**
     * Create a new hydration context
     */
    public function __construct(int $maxDepth = 2)
    {
        $this->maxDepth = $maxDepth;
        $this->meta = new \SplObjectStorage();
    }

    /**
     * Generate a key for an entity class+id pair
     */
    public function key(string $class, int $id): string
    {
        return $class . ':' . $id;
    }

    /**
     * Register an entity instance in the context
     */
    public function registerInstance(string $class, int $id, object $instance): void
    {
        if (!isset($this->instances[$class])) {
            $this->instances[$class] = [];
        }

        $this->instances[$class][$id] = $instance;
    }

    /**
     * Get a registered entity instance if it exists
     */
    public function getInstance(string $class, int $id): ?object
    {
        return $this->instances[$class][$id] ?? null;
    }

    /**
     * Check if an entity has been registered
     */
    public function hasInstance(string $class, int $id): bool
    {
        return isset($this->instances[$class][$id]);
    }

    /**
     * Increment path count for a given path
     */
    public function incrementPathCount(string $path): void
    {
        if (!isset($this->pathCounts[$path])) {
            $this->pathCounts[$path] = 0;
        }

        $this->pathCounts[$path]++;
    }

    /**
     * Get current depth for a path
     */
    public function getPathCount(string $path): int
    {
        return $this->pathCounts[$path] ?? 0;
    }

    /**
     * Check if we've reached max depth for a path
     */
    public function isMaxDepthReached(string $path): bool
    {
        return $this->getPathCount($path) >= $this->maxDepth;
    }

    /**
     * Set depth for an entity with debug logging
     */
    public function setDepth(object $entity, int $depth): void
    {
        $this->meta[$entity] = ['depth' => $depth];
    }

    public function getDepth(object $entity): int
    {
        return $this->meta[$entity]['depth'] ?? 0;
    }

    public function registerRow(string $class, int $id, array $row): void
    {
        if (!isset($this->rows[$class])) {
            $this->rows[$class] = [];
        }
        $this->rows[$class][$id] = $row;
    }

    public function getRow(string $class, int $id): ?array
    {
        return $this->rows[$class][$id] ?? null;
    }

    public function hasRow(string $class, int $id): bool
    {
        return isset($this->rows[$class][$id]);
    }
}
