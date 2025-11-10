<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

/**
 * Provides table management operations for the query builder.
 * 
 * Implements methods for managing table name mappings and
 * duplicating query builder instances.
 * 
 * @property array $tableMap Table name mapping for plural/singular forms
 * @property array $parts Query parts storage
 * @property array $params Query parameters
 * @property int $counter Parameter counter
 * @property \MonkeysLegion\Database\Contracts\ConnectionInterface $conn Database connection
 */
trait TableOperations
{
    public function setTableMap(array $map): void
    {
        $this->tableMap = $map + $this->tableMap;
    }

    /**
     * Creates a deep duplicate of the query builder for reuse.
     */
    public function duplicate(): self
    {
        $clone = new self($this->conn);
        $clone->parts = $this->deepCopyArray($this->parts);
        $clone->params = $this->deepCopyArray($this->params);
        $clone->counter = $this->counter;
        return $clone;
    }

    /**
     * Get the current bound parameters for debugging.
     */
    public function getParams(): array
    {
        return $this->params;
    }
}
