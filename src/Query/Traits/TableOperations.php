<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

use MonkeysLegion\Database\Contracts\ConnectionInterface;
use PDO;

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
 * @property ConnectionInterface $conn Database connection
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

    /**
     * Gets the current table map.
     */
    public function getTableMap(): array
    {
        return $this->tableMap;
    }

    /**
     * Resolves a table name through the table map.
     * If not found in map, returns the original name.
     */
    public function resolveTable(string $table): string
    {
        return $this->tableMap[$table] ?? $table;
    }

    /**
     * Checks if a table name exists in the map.
     */
    public function hasTableMapping(string $table): bool
    {
        return isset($this->tableMap[$table]);
    }

    /**
     * Adds a single table mapping.
     */
    public function addTableMapping(string $from, string $to): self
    {
        $this->tableMap[$from] = $to;
        return $this;
    }

    /**
     * Removes a table mapping.
     */
    public function removeTableMapping(string $table): self
    {
        unset($this->tableMap[$table]);
        return $this;
    }

    /**
     * Clears all table mappings.
     */
    public function clearTableMap(): self
    {
        $this->tableMap = [];
        return $this;
    }

    /**
     * Sets a prefix for table names.
     */
    public function setTablePrefix(string $prefix): self
    {
        $this->parts['tablePrefix'] = $prefix;
        return $this;
    }

    /**
     * Gets the current table prefix.
     */
    public function getTablePrefix(): string
    {
        return $this->parts['tablePrefix'] ?? '';
    }

    /**
     * Applies the table prefix to a table name.
     */
    public function withPrefix(string $table): string
    {
        $prefix = $this->getTablePrefix();

        if (empty($prefix)) {
            return $table;
        }

        // Don't add prefix if it already exists
        if (str_starts_with($table, $prefix)) {
            return $table;
        }

        // Handle table with alias: "users AS u" -> "prefix_users AS u"
        if (preg_match('/^(\S+)(\s+AS\s+\S+)$/i', $table, $matches)) {
            return $prefix . $matches[1] . $matches[2];
        }

        return $prefix . $table;
    }

    /**
     * Removes the table prefix from a table name.
     */
    public function withoutPrefix(string $table): string
    {
        $prefix = $this->getTablePrefix();

        if (empty($prefix) || !str_starts_with($table, $prefix)) {
            return $table;
        }

        return substr($table, strlen($prefix));
    }

    /**
     * Gets the current FROM table.
     */
    public function getTable(): ?string
    {
        return $this->parts['from'] ?? null;
    }

    /**
     * Sets the main table (alternative to from()).
     */
    public function setTable(string $table, ?string $alias = null): self
    {
        return $this->from($table, $alias);
    }

    /**
     * Extracts the table name without alias.
     */
    public function getTableName(): ?string
    {
        $from = $this->getTable();

        if (!$from) {
            return null;
        }

        // Extract table name from "table AS alias" format
        if (preg_match('/^(\S+)(?:\s+AS\s+\S+)?$/i', $from, $matches)) {
            return $matches[1];
        }

        return $from;
    }

    /**
     * Extracts the table alias if one exists.
     */
    public function getTableAlias(): ?string
    {
        $from = $this->getTable();

        if (!$from) {
            return null;
        }

        // Extract alias from "table AS alias" format
        if (preg_match('/\s+AS\s+(\S+)$/i', $from, $matches)) {
            return $matches[1];
        }

        return null;
    }

    /**
     * Gets all query parts.
     */
    public function getParts(): array
    {
        return $this->parts;
    }

    /**
     * Gets a specific query part.
     */
    public function getPart(string $key): mixed
    {
        return $this->parts[$key] ?? null;
    }

    /**
     * Sets a specific query part.
     */
    public function setPart(string $key, mixed $value): self
    {
        $this->parts[$key] = $value;
        return $this;
    }

    /**
     * Checks if a query part exists and is not empty.
     */
    public function hasPart(string $key): bool
    {
        return isset($this->parts[$key]) && !empty($this->parts[$key]);
    }

    /**
     * Removes a query part.
     */
    public function removePart(string $key): self
    {
        unset($this->parts[$key]);
        return $this;
    }

    /**
     * Resets the query builder to initial state.
     * Keeps connection and table map but clears all query parts.
     */
    public function reset(): self
    {
        $this->parts = [
            'select' => '*',
            'from' => null,
            'join' => [],
            'where' => [],
            'groupBy' => [],
            'having' => [],
            'orderBy' => [],
            'limit' => null,
            'offset' => null,
            'distinct' => false,
        ];
        $this->params = [];
        $this->counter = 0;
        return $this;
    }

    /**
     * Clears all bound parameters.
     */
    public function resetBindings(): self
    {
        $this->params = [];
        $this->counter = 0;
        return $this;
    }

    /**
     * Creates a fresh query builder instance with the same connection.
     */
    public function fresh(): self
    {
        return new self($this->conn);
    }

    /**
     * Gets the database connection.
     */
    public function getConnection(): ConnectionInterface
    {
        return $this->conn;
    }

    /**
     * Alias for duplicate() for clarity.
     */
    public function clone(): self
    {
        return $this->duplicate();
    }

    /**
     * Gets the current parameter counter value.
     */
    public function getCounter(): int
    {
        return $this->counter;
    }

    /**
     * Sets the parameter counter value.
     */
    public function setCounter(int $counter): self
    {
        $this->counter = $counter;
        return $this;
    }

    /**
     * Resets the parameter counter to zero.
     */
    public function resetCounter(): self
    {
        $this->counter = 0;
        return $this;
    }

    /**
     * Checks if a table exists in the database.
     */
    public function tableExists(string $table, ?string $schema = null): bool
    {
        try {
            $driver = $this->pdo()->getAttribute(PDO::ATTR_DRIVER_NAME);

            if ($driver === 'sqlite') {
                $sql = "SELECT name FROM sqlite_master WHERE type='table' AND name = :t";
                $stmt = $this->conn->pdo()->prepare($sql);
                $stmt->execute([':t' => $table]);
                return (bool) $stmt->fetchColumn();
            }

            // MySQL / MariaDB
            $sql = "SELECT 1
              FROM information_schema.tables
             WHERE table_name = :t
               AND table_schema = COALESCE(:s, DATABASE())
             LIMIT 1";
            $stmt = $this->conn->pdo()->prepare($sql);
            $stmt->execute([':t' => $table, ':s' => $schema]);
            return (bool) $stmt->fetchColumn();
        } catch (\Throwable $e) {
            error_log("[qb.exists] Throwable for '" . ($schema ? "$schema.$table" : $table) . "': " . $e->getMessage());
            return false;
        }
    }

    /**
     * Gets column information for a table.
     */
    public function getTableColumns(string $table): array
    {
        $table = $this->resolveTable($table);
        $table = $this->withPrefix($table);

        try {
            $pdo = $this->conn->pdo();
            $driver = $pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);

            if ($driver === 'mysql') {
                $stmt = $pdo->prepare("DESCRIBE `$table`");
                $stmt->execute();
                return array_column($stmt->fetchAll(\PDO::FETCH_ASSOC), 'Field');
            } elseif ($driver === 'pgsql') {
                $stmt = $pdo->prepare(
                    "SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = ?"
                );
                $stmt->execute([$table]);
                return array_column($stmt->fetchAll(\PDO::FETCH_ASSOC), 'column_name');
            } elseif ($driver === 'sqlite') {
                $stmt = $pdo->prepare("PRAGMA table_info($table)");
                $stmt->execute();
                return array_column($stmt->fetchAll(\PDO::FETCH_ASSOC), 'name');
            }

            return [];
        } catch (\PDOException $e) {
            error_log("[qb.getTableColumns] Error fetching columns: {$e->getMessage()}");
            return [];
        }
    }

    /**
     * Gets detailed column information for a table.
     */
    public function getTableSchema(string $table): array
    {
        $table = $this->resolveTable($table);
        $table = $this->withPrefix($table);

        try {
            $pdo = $this->conn->pdo();
            $driver = $pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);

            if ($driver === 'mysql') {
                $stmt = $pdo->prepare("DESCRIBE `$table`");
                $stmt->execute();
                return $stmt->fetchAll(\PDO::FETCH_ASSOC);
            } elseif ($driver === 'pgsql') {
                $stmt = $pdo->prepare(
                    "SELECT 
                    column_name, 
                    data_type, 
                    is_nullable,
                    column_default
                FROM information_schema.columns 
                WHERE table_name = ?
                ORDER BY ordinal_position"
                );
                $stmt->execute([$table]);
                return $stmt->fetchAll(\PDO::FETCH_ASSOC);
            } elseif ($driver === 'sqlite') {
                $stmt = $pdo->prepare("PRAGMA table_info($table)");
                $stmt->execute();
                return $stmt->fetchAll(\PDO::FETCH_ASSOC);
            }

            return [];
        } catch (\PDOException $e) {
            error_log("[qb.getTableSchema] Error fetching schema: {$e->getMessage()}");
            return [];
        }
    }

    /**
     * Converts the query state to an array for serialization.
     */
    public function toArray(): array
    {
        return [
            'parts' => $this->parts,
            'params' => $this->params,
            'counter' => $this->counter,
            'tableMap' => $this->tableMap,
        ];
    }

    /**
     * Restores query state from an array.
     */
    public function fromArray(array $state): self
    {
        $this->parts = $state['parts'] ?? [];
        $this->params = $state['params'] ?? [];
        $this->counter = $state['counter'] ?? 0;
        $this->tableMap = $state['tableMap'] ?? [];
        return $this;
    }

    /**
     * Dumps the query builder state for debugging.
     */
    public function dump(): self
    {
        echo "\n=== Query Builder State ===\n";
        echo "SQL: " . $this->toSql() . "\n";
        echo "Params: " . json_encode($this->params, JSON_PRETTY_PRINT) . "\n";
        echo "Parts: " . json_encode($this->parts, JSON_PRETTY_PRINT) . "\n";
        echo "Counter: " . $this->counter . "\n";
        echo "Table Map: " . json_encode($this->tableMap, JSON_PRETTY_PRINT) . "\n";
        echo "========================\n\n";
        return $this;
    }

    /**
     * Dumps the query builder state and exits.
     */
    public function dd(): void
    {
        $this->dump();
        exit(1);
    }

    /**
     * Returns a debug string representation.
     */
    public function toDebugString(): string
    {
        return sprintf(
            "QueryBuilder {\n  SQL: %s\n  Params: %s\n  From: %s\n}",
            $this->toSql(),
            json_encode($this->params),
            $this->getTable() ?? 'null'
        );
    }

    /**
     * Logs the current query for debugging.
     */
    public function log(string $prefix = '[QueryBuilder]'): self
    {
        error_log("$prefix SQL: " . $this->toSql());
        error_log("$prefix Params: " . json_encode($this->params));
        return $this;
    }

    /**
     * Conditionally executes a callback if condition is true.
     *
     * Example: ->when($isActive, fn($q) => $q->where('status', '=', 'active'))
     */
    public function when(bool $condition, callable $callback, ?callable $default = null): self
    {
        if ($condition) {
            $callback($this);
        } elseif ($default !== null) {
            $default($this);
        }

        return $this;
    }

    /**
     * Conditionally executes a callback if condition is false.
     */
    public function unless(bool $condition, callable $callback, ?callable $default = null): self
    {
        return $this->when(!$condition, $callback, $default);
    }

    /**
     * Taps into the query builder without breaking the chain.
     * Useful for debugging or side effects.
     */
    public function tap(callable $callback): self
    {
        $callback($this);
        return $this;
    }

    /**
     * Storage for custom macros.
     */
    protected static array $macros = [];

    /**
     * Registers a custom macro.
     *
     * Example:
     * QueryBuilder::macro('whereActive', function() {
     *     return $this->where('status', '=', 'active');
     * });
     */
    public static function macro(string $name, callable $callback): void
    {
        static::$macros[$name] = $callback;
    }

    /**
     * Checks if a macro exists.
     */
    public static function hasMacro(string $name): bool
    {
        return isset(static::$macros[$name]);
    }

    /**
     * Calls a registered macro.
     */
    public function __call(string $method, array $parameters): mixed
    {
        if (isset(static::$macros[$method])) {
            $macro = static::$macros[$method];

            if ($macro instanceof \Closure) {
                return $macro->bindTo($this, static::class)(...$parameters);
            }

            return $macro(...$parameters);
        }

        throw new \BadMethodCallException("Method {$method} does not exist.");
    }
}
