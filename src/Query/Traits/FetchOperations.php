<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

use MonkeysLegion\Entity\Hydrator;
use PDO;

/**
 * Provides data fetching operations for the query builder.
 *
 * Implements methods for retrieving and transforming query results
 * into various formats (objects, arrays, single values).
 *
 * @property array $parts Query parts storage
 * @property array $params Query parameters
 * @property \MonkeysLegion\Database\Contracts\ConnectionInterface $conn Database connection
 */
trait FetchOperations
{
    /**
     * Fetches all results as an array of objects.
     */
    public function fetchAll(string $class = 'stdClass'): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);

        if ($class !== 'stdClass' && class_exists($class)) {
            return array_map(
                fn(array $r) => Hydrator::hydrate($class, $r),
                $rows
            );
        }

        return $rows;
    }

    /**
     * Fetches a single row as an object.
     */
    public function fetch(string $class = 'stdClass'): object|false
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_ASSOC);


        if (!$row) {
            return false;
        }

        if ($class !== 'stdClass' && class_exists($class)) {
            return Hydrator::hydrate($class, $row);
        }

        return (object) $row;
    }

    /**
     * Fetches a single row as an associative array.
     */
    public function fetchOne(string $sql, array $params = []): array|null
    {
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_ASSOC);

        return $row ?: null;
    }

    /**
     * Gets a single column value from the first row.
     *
     * @throws \Throwable
     */
    public function value(string $column): mixed
    {
        // Work on a cloned builder so we don't mutate the original instance.
        $qb = $this->duplicate();


        try {
            $qb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log("[qb.value] preflightResolveTables FAILED: {$e->getMessage()}");
            throw $e;
        }

        // Force a single column + single row
        $qb->parts['select'] = $column;
        $qb->parts['limit']  = 1;

        try {
            $sql  = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("Value query failed: {$state}/{$code} – {$msg}");
            }

            $row = $stmt->fetch(\PDO::FETCH_ASSOC);

            return $row ? reset($row) : null;
        } catch (\PDOException $e) {
            error_log("[qb.value] PDOException: {$e->getMessage()}");
            throw $e;
        }
    }


    /**
     * Gets all values from a single column.
     *
     * @throws \Throwable
     */
    public function pluck(string $column, ?string $key = null): array
    {
        // Work on a cloned query builder so we don't mutate the original.
        $qb = $this->duplicate();


        try {
            $qb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log("[qb.pluck] preflightResolveTables FAILED: {$e->getMessage()}");
            throw $e;
        }

        // Select the value column and optionally the key column
        $selectColumns = $key ? "$key, $column" : $column;
        $qb->parts['select'] = $selectColumns;

        try {
            $sql = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("Pluck query failed: $state/$code – $msg");
            }

            $results = [];

            if ($key) {
                // Use the key column as array keys
                while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                    $keyValue    = reset($row);   // first column = key
                    $valueColumn = next($row);    // second column = value
                    $results[$keyValue] = $valueColumn;
                }
            } else {
                // Simple array of values
                while ($row = $stmt->fetch(\PDO::FETCH_ASSOC)) {
                    $results[] = reset($row);     // only column = value
                }
            }

            return $results;
        } catch (\PDOException $e) {
            error_log("[qb.pluck] PDOException: {$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Fetches all results as associative arrays.
     */
    public function fetchAllAssoc(): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Fetches all results as numeric arrays.
     */
    public function fetchAllNumeric(): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        return $stmt->fetchAll(PDO::FETCH_NUM);
    }

    /**
     * Fetches all results as objects (stdClass).
     */
    public function fetchAllObjects(): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        return $stmt->fetchAll(PDO::FETCH_OBJ);
    }

    /**
     * Gets the first row from the query.
     *
     * @return array|null
     * @throws \Throwable
     */
    public function first(): ?array
    {
        // Work on a cloned builder so we don't mutate the original chain.
        $qb = $this->duplicate();


        try {
            // Keep the same pipeline as other read operations.
            $qb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log("[qb.first] preflightResolveTables FAILED: {$e->getMessage()}");
            throw $e;
        }

        // Limit to a single row
        $qb->parts['limit'] = 1;

        try {
            $sql  = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("First query failed: {$state}/{$code} – {$msg}");
            }

            $row = $stmt->fetch(\PDO::FETCH_ASSOC);

            return $row ?: null;
        } catch (\PDOException $e) {
            error_log("[qb.first] PDOException: {$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Fetches first row as an object of specified class or null.
     * @throws \Throwable
     */
    public function firstAs(string $class): ?object
    {
        $row = $this->first();

        if (!$row) {
            return null;
        }

        if (class_exists($class)) {
            return Hydrator::hydrate($class, $row);
        }

        return (object) $row;
    }

    /**
     * Fetches first row or throws exception if not found.
     * @throws \Throwable
     */
    public function firstOrFail(): array
    {
        $row = $this->first();

        if ($row === null) {
            throw new \RuntimeException("No results found for query");
        }

        return $row;
    }

    /**
     * Fetches a single associative array (alias for first).
     */
    public function fetchAssoc(): ?array
    {
        return $this->first();
    }

    /**
     * Fetches a single numeric array.
     */
    public function fetchNumeric(): ?array
    {
        $qb = $this->duplicate();
        $qb->parts['limit'] = 1;


        $sql = $qb->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($qb->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_NUM);
        return $row ?: null;
    }

    /**
     * Fetches a single object (stdClass).
     */
    public function fetchObject(): ?object
    {
        $qb = $this->duplicate();
        $qb->parts['limit'] = 1;


        $sql = $qb->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($qb->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_OBJ);
        return $row ?: null;
    }

    /**
     * Fetches a single column value from the first row.
     */
    public function fetchColumn(int $columnNumber = 0): mixed
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        return $stmt->fetchColumn($columnNumber);
    }

    /**
     * Fetches all values from a single column.
     */
    public function fetchAllColumn(int $columnNumber = 0): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        return $stmt->fetchAll(PDO::FETCH_COLUMN, $columnNumber);
    }

    /**
     * Alias for pluck without key (simple column array).
     */
    public function lists(string $column): array
    {
        return $this->pluck($column);
    }

    /**
     * Fetches results as key-value pairs.
     *
     * @param string $key Column to use as array key
     * @param string $value Column to use as array value
     * @throws \Throwable
     */
    public function fetchPairs(string $key, string $value): array
    {
        return $this->pluck($value, $key);
    }

    /**
     * Fetches results grouped by a key column.
     *
     * @param string $key Column to group by
     */
    public function fetchGrouped(string $key): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $results = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $groupKey = $row[$key];
            unset($row[$key]);
            $results[$groupKey][] = $row;
        }

        return $results;
    }

    /**
     * Fetches results indexed by a specific column.
     */
    public function fetchIndexed(string $key, string $class = 'stdClass'): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $results = [];
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            $indexKey = $row[$key];

            if ($class !== 'stdClass' && class_exists($class)) {
                $results[$indexKey] = Hydrator::hydrate($class, $row);
            } else {
                $results[$indexKey] = $row;
            }
        }

        return $results;
    }

    /**
     * Processes results in chunks to reduce memory usage.
     *
     * @param int $size Number of records per chunk
     * @param callable $callback Function to process each chunk
     */
    public function chunk(int $size, callable $callback): void
    {
        $page = 0;

        do {
            $qb = $this->duplicate();
            $qb->limit($size)->offset($page * $size);


            $sql = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("Query failed: $state/$code – $msg");
            }

            $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (empty($results)) {
                break;
            }

            // Call the callback with the chunk
            if ($callback($results, $page) === false) {
                break;
            }

            $page++;
        } while (count($results) === $size);
    }

    /**
     * Iterates over results one at a time using a generator.
     * Memory efficient for large datasets.
     */
    public function cursor(): \Generator
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            yield $row;
        }
    }

    /**
     * Iterates over results as objects using a generator.
     */
    public function cursorAs(string $class): \Generator
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            if (class_exists($class)) {
                yield Hydrator::hydrate($class, $row);
            } else {
                yield (object) $row;
            }
        }
    }

    /**
     * Processes each result individually with a callback.
     * More memory efficient than fetchAll for large datasets.
     */
    public function each(callable $callback): void
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $index = 0;
        while ($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
            if ($callback($row, $index++) === false) {
                break;
            }
        }
    }

    /**
     * Lazy loads results (returns a generator that yields chunks).
     */
    public function lazy(int $chunkSize = 1000): \Generator
    {
        $page = 0;

        do {
            $qb = $this->duplicate();
            $qb->limit($chunkSize)->offset($page * $chunkSize);


            $sql = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("Query failed: $state/$code – $msg");
            }

            $results = $stmt->fetchAll(PDO::FETCH_ASSOC);

            if (empty($results)) {
                break;
            }

            foreach ($results as $result) {
                yield $result;
            }

            $page++;
        } while (count($results) === $chunkSize);
    }

    /**
     * Paginate results.
     *
     * @param int $page Current page number (1-indexed)
     * @param int $perPage Number of items per page
     * @return array{
     *     data: array,
     *     total: int,
     *     page: int,
     *     perPage: int,
     *     lastPage: int,
     *     from: int,
     *     to: int
     * }
     * @throws \Throwable
     */
    public function paginate(int $page = 1, int $perPage = 15): array
    {
        $page     = max(1, $page);
        $perPage  = max(1, $perPage);

        // Total count
        $countQb = $this->duplicate();
        $total   = $countQb->count();

        $lastPage = (int) ceil($total / $perPage);
        $offset   = ($page - 1) * $perPage;

        // Fetch page data
        $dataQb = $this->duplicate();
        $dataQb->limit($perPage)->offset($offset);


        $sql  = $dataQb->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($dataQb->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Pagination query failed: {$state}/{$code} – {$msg}");
        }

        $data = $stmt->fetchAll(\PDO::FETCH_ASSOC);

        return [
            'data'     => $data,
            'total'    => $total,
            'page'     => $page,
            'perPage'  => $perPage,
            'lastPage' => $lastPage,
            'from'     => $offset + 1,
            'to'       => min($offset + $perPage, $total),
        ];
    }

    /**
     * Alias for limit+offset, similar to Laravel "forPage".
     */
    public function forPage(int $page, int $perPage = 15): static
    {
        $page = max(1, $page);
        return $this->limit($perPage)->offset(($page - 1) * $perPage);
    }

    /**
     * Simple pagination (doesn't count total).
     * Faster for large datasets.
     */
    public function simplePaginate(int $page = 1, int $perPage = 15): array
    {
        $page = max(1, $page);
        $offset = ($page - 1) * $perPage;

        $qb = $this->duplicate();
        $qb->limit($perPage + 1)->offset($offset);


        $sql = $qb->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($qb->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $data = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $hasMore = count($data) > $perPage;

        if ($hasMore) {
            array_pop($data);
        }

        return [
            'data' => $data,
            'page' => $page,
            'perPage' => $perPage,
            'hasMore' => $hasMore,
        ];
    }

    /**
     * Finds a record by ID.
     * Assumes 'id' column by default.
     */
    public function find(mixed $id, string $column = 'id'): ?array
    {
        $qb = $this->duplicate();

        $qb->where($column, '=', $id)->limit(1);

        $sql = $qb->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($qb->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row ?: null;
    }

    /**
     * Finds a record by ID or throws exception.
     */
    public function findOrFail(mixed $id, string $column = 'id'): array
    {
        $row = $this->find($id, $column);

        if ($row === null) {
            throw new \RuntimeException("Record with $column = $id not found");
        }

        return $row;
    }

    /**
     * Finds multiple records by IDs.
     */
    public function findMany(array $ids, string $column = 'id'): array
    {
        if (empty($ids)) {
            return [];
        }

        $qb = $this->duplicate();

        $qb->whereIn($column, $ids);

        $sql = $qb->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($qb->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Maps results through a callback function.
     */
    public function map(callable $callback): array
    {
        $results = $this->fetchAllAssoc();
        return array_map($callback, $results);
    }

    /**
     * Filters results through a callback function.
     */
    public function filter(callable $callback): array
    {
        $results = $this->fetchAllAssoc();
        return array_filter($results, $callback);
    }

    /**
     * Reduces results to a single value.
     */
    public function reduce(callable $callback, mixed $initial = null): mixed
    {
        $results = $this->fetchAllAssoc();
        return array_reduce($results, $callback, $initial);
    }

    /**
     * Returns first result or a default value.
     */
    public function firstOr(mixed $default): mixed
    {
        $result = $this->first();
        return $result ?? $default;
    }

    /**
     * Returns first result or executes callback.
     */
    public function firstOrCallback(callable $callback): mixed
    {
        $result = $this->first();
        return $result ?? $callback();
    }

    /**
     * Fetches and hydrates a collection of entities.
     */
    public function fetchCollection(string $class): array
    {
        return $this->fetchAll($class);
    }

    /**
     * Fetches a single entity or null.
     */
    public function fetchEntity(string $class): ?object
    {
        $row = $this->first();

        if (!$row) {
            return null;
        }

        return class_exists($class) ? Hydrator::hydrate($class, $row) : (object) $row;
    }

    /**
     * Hydrates results into a specific class with custom hydrator.
     */
    public function hydrateWith(string $class, callable $hydrator): array
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        return array_map(fn($row) => $hydrator($class, $row), $rows);
    }

    /**
     * Executes a raw SQL query and returns results.
     */
    public function raw(string $sql, array $params = []): array
    {
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        return $stmt->fetchAll(PDO::FETCH_ASSOC);
    }

    /**
     * Executes a raw SQL query and returns first row.
     */
    public function rawOne(string $sql, array $params = []): ?array
    {
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query failed: $state/$code – $msg");
        }

        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        return $row ?: null;
    }
}
