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
        $this->reset();

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
        $this->reset();

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
        $this->reset();
        return $row ?: null;
    }

    /**
     * Fetches a single column value.
     */
    public function value(string $column): mixed
    {
        $originalSelect = $this->parts['select'];
        $this->parts['select'] = $column;

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);

        $result = $stmt->fetchColumn();
        $this->parts['select'] = $originalSelect;

        return $result;
    }

    /**
     * Fetches values from a single column as an array.
     */
    public function pluck(string $column, ?string $key = null): array
    {
        $originalSelect = $this->parts['select'];
        $this->parts['select'] = $key ? "$key, $column" : $column;

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);

        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->parts['select'] = $originalSelect;

        if ($key) {
            $result = [];
            foreach ($rows as $row) {
                $result[$row[$key]] = $row[$column];
            }
            return $result;
        }

        return array_column($rows, $column);
    }
}
