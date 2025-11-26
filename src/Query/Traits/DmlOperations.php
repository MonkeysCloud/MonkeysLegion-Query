<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

use MonkeysLegion\Entity\Utils\Uuid;

/**
 * Provides Data Manipulation Language (DML) operations for the query builder.
 * 
 * Implements methods for INSERT, UPDATE, DELETE operations and 
 * custom SQL execution.
 * 
 * @property array $parts Query parts storage including 'custom' 
 * @property array $params Query parameters
 * @property \MonkeysLegion\Database\Contracts\ConnectionInterface $conn Database connection
 */
trait DmlOperations
{
    /**
     * Sets a custom SQL statement.
     */
    public function custom(string $sql, array $params = []): static
    {
        $this->parts['custom'] = $sql;
        $this->params = $params;
        return $this;
    }

    /**
     * Inserts a new row into the specified table.
     *
     * @param string $table The table name.
     * @param array<string, mixed> $data The data to insert, as an associative array.
     *
     * @return int|string The ID of the inserted row (int for auto-increment, string for UUID).
     *
     * @throws \InvalidArgumentException If $data is empty.
     * @throws \RuntimeException If the insert fails.
     * @throws \PDOException If PDO throws during prepare/execute.
     */
    public function insert(string $table, array $data): int|string
    {
        if ($data === []) {
            throw new \InvalidArgumentException("Cannot insert empty data");
        }

        $columns = array_keys($data);

        // Build "col1, col2, col3"
        $cols = implode(', ', $columns);

        // Build ":col1, :col2, :col3"
        $placeholders = implode(
            ', ',
            array_map(fn(string $k) => ':' . $k, $columns)
        );

        $sql = "INSERT INTO {$table} ({$cols}) VALUES ({$placeholders})";

        $stmt = $this->conn->pdo()->prepare($sql);

        // Map ":col" => value
        $bound = array_combine(
            array_map(fn(string $k) => ':' . $k, $columns),
            $data
        );

        if ($bound === false) {
            throw new \RuntimeException('Failed to build parameter bindings for insert()');
        }

        if (!$stmt->execute($bound)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Insert failed: {$state}/{$code} – {$msg}");
        }

        // Let PDO/driver decide the type; we normalize to int|string.
        $id = $this->conn->pdo()->lastInsertId();

        $this->reset();

        if ($id === '' || $id === '0') {
            // No meaningful lastInsertId (e.g. no PK / some drivers)
            // You *could* return 0 or throw; for now, return 0 as int.
            return 0;
        }

        // If it's a valid UUID, return as string; otherwise cast to int.
        if (Uuid::isValid($id)) {
            return $id;
        }

        return (int) $id;
    }

    /**
     * Inserts multiple rows into the specified table.
     */
    public function insertBatch(string $table, array $rows): int
    {
        if (empty($rows)) {
            return 0;
        }

        $cols = array_keys(reset($rows));
        $colsStr = implode(', ', $cols);

        $values = [];
        $params = [];
        $counter = 0;

        foreach ($rows as $row) {
            $placeholders = [];
            foreach ($cols as $col) {
                $key = ":p{$counter}";
                $placeholders[] = $key;
                $params[$key] = $row[$col] ?? null;
                $counter++;
            }
            $values[] = '(' . implode(', ', $placeholders) . ')';
        }

        $sql = "INSERT INTO $table ($colsStr) VALUES " . implode(', ', $values);
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Batch insert failed: $state/$code – $msg");
        }

        $count = $stmt->rowCount();
        $this->reset();
        return $count;
    }

    /**
     * Updates rows in the specified table.
     */
    public function update(string $table, array $data): static
    {
        if (empty($data)) {
            throw new \InvalidArgumentException("Cannot update with empty data");
        }

        $sets = implode(', ', array_map(fn(string $k) => "$k = :set_$k", array_keys($data)));
        $this->parts['custom'] = "UPDATE $table SET $sets";

        // Prefix params to avoid collision with WHERE params
        foreach ($data as $key => $value) {
            $this->params[":set_$key"] = $value;
        }

        return $this;
    }

    /**
     * Deletes rows from the specified table.
     */
    public function delete(string $table): static
    {
        $this->parts['custom'] = "DELETE FROM $table";
        return $this;
    }

    /**
     * Executes the query and returns the number of affected rows.
     */
    public function execute(): int
    {
        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Query execution failed: $state/$code – $msg");
        }

        $count = $stmt->rowCount();
        $this->reset();
        return $count;
    }

    /**
     * Executes a raw SQL statement (non-select).
     */
    public function executeRaw(string $sql): int
    {
        return $this->pdo()->exec($sql);
    }
}
