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
    public function custom(string $sql, array $params = []): self
    {
        $this->parts['custom'] = $sql;
        $this->params = $params;
        return $this;
    }

    /**
     * Inserts a new row into the specified table.
     */
    public function insert(string $table, array $data): int | string
    {
        if (empty($data)) {
            throw new \InvalidArgumentException("Cannot insert empty data");
        }

        $cols = implode(', ', array_keys($data));
        $phs = implode(', ', array_map(fn(string $k) => ":$k", array_keys($data)));
        $sql = "INSERT INTO $table ($cols) VALUES ($phs)";

        $stmt = $this->conn->pdo()->prepare($sql);
        $bound = array_combine(
            array_map(fn(string $k) => ":$k", array_keys($data)),
            $data
        );

        if (!$stmt->execute($bound)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Insert failed: $state/$code – $msg");
        }

        $id = $this->conn->pdo()->lastInsertId();
        $this->reset();
        if (Uuid::isValid($id)) {
            return (string) $id;
        } else {
            return (int) $id;
        }
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
    public function update(string $table, array $data): self
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
    public function delete(string $table): self
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
