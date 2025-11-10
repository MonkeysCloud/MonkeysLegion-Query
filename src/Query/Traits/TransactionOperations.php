<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

/**
 * Provides database transaction operations for the query builder.
 * 
 * Implements methods for transaction control with begin, commit,
 * rollback and callback-based transaction wrapping.
 * 
 * @property \MonkeysLegion\Database\Contracts\ConnectionInterface $conn Database connection
 * @property bool $inTransaction Flag indicating if a transaction is active
 */
trait TransactionOperations
{
    /**
     * Starts a database transaction.
     */
    public function beginTransaction(): self
    {
        if (!$this->inTransaction) {
            $this->conn->pdo()->beginTransaction();
            $this->inTransaction = true;
        }
        return $this;
    }

    /**
     * Commits the current transaction.
     */
    public function commit(): self
    {
        if ($this->inTransaction) {
            $this->conn->pdo()->commit();
            $this->inTransaction = false;
        }
        return $this;
    }

    /**
     * Rolls back the current transaction.
     */
    public function rollback(): self
    {
        if ($this->inTransaction) {
            $this->conn->pdo()->rollBack();
            $this->inTransaction = false;
        }
        return $this;
    }

    /**
     * Executes a callback within a transaction.
     */
    public function transaction(callable $callback): mixed
    {
        $this->beginTransaction();

        try {
            $result = $callback($this);
            $this->commit();
            return $result;
        } catch (\Exception $e) {
            $this->rollback();
            throw $e;
        }
    }
}
