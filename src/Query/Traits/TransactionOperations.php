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
     * Counter for savepoint nesting level.
     */
    protected int $transactionLevel = 0;

    /**
     * Callbacks to execute after commit.
     */
    protected array $afterCommitCallbacks = [];

    /**
     * Callbacks to execute after rollback.
     */
    protected array $afterRollbackCallbacks = [];

    /**
     * Transaction statistics.
     */
    protected array $transactionStats = [
        'commits' => 0,
        'rollbacks' => 0,
        'deadlocks' => 0,
    ];

    /**
     * Starts a database transaction.
     */
    public function beginTransaction(): static
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
    public function commit(): static
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
    public function rollback(): static
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

    /**
     * Starts a transaction or creates a savepoint for nested transactions.
     */
    public function beginTransactionNested(): static
    {
        $this->transactionLevel++;

        if ($this->transactionLevel === 1) {
            $this->conn->pdo()->beginTransaction();
            $this->inTransaction = true;
        } else {
            // Create a savepoint for nested transaction
            $savepointName = "savepoint_level_{$this->transactionLevel}";
            $this->conn->pdo()->exec("SAVEPOINT $savepointName");
        }

        return $this;
    }

    /**
     * Commits a transaction or releases a savepoint.
     */
    public function commitNested(): static
    {
        if ($this->transactionLevel === 0) {
            throw new \RuntimeException("No active transaction to commit");
        }

        if ($this->transactionLevel === 1) {
            $this->conn->pdo()->commit();
            $this->inTransaction = false;
        } else {
            // Release savepoint
            $savepointName = "savepoint_level_{$this->transactionLevel}";
            $this->conn->pdo()->exec("RELEASE SAVEPOINT $savepointName");
        }

        $this->transactionLevel--;
        return $this;
    }

    /**
     * Rolls back a transaction or rolls back to a savepoint.
     */
    public function rollbackNested(): static
    {
        if ($this->transactionLevel === 0) {
            throw new \RuntimeException("No active transaction to rollback");
        }

        if ($this->transactionLevel === 1) {
            $this->conn->pdo()->rollBack();
            $this->inTransaction = false;
        } else {
            // Rollback to savepoint
            $savepointName = "savepoint_level_{$this->transactionLevel}";
            $this->conn->pdo()->exec("ROLLBACK TO SAVEPOINT $savepointName");
        }

        $this->transactionLevel--;
        return $this;
    }

    /**
     * Creates a savepoint manually.
     */
    public function savepoint(string $name): static
    {
        if (!$this->inTransaction) {
            throw new \RuntimeException("Cannot create savepoint outside transaction");
        }

        $this->conn->pdo()->exec("SAVEPOINT $name");
        return $this;
    }

    /**
     * Rolls back to a specific savepoint.
     */
    public function rollbackToSavepoint(string $name): static
    {
        if (!$this->inTransaction) {
            throw new \RuntimeException("Cannot rollback to savepoint outside transaction");
        }

        $this->conn->pdo()->exec("ROLLBACK TO SAVEPOINT $name");
        return $this;
    }

    /**
     * Releases a savepoint.
     */
    public function releaseSavepoint(string $name): static
    {
        if (!$this->inTransaction) {
            throw new \RuntimeException("Cannot release savepoint outside transaction");
        }

        $this->conn->pdo()->exec("RELEASE SAVEPOINT $name");
        return $this;
    }

    /**
     * Checks if currently in a transaction.
     */
    public function inTransaction(): bool
    {
        return $this->inTransaction;
    }

    /**
     * Gets the current transaction nesting level.
     */
    public function getTransactionLevel(): int
    {
        return $this->transactionLevel;
    }

    /**
     * Checks if PDO is in a transaction (more reliable check).
     */
    public function isActiveTransaction(): bool
    {
        return $this->conn->pdo()->inTransaction();
    }

    /**
     * Sets the transaction isolation level.
     *
     * @param string $level One of: READ UNCOMMITTED, READ COMMITTED, REPEATABLE READ, SERIALIZABLE
     */
    public function setTransactionIsolation(string $level): static
    {
        $validLevels = [
            'READ UNCOMMITTED',
            'READ COMMITTED',
            'REPEATABLE READ',
            'SERIALIZABLE'
        ];

        $level = strtoupper($level);

        if (!in_array($level, $validLevels, true)) {
            throw new \InvalidArgumentException("Invalid isolation level: $level");
        }

        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        if ($driver === 'mysql') {
            $this->conn->pdo()->exec("SET SESSION TRANSACTION ISOLATION LEVEL $level");
        } elseif ($driver === 'pgsql') {
            // PostgreSQL sets isolation level per transaction
            if ($this->inTransaction) {
                $this->conn->pdo()->exec("SET TRANSACTION ISOLATION LEVEL $level");
            } else {
                $this->conn->pdo()->exec("SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL $level");
            }
        } elseif ($driver === 'sqlite') {
            // SQLite has limited support
            error_log("[TransactionOperations] SQLite has limited isolation level support");
        }

        return $this;
    }

    /**
     * Gets the current transaction isolation level (MySQL).
     */
    public function getTransactionIsolation(): ?string
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        try {
            if ($driver === 'mysql') {
                $stmt = $this->conn->pdo()->query("SELECT @@transaction_isolation");
                return $stmt->fetchColumn();
            } elseif ($driver === 'pgsql') {
                $stmt = $this->conn->pdo()->query("SHOW transaction_isolation");
                return $stmt->fetchColumn();
            }
        } catch (\PDOException $e) {
            error_log("[TransactionOperations] Error getting isolation level: {$e->getMessage()}");
        }

        return null;
    }

    /**
     * Sets READ UNCOMMITTED isolation level.
     */
    public function readUncommitted(): static
    {
        return $this->setTransactionIsolation('READ UNCOMMITTED');
    }

    /**
     * Sets READ COMMITTED isolation level.
     */
    public function readCommitted(): static
    {
        return $this->setTransactionIsolation('READ COMMITTED');
    }

    /**
     * Sets REPEATABLE READ isolation level.
     */
    public function repeatableRead(): static
    {
        return $this->setTransactionIsolation('REPEATABLE READ');
    }

    /**
     * Sets SERIALIZABLE isolation level.
     */
    public function serializable(): static
    {
        return $this->setTransactionIsolation('SERIALIZABLE');
    }

    /**
     * Executes a callback within a transaction with retry logic.
     *
     * @param callable $callback Callback to execute
     * @param int $attempts Maximum number of attempts
     * @param int $sleep Milliseconds to sleep between attempts
     */
    public function transactionWithRetry(callable $callback, int $attempts = 3, int $sleep = 100): mixed
    {
        $attempt = 0;
        $lastException = null;

        while ($attempt < $attempts) {
            $attempt++;

            try {
                return $this->transaction($callback);
            } catch (\PDOException $e) {
                $lastException = $e;

                // Check if error is retryable (deadlock, lock timeout, etc.)
                $errorCode = $e->getCode();
                $errorInfo = $e->errorInfo ?? [];

                $isDeadlock = in_array($errorCode, ['40001', '40P01']) || // PostgreSQL
                    (isset($errorInfo[1]) && $errorInfo[1] === 1213); // MySQL deadlock

                $isLockTimeout = in_array($errorCode, ['40001']) ||
                    (isset($errorInfo[1]) && $errorInfo[1] === 1205); // MySQL lock timeout

                if (!$isDeadlock && !$isLockTimeout) {
                    // Not a retryable error
                    throw $e;
                }

                if ($attempt < $attempts) {
                    error_log("[TransactionOperations] Transaction failed (attempt $attempt/$attempts): {$e->getMessage()}. Retrying...");
                    usleep($sleep * 1000);
                }
            }
        }

        throw new \RuntimeException(
            "Transaction failed after $attempts attempts",
            0,
            $lastException
        );
    }

    /**
     * Executes a callback within a nested transaction.
     */
    public function transactionNested(callable $callback): mixed
    {
        $this->beginTransactionNested();

        try {
            $result = $callback($this);
            $this->commitNested();
            return $result;
        } catch (\Throwable $e) {
            $this->rollbackNested();
            throw $e;
        }
    }

    /**
     * Safely executes a transaction and ensures cleanup.
     * Catches all throwables (including Error).
     */
    public function safeTransaction(callable $callback): mixed
    {
        $this->beginTransaction();

        try {
            $result = $callback($this);
            $this->commit();
            return $result;
        } catch (\Throwable $e) {
            try {
                $this->rollback();
            } catch (\Throwable $rollbackException) {
                error_log("[TransactionOperations] Rollback failed: {$rollbackException->getMessage()}");
            }
            throw $e;
        }
    }

    /**
     * Registers a callback to execute after successful commit.
     */
    public function afterCommit(callable $callback): static
    {
        $this->afterCommitCallbacks[] = $callback;
        return $this;
    }

    /**
     * Registers a callback to execute after rollback.
     */
    public function afterRollback(callable $callback): static
    {
        $this->afterRollbackCallbacks[] = $callback;
        return $this;
    }

    /**
     * Commits the transaction and executes after-commit callbacks.
     */
    public function commitWithCallbacks(): static
    {
        if ($this->inTransaction) {
            $this->conn->pdo()->commit();
            $this->inTransaction = false;

            // Execute callbacks
            foreach ($this->afterCommitCallbacks as $callback) {
                try {
                    $callback($this);
                } catch (\Throwable $e) {
                    error_log("[TransactionOperations] After-commit callback failed: {$e->getMessage()}");
                }
            }

            $this->afterCommitCallbacks = [];
            $this->afterRollbackCallbacks = [];
        }

        return $this;
    }

    /**
     * Rolls back the transaction and executes after-rollback callbacks.
     */
    public function rollbackWithCallbacks(): static
    {
        if ($this->inTransaction) {
            $this->conn->pdo()->rollBack();
            $this->inTransaction = false;

            // Execute callbacks
            foreach ($this->afterRollbackCallbacks as $callback) {
                try {
                    $callback($this);
                } catch (\Throwable $e) {
                    error_log("[TransactionOperations] After-rollback callback failed: {$e->getMessage()}");
                }
            }

            $this->afterCommitCallbacks = [];
            $this->afterRollbackCallbacks = [];
        }

        return $this;
    }

    /**
     * Clears all registered callbacks.
     */
    public function clearCallbacks(): static
    {
        $this->afterCommitCallbacks = [];
        $this->afterRollbackCallbacks = [];
        return $this;
    }

    /**
     * Sets constraints to be deferred until transaction commit (PostgreSQL).
     */
    public function deferConstraints(): static
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        if ($driver === 'pgsql') {
            if (!$this->inTransaction) {
                throw new \RuntimeException("Cannot defer constraints outside transaction");
            }

            $this->conn->pdo()->exec("SET CONSTRAINTS ALL DEFERRED");
        } else {
            error_log("[TransactionOperations] Deferred constraints only supported in PostgreSQL");
        }

        return $this;
    }

    /**
     * Sets constraints to be checked immediately (PostgreSQL).
     */
    public function immediateConstraints(): static
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        if ($driver === 'pgsql') {
            $this->conn->pdo()->exec("SET CONSTRAINTS ALL IMMEDIATE");
        }

        return $this;
    }

    /**
     * Acquires an advisory lock (PostgreSQL/MySQL).
     *
     * @param string|int $key Lock key/name
     * @param int $timeout Timeout in seconds (0 = wait indefinitely)
     */
    public function getLock(string|int $key, int $timeout = 0): bool
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        try {
            if ($driver === 'mysql') {
                $stmt = $this->conn->pdo()->prepare("SELECT GET_LOCK(?, ?)");
                $stmt->execute([$key, $timeout]);
                return (bool) $stmt->fetchColumn();
            } elseif ($driver === 'pgsql') {
                $lockId = is_numeric($key) ? (int)$key : crc32($key);

                if ($timeout > 0) {
                    // PostgreSQL doesn't have built-in timeout, implement with retry
                    $start = time();
                    while (time() - $start < $timeout) {
                        $stmt = $this->conn->pdo()->prepare("SELECT pg_try_advisory_lock(?)");
                        $stmt->execute([$lockId]);
                        if ($stmt->fetchColumn()) {
                            return true;
                        }
                        usleep(100000); // 100ms
                    }
                    return false;
                } else {
                    $stmt = $this->conn->pdo()->prepare("SELECT pg_advisory_lock(?)");
                    $stmt->execute([$lockId]);
                    return true;
                }
            }
        } catch (\PDOException $e) {
            error_log("[TransactionOperations] Get lock failed: {$e->getMessage()}");
            return false;
        }

        return false;
    }

    /**
     * Releases an advisory lock.
     */
    public function releaseLock(string|int $key): bool
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        try {
            if ($driver === 'mysql') {
                $stmt = $this->conn->pdo()->prepare("SELECT RELEASE_LOCK(?)");
                $stmt->execute([$key]);
                return (bool) $stmt->fetchColumn();
            } elseif ($driver === 'pgsql') {
                $lockId = is_numeric($key) ? (int)$key : crc32($key);
                $stmt = $this->conn->pdo()->prepare("SELECT pg_advisory_unlock(?)");
                $stmt->execute([$lockId]);
                return (bool) $stmt->fetchColumn();
            }
        } catch (\PDOException $e) {
            error_log("[TransactionOperations] Release lock failed: {$e->getMessage()}");
            return false;
        }

        return false;
    }

    /**
     * Executes a callback with an exclusive lock.
     */
    public function withLock(string|int $key, callable $callback, int $timeout = 10): mixed
    {
        if (!$this->getLock($key, $timeout)) {
            throw new \RuntimeException("Failed to acquire lock: $key");
        }

        try {
            return $callback($this);
        } finally {
            $this->releaseLock($key);
        }
    }

    /**
     * Tracks a commit.
     */
    protected function trackCommit(): void
    {
        $this->transactionStats['commits']++;
    }

    /**
     * Tracks a rollback.
     */
    protected function trackRollback(): void
    {
        $this->transactionStats['rollbacks']++;
    }

    /**
     * Tracks a deadlock.
     */
    protected function trackDeadlock(): void
    {
        $this->transactionStats['deadlocks']++;
    }

    /**
     * Gets transaction statistics.
     */
    public function getTransactionStats(): array
    {
        return $this->transactionStats;
    }

    /**
     * Resets transaction statistics.
     */
    public function resetTransactionStats(): static
    {
        $this->transactionStats = [
            'commits' => 0,
            'rollbacks' => 0,
            'deadlocks' => 0,
        ];
        return $this;
    }

    /**
     * Forces a rollback if currently in transaction (cleanup method).
     */
    public function forceRollback(): static
    {
        if ($this->inTransaction) {
            try {
                $this->conn->pdo()->rollBack();
            } catch (\PDOException $e) {
                error_log("[TransactionOperations] Force rollback failed: {$e->getMessage()}");
            }
            $this->inTransaction = false;
            $this->transactionLevel = 0;
        }
        return $this;
    }

    /**
     * Ensures no transaction is active (for cleanup).
     */
    public function ensureNoTransaction(): static
    {
        if ($this->isActiveTransaction()) {
            error_log("[TransactionOperations] Warning: Uncommitted transaction detected, forcing rollback");
            $this->forceRollback();
        }
        return $this;
    }

    /**
     * Begins a transaction with a specific isolation level.
     */
    public function beginTransactionWith(string $isolationLevel): static
    {
        $this->setTransactionIsolation($isolationLevel);
        return $this->beginTransaction();
    }

    /**
     * Begins a read-only transaction (PostgreSQL/MySQL 5.6+).
     */
    public function beginReadOnlyTransaction(): static
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        if ($driver === 'mysql') {
            $this->conn->pdo()->exec("START TRANSACTION READ ONLY");
            $this->inTransaction = true;
        } elseif ($driver === 'pgsql') {
            $this->conn->pdo()->beginTransaction();
            $this->conn->pdo()->exec("SET TRANSACTION READ ONLY");
            $this->inTransaction = true;
        } else {
            throw new \RuntimeException("Read-only transactions not supported for driver: $driver");
        }

        return $this;
    }

    /**
     * Begins a read-write transaction (explicit).
     */
    public function beginReadWriteTransaction(): static
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        if ($driver === 'mysql') {
            $this->conn->pdo()->exec("START TRANSACTION READ WRITE");
            $this->inTransaction = true;
        } elseif ($driver === 'pgsql') {
            $this->conn->pdo()->beginTransaction();
            $this->conn->pdo()->exec("SET TRANSACTION READ WRITE");
            $this->inTransaction = true;
        } else {
            return $this->beginTransaction();
        }

        return $this;
    }
}
