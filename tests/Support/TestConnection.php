<?php
declare(strict_types=1);

namespace Tests\Support;

use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Database\Types\DatabaseDriver;
use MonkeysLegion\Database\Types\IsolationLevel;
use PDO;
use PDOStatement;

/**
 * Concrete test double for ConnectionInterface.
 * Required because PHP 8.4 property hooks on interfaces are not mockable by PHPUnit.
 */
final class TestConnection implements ConnectionInterface
{
    private int $queryCountValue = 0;

    public int $queryCount {
        get => $this->queryCountValue;
    }

    public float $uptimeSeconds {
        get => 0.0;
    }

    public function __construct(
        private readonly PDO $pdoInstance,
        private readonly DatabaseDriver $driver = DatabaseDriver::SQLite,
        private readonly string $name = 'test',
    ) {}

    public function resetQueryCount(): void
    {
        $this->queryCountValue = 0;
    }

    public function connect(): void {}
    public function disconnect(): void {}
    public function reconnect(): void {}

    public function isConnected(): bool
    {
        return true;
    }

    public function isAlive(): bool
    {
        return true;
    }

    public function pdo(): PDO
    {
        return $this->pdoInstance;
    }

    public function getDriver(): DatabaseDriver
    {
        return $this->driver;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function beginTransaction(?IsolationLevel $isolation = null): void
    {
        $this->pdoInstance->beginTransaction();
    }

    public function commit(): void
    {
        $this->pdoInstance->commit();
    }

    public function rollBack(): void
    {
        $this->pdoInstance->rollBack();
    }

    public function inTransaction(): bool
    {
        return $this->pdoInstance->inTransaction();
    }

    public function transaction(callable $callback, ?IsolationLevel $isolation = null): mixed
    {
        $this->beginTransaction($isolation);
        try {
            $result = $callback($this);
            $this->commit();
            return $result;
        } catch (\Throwable $e) {
            $this->rollBack();
            throw $e;
        }
    }

    public function execute(string $sql, array $params = []): int
    {
        $stmt = $this->pdoInstance->prepare($sql);
        $stmt->execute($params);
        $this->queryCountValue++;
        return $stmt->rowCount();
    }

    public function query(string $sql, array $params = []): PDOStatement
    {
        $stmt = $this->pdoInstance->prepare($sql);
        $stmt->execute($params);
        $this->queryCountValue++;
        return $stmt;
    }

    public function lastInsertId(?string $name = null): string|false
    {
        return $this->pdoInstance->lastInsertId($name);
    }
}
