<?php
declare(strict_types=1);

namespace Tests\Support;

use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Database\Contracts\ConnectionManagerInterface;
use MonkeysLegion\Database\Support\ConnectionPoolStats;

/**
 * Concrete test double for ConnectionManagerInterface.
 * Required because PHP 8.4 property hooks on ConnectionInterface
 * prevent PHPUnit from mocking the interface chain.
 */
final class TestConnectionManager implements ConnectionManagerInterface
{
    public function __construct(
        private readonly ConnectionInterface $connection,
        private readonly string $defaultName = 'default',
    ) {}

    public function connection(?string $name = null): ConnectionInterface
    {
        return $this->connection;
    }

    public function read(?string $name = null): ConnectionInterface
    {
        return $this->connection;
    }

    public function write(?string $name = null): ConnectionInterface
    {
        return $this->connection;
    }

    public function disconnect(?string $name = null): void {}
    public function disconnectAll(): void {}
    public function purge(?string $name = null): void {}

    public function getDefaultConnectionName(): string
    {
        return $this->defaultName;
    }

    public function setDefaultConnection(string $name): void {}

    public function stats(): array
    {
        return [];
    }
}
