<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Database\Contracts\ConnectionInterface;

/**
 * Tests for performance optimization features added to QueryBuilder:
 * P2-A (PSR-16 cache), P2-B (keyset pagination), P2-D (fresh() tableMap),
 * P2-G (statement cache), P3-A (read/write splitting), P3-B (preflight cache),
 * P3-D (query profiling), P2-F (PSR-3 logger).
 */
class PerformanceOptimizationsTest extends TestCase
{
    private QueryBuilder $qb;
    private \PDO $pdo;

    protected function setUp(): void
    {
        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, score INTEGER DEFAULT 0)');
        $this->pdo->exec("INSERT INTO users (name, email, score) VALUES ('Alice', 'alice@test.com', 10)");
        $this->pdo->exec("INSERT INTO users (name, email, score) VALUES ('Bob', 'bob@test.com', 20)");
        $this->pdo->exec("INSERT INTO users (name, email, score) VALUES ('Charlie', 'charlie@test.com', 30)");
        $this->pdo->exec("INSERT INTO users (name, email, score) VALUES ('Diana', 'diana@test.com', 40)");
        $this->pdo->exec("INSERT INTO users (name, email, score) VALUES ('Eve', 'eve@test.com', 50)");

        $conn = $this->createConnection($this->pdo);
        $this->qb = new QueryBuilder($conn);
    }

    private function createConnection(\PDO $pdo): ConnectionInterface
    {
        return new class($pdo) implements ConnectionInterface {
            public function __construct(private PDO $pdo) {}
            public function pdo(): PDO { return $this->pdo; }
            public function connect(): void {}
            public function disconnect(): void {}
            public function isConnected(): bool { return true; }
            public function getDsn(): string { return 'sqlite::memory:'; }
            public function isAlive(): bool { return true; }
        };
    }

    // ── P2-B: Keyset pagination ──

    public function testPaginateAfterFirstPage(): void
    {
        $result = $this->qb->select()->from('users')
            ->paginateAfter('id', null, 2);

        $this->assertCount(2, $result['data']);
        $this->assertTrue($result['hasMore']);
        $this->assertEquals(2, $result['nextCursor']);
        $this->assertEquals('Alice', $result['data'][0]['name']);
        $this->assertEquals('Bob', $result['data'][1]['name']);
    }

    public function testPaginateAfterSecondPage(): void
    {
        $result = $this->qb->select()->from('users')
            ->paginateAfter('id', 2, 2);

        $this->assertCount(2, $result['data']);
        $this->assertTrue($result['hasMore']);
        $this->assertEquals(4, $result['nextCursor']);
        $this->assertEquals('Charlie', $result['data'][0]['name']);
        $this->assertEquals('Diana', $result['data'][1]['name']);
    }

    public function testPaginateAfterLastPage(): void
    {
        $result = $this->qb->select()->from('users')
            ->paginateAfter('id', 4, 2);

        $this->assertCount(1, $result['data']);
        $this->assertFalse($result['hasMore']);
        $this->assertEquals('Eve', $result['data'][0]['name']);
    }

    public function testPaginateAfterDescDirection(): void
    {
        $result = $this->qb->select()->from('users')
            ->paginateAfter('id', null, 2, 'DESC');

        $this->assertCount(2, $result['data']);
        $this->assertTrue($result['hasMore']);
        $this->assertEquals('Eve', $result['data'][0]['name']);
        $this->assertEquals('Diana', $result['data'][1]['name']);
    }

    public function testPaginateAfterEmptyResult(): void
    {
        $result = $this->qb->select()->from('users')
            ->paginateAfter('id', 999, 10);

        $this->assertCount(0, $result['data']);
        $this->assertFalse($result['hasMore']);
        $this->assertNull($result['nextCursor']);
    }

    public function testPaginateAfterWithScore(): void
    {
        $result = $this->qb->select()->from('users')
            ->paginateAfter('score', 20, 2);

        $this->assertCount(2, $result['data']);
        $this->assertEquals('Charlie', $result['data'][0]['name']);
        $this->assertEquals('Diana', $result['data'][1]['name']);
    }

    // ── P2-D: fresh() copies tableMap ──

    public function testFreshCopiesTableMap(): void
    {
        $this->qb->setTableMap(['usr' => 'users']);
        $fresh = $this->qb->fresh();

        $this->assertEquals($this->qb->getTableMap(), $fresh->getTableMap());
        $this->assertNotSame($this->qb, $fresh);
    }

    public function testFreshResetsQueryState(): void
    {
        $this->qb->select(['name'])->from('users')->where('id', '=', 1);
        $fresh = $this->qb->fresh();

        // fresh should have clean parts
        $parts = $fresh->getParts();
        $this->assertEquals('*', $parts['select']);
        $this->assertEquals('', $parts['from']);
        $this->assertEmpty($parts['where']);
    }

    // ── P2-G: Statement cache ──

    public function testEnableStatementCache(): void
    {
        $this->qb->enableStatementCache();

        // Execute same query twice — should reuse statement
        $result1 = $this->qb->select()->from('users')->where('id', '=', 1)->fetchAll();
        $this->qb->reset();
        $result2 = $this->qb->select()->from('users')->where('id', '=', 1)->fetchAll();

        $this->assertEquals($result1, $result2);

        // Cleanup
        QueryBuilder::clearStatementCache();
    }

    public function testClearStatementCache(): void
    {
        $this->qb->enableStatementCache();
        $this->qb->select()->from('users')->fetchAll();

        QueryBuilder::clearStatementCache();
        // Should not throw after clearing
        $this->assertTrue(true);
    }

    // ── P3-A: Read/Write splitting ──

    public function testSetReadConnection(): void
    {
        // Create a separate "read" connection with different data
        $readPdo = new PDO('sqlite::memory:');
        $readPdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $readPdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, score INTEGER DEFAULT 0)');
        $readPdo->exec("INSERT INTO users (name, email, score) VALUES ('ReadAlice', 'read@test.com', 100)");

        $readConn = $this->createConnection($readPdo);
        $this->qb->setReadConnection($readConn);

        // SELECT should go to read connection
        $result = $this->qb->select()->from('users')->fetchAll();
        $this->assertCount(1, $result);
        $this->assertEquals('ReadAlice', $result[0]['name']);
    }

    public function testWriteStaysOnPrimary(): void
    {
        $readPdo = new PDO('sqlite::memory:');
        $readPdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $readPdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, score INTEGER DEFAULT 0)');

        $readConn = $this->createConnection($readPdo);
        $this->qb->setReadConnection($readConn);

        // INSERT should go to primary connection
        $id = $this->qb->insert('users', ['name' => 'WriteTest', 'email' => 'write@test.com', 'score' => 0]);
        $this->assertGreaterThan(0, $id);

        // Verify row is on primary, not read replica
        $stmt = $this->pdo->prepare("SELECT name FROM users WHERE name = 'WriteTest'");
        $stmt->execute();
        $this->assertEquals('WriteTest', $stmt->fetchColumn());
    }

    public function testIsReadQuery(): void
    {
        $pdo = $this->qb->getEffectivePdo('SELECT * FROM users');
        $this->assertInstanceOf(PDO::class, $pdo);

        $pdo = $this->qb->getEffectivePdo('INSERT INTO users VALUES (1)');
        $this->assertInstanceOf(PDO::class, $pdo);
    }

    // ── P3-D: Query profiling ──

    public function testProfilingDisabledByDefault(): void
    {
        $this->qb->select()->from('users')->fetchAll();
        $this->assertEmpty($this->qb->getQueryLog());
    }

    public function testProfilingRecordsQueries(): void
    {
        $this->qb->enableProfiling();

        $this->qb->select()->from('users')->fetchAll();

        $log = $this->qb->getQueryLog();
        $this->assertCount(1, $log);
        $this->assertArrayHasKey('sql', $log[0]);
        $this->assertArrayHasKey('params', $log[0]);
        $this->assertArrayHasKey('time_ms', $log[0]);
        $this->assertStringContainsString('SELECT', $log[0]['sql']);
        $this->assertIsFloat($log[0]['time_ms']);
        $this->assertGreaterThanOrEqual(0, $log[0]['time_ms']);
    }

    public function testProfilingMultipleQueries(): void
    {
        $this->qb->enableProfiling();

        $this->qb->select()->from('users')->fetchAll();
        $this->qb->reset();
        $this->qb->select()->from('users')->where('id', '=', 1)->fetch();

        $log = $this->qb->getQueryLog();
        $this->assertCount(2, $log);
    }

    public function testResetQueryLog(): void
    {
        $this->qb->enableProfiling();
        $this->qb->select()->from('users')->fetchAll();

        $this->assertNotEmpty($this->qb->getQueryLog());

        $this->qb->resetQueryLog();
        $this->assertEmpty($this->qb->getQueryLog());
    }

    // ── P2-F: PSR-3 Logger ──

    public function testSetLoggerAcceptsPsr3(): void
    {
        // Use a simple mock logger
        $logger = new class implements \Psr\Log\LoggerInterface {
            public array $messages = [];
            public function emergency(\Stringable|string $message, array $context = []): void { $this->messages[] = ['emergency', $message]; }
            public function alert(\Stringable|string $message, array $context = []): void { $this->messages[] = ['alert', $message]; }
            public function critical(\Stringable|string $message, array $context = []): void { $this->messages[] = ['critical', $message]; }
            public function error(\Stringable|string $message, array $context = []): void { $this->messages[] = ['error', $message]; }
            public function warning(\Stringable|string $message, array $context = []): void { $this->messages[] = ['warning', $message]; }
            public function notice(\Stringable|string $message, array $context = []): void { $this->messages[] = ['notice', $message]; }
            public function info(\Stringable|string $message, array $context = []): void { $this->messages[] = ['info', $message]; }
            public function debug(\Stringable|string $message, array $context = []): void { $this->messages[] = ['debug', $message]; }
            public function log($level, \Stringable|string $message, array $context = []): void { $this->messages[] = [$level, $message]; }
        };

        $result = $this->qb->setLogger($logger);

        // Fluent interface
        $this->assertSame($this->qb, $result);
    }

    // ── P2-A: PSR-16 result cache ──

    public function testCacheMethodIsFluent(): void
    {
        $result = $this->qb->cache(60, 'test_key');
        $this->assertSame($this->qb, $result);
    }

    public function testSetCacheAcceptsPsr16(): void
    {
        $cache = new class implements \Psr\SimpleCache\CacheInterface {
            private array $store = [];
            public function get(string $key, mixed $default = null): mixed { return $this->store[$key] ?? $default; }
            public function set(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool { $this->store[$key] = $value; return true; }
            public function delete(string $key): bool { unset($this->store[$key]); return true; }
            public function clear(): bool { $this->store = []; return true; }
            public function getMultiple(iterable $keys, mixed $default = null): iterable { return []; }
            public function setMultiple(iterable $values, null|int|\DateInterval $ttl = null): bool { return true; }
            public function deleteMultiple(iterable $keys): bool { return true; }
            public function has(string $key): bool { return isset($this->store[$key]); }
        };

        $this->qb->setCache($cache, 120);

        // Query with caching
        $result1 = $this->qb->select()->from('users')->cache(60)->fetchAll();
        $this->assertCount(5, $result1);

        // Second query should come from cache (same SQL + params)
        $this->qb->reset();
        $result2 = $this->qb->select()->from('users')->cache(60)->fetchAll();
        $this->assertEquals($result1, $result2);
    }

    public function testCacheWithCustomKey(): void
    {
        $cache = new class implements \Psr\SimpleCache\CacheInterface {
            public array $store = [];
            public function get(string $key, mixed $default = null): mixed { return $this->store[$key] ?? $default; }
            public function set(string $key, mixed $value, null|int|\DateInterval $ttl = null): bool { $this->store[$key] = $value; return true; }
            public function delete(string $key): bool { unset($this->store[$key]); return true; }
            public function clear(): bool { $this->store = []; return true; }
            public function getMultiple(iterable $keys, mixed $default = null): iterable { return []; }
            public function setMultiple(iterable $values, null|int|\DateInterval $ttl = null): bool { return true; }
            public function deleteMultiple(iterable $keys): bool { return true; }
            public function has(string $key): bool { return isset($this->store[$key]); }
        };

        $this->qb->setCache($cache);
        $this->qb->select()->from('users')->cache(60, 'all_users')->fetchAll();

        $this->assertArrayHasKey('mlq:all_users', $cache->store);
    }

    // ── P3-B: Preflight cache ──

    public function testPreflightCacheDoesNotBreakQueries(): void
    {
        // Run same structural query twice — second should use cache
        $result1 = $this->qb->select()->from('users')->where('id', '=', 1)->fetchAll();
        $this->qb->reset();
        $result2 = $this->qb->select()->from('users')->where('id', '=', 2)->fetchAll();

        $this->assertCount(1, $result1);
        $this->assertCount(1, $result2);
        $this->assertEquals('Alice', $result1[0]['name']);
        $this->assertEquals('Bob', $result2[0]['name']);
    }

    // ── Combined feature test ──

    public function testProfilingWithStatementCache(): void
    {
        $this->qb->enableProfiling()->enableStatementCache();

        $this->qb->select()->from('users')->fetchAll();
        $this->qb->reset();
        $this->qb->select()->from('users')->where('id', '=', 1)->fetch();

        $log = $this->qb->getQueryLog();
        $this->assertCount(2, $log);

        // Both should have positive timing
        foreach ($log as $entry) {
            $this->assertGreaterThanOrEqual(0, $entry['time_ms']);
        }

        QueryBuilder::clearStatementCache();
    }
}
