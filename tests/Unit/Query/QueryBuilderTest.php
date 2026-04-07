<?php
declare(strict_types=1);

namespace Tests\Unit\Query;

use MonkeysLegion\Query\Query\QueryBuilder;
use PDO;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tests\Support\TestConnection;
use Tests\Support\TestConnectionManager;

#[CoversClass(QueryBuilder::class)]
final class QueryBuilderTest extends TestCase
{
    private TestConnectionManager $manager;
    private PDO $pdo;

    protected function setUp(): void
    {
        QueryBuilder::clearStatementCache();

        // Create in-memory SQLite for real execution tests
        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        // Seed test data
        $this->pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT, status TEXT, age INTEGER, created_at TEXT)');
        $this->pdo->exec("INSERT INTO users (name, email, status, age, created_at) VALUES ('Alice', 'alice@test.com', 'active', 30, '2026-01-01')");
        $this->pdo->exec("INSERT INTO users (name, email, status, age, created_at) VALUES ('Bob', 'bob@test.com', 'inactive', 25, '2026-02-01')");
        $this->pdo->exec("INSERT INTO users (name, email, status, age, created_at) VALUES ('Charlie', 'charlie@test.com', 'active', 35, '2026-03-01')");

        $this->pdo->exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER, total REAL, status TEXT)');
        $this->pdo->exec("INSERT INTO orders (user_id, total, status) VALUES (1, 99.99, 'completed')");
        $this->pdo->exec("INSERT INTO orders (user_id, total, status) VALUES (1, 149.50, 'pending')");
        $this->pdo->exec("INSERT INTO orders (user_id, total, status) VALUES (2, 75.00, 'completed')");

        // Build concrete test doubles (no mocks — PHP 8.4 property hooks prevent mocking)
        $connection = new TestConnection($this->pdo);
        $this->manager = new TestConnectionManager($connection);
    }

    private function qb(): QueryBuilder
    {
        return new QueryBuilder($this->manager);
    }

    // ── SQL Compilation (no execution) ──────────────────────────

    public function testSelectAll(): void
    {
        $sql = $this->qb()->from('users')->toSql();
        self::assertSame('SELECT * FROM users', $sql);
    }

    public function testSelectColumns(): void
    {
        $sql = $this->qb()->select(['id', 'name'])->from('users')->toSql();
        self::assertSame('SELECT id, name FROM users', $sql);
    }

    public function testSelectDistinct(): void
    {
        $sql = $this->qb()->select(['status'])->distinct()->from('users')->toSql();
        self::assertSame('SELECT DISTINCT status FROM users', $sql);
    }

    public function testWhereEqual(): void
    {
        $sql = $this->qb()->from('users')->where('status', '=', 'active')->toSql();
        self::assertSame('SELECT * FROM users WHERE status = ?', $sql);
    }

    public function testMultipleWheres(): void
    {
        $sql = $this->qb()
            ->from('users')
            ->where('status', '=', 'active')
            ->where('age', '>', 18)
            ->toSql();

        self::assertSame('SELECT * FROM users WHERE status = ? AND age > ?', $sql);
    }

    public function testOrWhere(): void
    {
        $sql = $this->qb()
            ->from('users')
            ->where('role', '=', 'admin')
            ->orWhere('role', '=', 'super_admin')
            ->toSql();

        self::assertSame('SELECT * FROM users WHERE role = ? OR role = ?', $sql);
    }

    public function testWhereIn(): void
    {
        $sql = $this->qb()->from('users')->whereIn('id', [1, 2, 3])->toSql();
        self::assertSame('SELECT * FROM users WHERE id IN (?, ?, ?)', $sql);
    }

    public function testWhereNull(): void
    {
        $sql = $this->qb()->from('users')->whereNull('deleted_at')->toSql();
        self::assertSame('SELECT * FROM users WHERE deleted_at IS NULL', $sql);
    }

    public function testWhereBetween(): void
    {
        $sql = $this->qb()->from('users')->whereBetween('age', 18, 65)->toSql();
        self::assertSame('SELECT * FROM users WHERE age BETWEEN ? AND ?', $sql);
    }

    public function testOrderBy(): void
    {
        $sql = $this->qb()->from('users')->orderBy('name', 'ASC')->toSql();
        self::assertSame('SELECT * FROM users ORDER BY name ASC', $sql);
    }

    public function testOrderByDesc(): void
    {
        $sql = $this->qb()->from('users')->orderByDesc('created_at')->toSql();
        self::assertSame('SELECT * FROM users ORDER BY created_at DESC', $sql);
    }

    public function testGroupByHaving(): void
    {
        $sql = $this->qb()
            ->select(['status', 'COUNT(*) AS cnt'])
            ->from('users')
            ->groupBy('status')
            ->having('COUNT(*) > ?', [1])
            ->toSql();

        self::assertSame('SELECT status, COUNT(*) AS cnt FROM users GROUP BY status HAVING COUNT(*) > ?', $sql);
    }

    public function testLimitOffset(): void
    {
        $sql = $this->qb()->from('users')->limit(10)->offset(20)->toSql();
        self::assertSame('SELECT * FROM users LIMIT 10 OFFSET 20', $sql);
    }

    public function testForPage(): void
    {
        $sql = $this->qb()->from('users')->forPage(3, 25)->toSql();
        self::assertSame('SELECT * FROM users LIMIT 25 OFFSET 50', $sql);
    }

    public function testJoinOn(): void
    {
        $sql = $this->qb()
            ->select(['u.name', 'o.total'])
            ->from('users', 'u')
            ->joinOn('orders', 'u.id', '=', 'orders.user_id', 'o')
            ->toSql();

        self::assertStringContainsString('INNER JOIN orders AS o ON u.id = orders.user_id', $sql);
    }

    public function testLeftJoinOn(): void
    {
        $sql = $this->qb()
            ->from('users', 'u')
            ->leftJoinOn('profiles', 'u.id', '=', 'profiles.user_id')
            ->toSql();

        self::assertStringContainsString('LEFT JOIN profiles ON u.id = profiles.user_id', $sql);
    }

    public function testJoinWithCallback(): void
    {
        $sql = $this->qb()
            ->from('users', 'u')
            ->join('orders', fn($j) => $j
                ->on('u.id', '=', 'orders.user_id')
                ->andOn('orders.status', '=', "'active'"),
                'o',
            )
            ->toSql();

        self::assertStringContainsString("INNER JOIN orders AS o ON u.id = orders.user_id AND orders.status = 'active'", $sql);
    }

    // ── Execution Tests (SQLite in-memory) ──────────────────────

    public function testGetAll(): void
    {
        $rows = $this->qb()->from('users')->get();
        self::assertCount(3, $rows);
        self::assertSame('Alice', $rows[0]['name']);
    }

    public function testFirst(): void
    {
        $row = $this->qb()->from('users')->where('name', '=', 'Bob')->first();
        self::assertNotNull($row);
        self::assertSame('bob@test.com', $row['email']);
    }

    public function testFirstReturnsNull(): void
    {
        $row = $this->qb()->from('users')->where('name', '=', 'Nobody')->first();
        self::assertNull($row);
    }

    public function testValue(): void
    {
        $email = $this->qb()->from('users')->where('name', '=', 'Alice')->value('email');
        self::assertSame('alice@test.com', $email);
    }

    public function testPluck(): void
    {
        $names = $this->qb()->from('users')->orderBy('name')->pluck('name');
        self::assertSame(['Alice', 'Bob', 'Charlie'], $names);
    }

    public function testExists(): void
    {
        self::assertTrue($this->qb()->from('users')->where('status', '=', 'active')->exists());
        self::assertFalse($this->qb()->from('users')->where('status', '=', 'banned')->exists());
    }

    public function testCount(): void
    {
        $count = $this->qb()->from('users')->count();
        self::assertSame(3, $count);
    }

    public function testCountWithWhere(): void
    {
        $count = $this->qb()->from('users')->where('status', '=', 'active')->count();
        self::assertSame(2, $count);
    }

    public function testSum(): void
    {
        $sum = $this->qb()->from('orders')->sum('total');
        self::assertEqualsWithDelta(324.49, $sum, 0.01);
    }

    public function testAvg(): void
    {
        $avg = $this->qb()->from('users')->avg('age');
        self::assertEqualsWithDelta(30.0, $avg, 0.01);
    }

    public function testMin(): void
    {
        $min = $this->qb()->from('users')->min('age');
        self::assertSame(25, (int) $min);
    }

    public function testMax(): void
    {
        $max = $this->qb()->from('users')->max('age');
        self::assertSame(35, (int) $max);
    }

    // ── DML Tests ───────────────────────────────────────────────

    public function testInsert(): void
    {
        $id = $this->qb()->from('users')->insert([
            'name'       => 'Diana',
            'email'      => 'diana@test.com',
            'status'     => 'active',
            'age'        => 28,
            'created_at' => '2026-04-01',
        ]);

        self::assertNotFalse($id);
        self::assertSame(4, $this->qb()->from('users')->count());
    }

    public function testInsertMany(): void
    {
        $count = $this->qb()->from('users')->insertMany([
            ['name' => 'Eve', 'email' => 'eve@test.com', 'status' => 'active', 'age' => 22, 'created_at' => '2026-04-01'],
            ['name' => 'Frank', 'email' => 'frank@test.com', 'status' => 'inactive', 'age' => 40, 'created_at' => '2026-04-01'],
        ]);

        self::assertSame(2, $count);
        self::assertSame(5, $this->qb()->from('users')->count());
    }

    public function testUpdate(): void
    {
        $affected = $this->qb()
            ->from('users')
            ->where('name', '=', 'Bob')
            ->update(['status' => 'active']);

        self::assertSame(1, $affected);

        $bob = $this->qb()->from('users')->where('name', '=', 'Bob')->first();
        self::assertSame('active', $bob['status']);
    }

    public function testDelete(): void
    {
        $affected = $this->qb()
            ->from('users')
            ->where('status', '=', 'inactive')
            ->delete();

        self::assertSame(1, $affected);
        self::assertSame(2, $this->qb()->from('users')->count());
    }

    // ── Property Hooks ──────────────────────────────────────────

    public function testBindingCount(): void
    {
        $qb = $this->qb()
            ->from('users')
            ->where('status', '=', 'active')
            ->where('age', '>', 18);

        self::assertSame(2, $qb->bindingCount);
    }

    public function testConnectionName(): void
    {
        $qb = $this->qb();
        self::assertSame('default', $qb->connectionName);
    }

    // ── Reset ───────────────────────────────────────────────────

    public function testReset(): void
    {
        $qb = $this->qb()
            ->from('users')
            ->where('status', '=', 'active')
            ->orderBy('name')
            ->limit(10);

        $qb->reset();
        self::assertSame('SELECT * FROM users', $qb->toSql());
        self::assertSame(0, $qb->bindingCount);
    }

    // ── Debug SQL ───────────────────────────────────────────────

    public function testToDebugSql(): void
    {
        $debug = $this->qb()
            ->from('users')
            ->where('status', '=', 'active')
            ->where('age', '>', 18)
            ->toDebugSql();

        self::assertSame("SELECT * FROM users WHERE status = 'active' AND age > 18", $debug);
    }
}
