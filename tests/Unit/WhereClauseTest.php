<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Database\Contracts\ConnectionInterface;

/**
 * Unit tests for QueryBuilder WHERE clause operations.
 */
class WhereClauseTest extends TestCase
{
    private QueryBuilder $qb;
    private \PDO $pdo;

    protected function setUp(): void
    {
        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $pdo = $this->pdo;
        $conn = new class($pdo) implements ConnectionInterface {
            public function __construct(private \PDO $pdo) {}
            public function pdo(): \PDO { return $this->pdo; }
            public function connect(): void {}
            public function disconnect(): void {}
            public function isConnected(): bool { return true; }
            public function getDsn(): string { return ''; }
            public function isAlive(): bool { return true; }
        };

        $this->qb = new QueryBuilder($conn);
    }

    public function testSimpleWhere(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->where('id', '=', 1)
            ->toSql();

        $this->assertStringContainsString('WHERE', $sql);
        $this->assertStringContainsString('id', $sql);
    }

    public function testWhereWithMultipleConditions(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->where('status', '=', 'active')
            ->where('role', '=', 'admin')
            ->toSql();

        $this->assertStringContainsString('WHERE', $sql);
        $this->assertStringContainsString('AND', $sql);
    }

    public function testOrWhere(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->where('status', '=', 'active')
            ->orWhere('status', '=', 'pending')
            ->toSql();

        $this->assertStringContainsString('OR', $sql);
    }

    public function testWhereIn(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->whereIn('id', [1, 2, 3])
            ->toSql();

        $this->assertStringContainsString('IN', $sql);
    }

    public function testWhereNotIn(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->whereNotIn('status', ['deleted', 'banned'])
            ->toSql();

        $this->assertStringContainsString('NOT IN', $sql);
    }

    public function testWhereNull(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->whereNull('deleted_at')
            ->toSql();

        $this->assertStringContainsString('IS NULL', $sql);
    }

    public function testWhereNotNull(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->whereNotNull('email')
            ->toSql();

        $this->assertStringContainsString('IS NOT NULL', $sql);
    }

    public function testWhereBetween(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('orders')
            ->whereBetween('total', 100, 500)
            ->toSql();

        $this->assertStringContainsString('BETWEEN', $sql);
    }

    public function testWhereLike(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->where('name', 'LIKE', '%john%')
            ->toSql();

        $this->assertStringContainsString('LIKE', $sql);
    }

    public function testWhereGreaterThan(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('products')
            ->where('price', '>', 100)
            ->toSql();

        $this->assertStringContainsString('>', $sql);
    }

    public function testWhereLessThanOrEqual(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('products')
            ->where('stock', '<=', 10)
            ->toSql();

        $this->assertStringContainsString('<=', $sql);
    }

    public function testWhereNotEqual(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->where('status', '!=', 'deleted')
            ->toSql();

        $this->assertStringContainsString('!=', $sql);
    }

    public function testComplexWhere(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('products')
            ->where('active', '=', 1)
            ->where('stock', '>', 0)
            ->where('price', '<', 1000)
            ->whereNotNull('category_id')
            ->toSql();

        $this->assertStringContainsString('WHERE', $sql);
        $this->assertStringContainsString('AND', $sql);
        $this->assertStringContainsString('IS NOT NULL', $sql);
    }

    public function testResetClearsWhere(): void
    {
        $this->qb
            ->select(['*'])
            ->from('users')
            ->where('id', '=', 1)
            ->reset();

        $sql = $this->qb
            ->select(['*'])
            ->from('posts')
            ->toSql();

        $this->assertStringNotContainsString('WHERE', $sql);
    }
}
