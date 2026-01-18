<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Database\Contracts\ConnectionInterface;

/**
 * Unit tests for QueryBuilder JOIN operations.
 */
class JoinClauseTest extends TestCase
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

    public function testSimpleJoin(): void
    {
        $sql = $this->qb
            ->select(['users.*', 'profiles.bio'])
            ->from('users')
            ->join('profiles', 'p', 'users.id', '=', 'p.user_id')
            ->toSql();

        $this->assertStringContainsString('JOIN', $sql);
        $this->assertStringContainsString('profiles', $sql);
        $this->assertStringContainsString('ON', $sql);
    }

    public function testLeftJoin(): void
    {
        $sql = $this->qb
            ->select(['users.*', 'orders.total'])
            ->from('users')
            ->leftJoin('orders', 'o', 'users.id', '=', 'o.user_id')
            ->toSql();

        $this->assertStringContainsString('LEFT JOIN', $sql);
    }

    public function testRightJoin(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('orders')
            ->rightJoin('users', 'u', 'orders.user_id', '=', 'u.id')
            ->toSql();

        $this->assertStringContainsString('RIGHT JOIN', $sql);
    }

    public function testMultipleJoins(): void
    {
        $sql = $this->qb
            ->select(['u.*', 'p.bio', 'r.name as role_name'])
            ->from('users', 'u')
            ->join('profiles', 'p', 'u.id', '=', 'p.user_id')
            ->join('roles', 'r', 'u.role_id', '=', 'r.id')
            ->toSql();

        $this->assertMatchesRegularExpression('/JOIN.*JOIN/', $sql);
    }

    public function testJoinWithWhere(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->join('orders', 'o', 'users.id', '=', 'o.user_id')
            ->where('o.status', '=', 'completed')
            ->toSql();

        $this->assertStringContainsString('JOIN', $sql);
        $this->assertStringContainsString('WHERE', $sql);
    }

    public function testJoinWithAlias(): void
    {
        $sql = $this->qb
            ->select(['u.name', 'o.total'])
            ->from('users', 'u')
            ->join('orders', 'o', 'u.id', '=', 'o.user_id')
            ->toSql();

        // The join syntax uses table alias after the table name
        $this->assertStringContainsString('orders', $sql);
        $this->assertStringContainsString('o.user_id', $sql);
    }

    public function testLeftJoinWithNullCheck(): void
    {
        $sql = $this->qb
            ->select(['users.*'])
            ->from('users')
            ->leftJoin('orders', 'o', 'users.id', '=', 'o.user_id')
            ->whereNull('o.id')
            ->toSql();

        $this->assertStringContainsString('LEFT JOIN', $sql);
        $this->assertStringContainsString('IS NULL', $sql);
    }

    public function testJoinWithOrderBy(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->join('profiles', 'p', 'users.id', '=', 'p.user_id')
            ->orderBy('users.name', 'ASC')
            ->toSql();

        $this->assertStringContainsString('JOIN', $sql);
        $this->assertStringContainsString('ORDER BY', $sql);
    }

    public function testJoinWithGroupBy(): void
    {
        $sql = $this->qb
            ->select(['users.id', 'COUNT(orders.id) as order_count'])
            ->from('users')
            ->leftJoin('orders', 'o', 'users.id', '=', 'o.user_id')
            ->groupBy('users.id')
            ->toSql();

        $this->assertStringContainsString('LEFT JOIN', $sql);
        $this->assertStringContainsString('GROUP BY', $sql);
    }

    public function testJoinWithLimit(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('products')
            ->join('categories', 'c', 'products.category_id', '=', 'c.id')
            ->limit(10)
            ->toSql();

        $this->assertStringContainsString('JOIN', $sql);
        $this->assertStringContainsString('LIMIT', $sql);
    }

    public function testCrossJoin(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('colors')
            ->crossJoin('sizes')
            ->toSql();

        $this->assertStringContainsString('CROSS JOIN', $sql);
    }

    public function testInnerJoin(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('posts')
            ->innerJoin('users', 'u', 'posts.user_id', '=', 'u.id')
            ->toSql();

        $this->assertStringContainsString('INNER JOIN', $sql);
    }
}
