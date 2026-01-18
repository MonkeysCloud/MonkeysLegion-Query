<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Database\Contracts\ConnectionInterface;

/**
 * Unit tests for QueryBuilder SELECT operations.
 */
class SelectClauseTest extends TestCase
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

    public function testSelectAll(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->toSql();

        $this->assertStringContainsString('SELECT', $sql);
        $this->assertStringContainsString('*', $sql);
        $this->assertStringContainsString('FROM', $sql);
        $this->assertStringContainsString('users', $sql);
    }

    public function testSelectSpecificColumns(): void
    {
        $sql = $this->qb
            ->select(['id', 'name', 'email'])
            ->from('users')
            ->toSql();

        $this->assertStringContainsString('id', $sql);
        $this->assertStringContainsString('name', $sql);
        $this->assertStringContainsString('email', $sql);
    }

    public function testSelectWithAlias(): void
    {
        $sql = $this->qb
            ->select(['id', 'full_name as name'])
            ->from('users')
            ->toSql();

        $this->assertStringContainsString('full_name', $sql);
        $this->assertStringContainsString('name', $sql);
    }

    public function testSelectDistinct(): void
    {
        $sql = $this->qb
            ->select(['status'])
            ->distinct()
            ->from('orders')
            ->toSql();

        $this->assertStringContainsString('DISTINCT', $sql);
    }

    public function testSelectWithTableAlias(): void
    {
        $sql = $this->qb
            ->select(['u.id', 'u.name'])
            ->from('users', 'u')
            ->toSql();

        $this->assertStringContainsString('u.id', $sql);
        $this->assertStringContainsString('u.name', $sql);
    }

    public function testSelectCount(): void
    {
        $sql = $this->qb
            ->select(['COUNT(*) as total'])
            ->from('users')
            ->toSql();

        $this->assertStringContainsString('COUNT(*)', $sql);
    }

    public function testSelectSum(): void
    {
        $sql = $this->qb
            ->select(['SUM(amount) as total_amount'])
            ->from('orders')
            ->toSql();

        $this->assertStringContainsString('SUM(amount)', $sql);
    }

    public function testSelectAvg(): void
    {
        $sql = $this->qb
            ->select(['AVG(price) as avg_price'])
            ->from('products')
            ->toSql();

        $this->assertStringContainsString('AVG(price)', $sql);
    }

    public function testSelectMax(): void
    {
        $sql = $this->qb
            ->select(['MAX(amount) as max_amount'])
            ->from('orders')
            ->toSql();

        $this->assertStringContainsString('MAX(amount)', $sql);
    }

    public function testSelectMin(): void
    {
        $sql = $this->qb
            ->select(['MIN(price) as min_price'])
            ->from('products')
            ->toSql();

        $this->assertStringContainsString('MIN(price)', $sql);
    }

    public function testSelectWithGroupBy(): void
    {
        $sql = $this->qb
            ->select(['category_id', 'COUNT(*) as count'])
            ->from('products')
            ->groupBy('category_id')
            ->toSql();

        $this->assertStringContainsString('GROUP BY', $sql);
    }

    public function testSelectWithHaving(): void
    {
        $sql = $this->qb
            ->select(['category_id', 'COUNT(*) as count'])
            ->from('products')
            ->groupBy('category_id')
            ->having('COUNT(*)', '>', 5)
            ->toSql();

        $this->assertStringContainsString('HAVING', $sql);
    }

    public function testSelectWithOrderBy(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->orderBy('created_at', 'DESC')
            ->toSql();

        $this->assertStringContainsString('ORDER BY', $sql);
        $this->assertStringContainsString('DESC', $sql);
    }

    public function testSelectWithLimit(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->limit(10)
            ->toSql();

        $this->assertStringContainsString('LIMIT', $sql);
        $this->assertStringContainsString('10', $sql);
    }

    public function testSelectWithOffset(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('users')
            ->limit(10)
            ->offset(20)
            ->toSql();

        $this->assertStringContainsString('OFFSET', $sql);
        $this->assertStringContainsString('20', $sql);
    }

    public function testSelectWithMultipleOrderBy(): void
    {
        $sql = $this->qb
            ->select(['*'])
            ->from('products')
            ->orderBy('category_id', 'ASC')
            ->orderBy('price', 'DESC')
            ->toSql();

        $this->assertStringContainsString('ORDER BY', $sql);
    }

    public function testCompleteQuery(): void
    {
        $sql = $this->qb
            ->select(['category_id', 'AVG(price) as avg_price'])
            ->from('products')
            ->where('active', '=', 1)
            ->groupBy('category_id')
            ->having('AVG(price)', '>', 100)
            ->orderBy('avg_price', 'DESC')
            ->limit(5)
            ->toSql();

        $this->assertStringContainsString('SELECT', $sql);
        $this->assertStringContainsString('FROM', $sql);
        $this->assertStringContainsString('WHERE', $sql);
        $this->assertStringContainsString('GROUP BY', $sql);
        $this->assertStringContainsString('HAVING', $sql);
        $this->assertStringContainsString('ORDER BY', $sql);
        $this->assertStringContainsString('LIMIT', $sql);
    }
}
