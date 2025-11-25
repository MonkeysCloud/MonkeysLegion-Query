<?php

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Database\Contracts\ConnectionInterface;

class QueryBuilderTest extends TestCase
{
    private QueryBuilder $qb;

    protected function setUp(): void
    {
        // Use a mock or in-memory SQLite for demonstration
        $pdo = new PDO('sqlite::memory:');
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)');
        $pdo->exec("INSERT INTO users (name, email) VALUES ('Alice', 'alice@example.com'), ('Bob', 'bob@example.com')");

        $conn = new class($pdo) implements ConnectionInterface {
            public function __construct(private PDO $pdo) {}
            public function pdo(): PDO
            {
                return $this->pdo;
            }
            public function connect(): void {}
            public function disconnect(): void {}
            public function isConnected(): bool
            {
                return true;
            }
            public function getDsn(): string
            {
                return '';
            }
            public function isAlive(): bool
            {
                return true;
            }
        };

        $this->qb = new QueryBuilder($conn);
    }

    public function testSelectBasic()
    {
        $sql = $this->qb->select(['id', 'name'])->from('users')->toSql();
        // Accept both quoted and unquoted table names
        $this->assertMatchesRegularExpression('/SELECT id, name FROM [`"]?users[`"]?/i', $sql);
    }

    public function testWhere()
    {
        $this->qb->select()->from('users')->where('name', '=', 'Alice');
        $sql = $this->qb->toSql();
        $this->assertStringContainsString('WHERE name =', $sql);
        $result = $this->qb->fetchAll();
        $this->assertCount(1, $result);
        $this->assertEquals('Alice', $result[0]['name']);
    }

    public function testOrWhere()
    {
        $this->qb->select()->from('users')
            ->where('name', '=', 'Alice')
            ->orWhere('name', '=', 'Bob');
        $result = $this->qb->fetchAll();
        $this->assertCount(2, $result);
    }

    public function testJoin()
    {
        $this->qb->select(['u.id', 'u.name'])
            ->from('users', 'u')
            ->leftJoin('users', 'u2', 'u.id', '=', 'u2.id');
        $sql = $this->qb->toSql();
        // Accept both quoted and unquoted table names and aliases
        $this->assertMatchesRegularExpression('/LEFT JOIN [`"]?users[`"]? (AS )?u2 ON u\.id = u2\.id/i', $sql);
    }

    public function testInsertAndFetch()
    {
        $id = $this->qb->insert('users', ['name' => 'Charlie', 'email' => 'charlie@example.com']);
        $this->assertIsInt($id);

        $user = $this->qb->select()->from('users')->where('id', '=', $id)->fetch();
        $this->assertEquals('Charlie', $user->name);
    }

    public function testUpdate()
    {
        $this->qb->update('users', ['name' => 'AliceUpdated'])->where('name', '=', 'Alice')->execute();
        $user = $this->qb->select()->from('users')->where('name', '=', 'AliceUpdated')->fetch();
        $this->assertEquals('AliceUpdated', $user->name);
    }

    public function testDelete()
    {
        $this->qb->delete('users')->where('name', '=', 'Bob')->execute();
        $user = $this->qb->select()->from('users')->where('name', '=', 'Bob')->fetch();
        $this->assertFalse($user);
    }

    public function testAggregateCount()
    {
        $count = $this->qb->select()->from('users')->count();
        $this->assertIsInt($count);
        $this->assertGreaterThan(0, $count);
    }

    public function testAggregateSum()
    {
        // Add a numeric column for sum test
        $this->qb->pdo()->exec('ALTER TABLE users ADD COLUMN score INTEGER DEFAULT 0');
        $this->qb->pdo()->exec('UPDATE users SET score = 10');
        $sum = $this->qb->select()->from('users')->sum('score');
        $this->assertEquals(20, $sum);
    }

    public function testTransaction()
    {
        $this->qb->beginTransaction();
        $this->qb->insert('users', ['name' => 'TxUser', 'email' => 'tx@example.com']);
        $this->qb->rollback();
        $user = $this->qb->select()->from('users')->where('name', '=', 'TxUser')->fetch();
        $this->assertFalse($user);
    }

    public function testDuplicate()
    {
        $qb2 = $this->qb->select(['id'])->from('users')->duplicate();
        $this->assertNotSame($this->qb, $qb2);
        $this->assertEquals($this->qb->toSql(), $qb2->toSql());
    }
}
