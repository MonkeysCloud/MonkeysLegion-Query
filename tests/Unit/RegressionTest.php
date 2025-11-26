<?php

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Database\Contracts\ConnectionInterface;

class RegressionTest extends TestCase
{
    private QueryBuilder $qb;

    protected function setUp(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, active INTEGER)');
        $pdo->exec("INSERT INTO users (name, active) VALUES ('Alice', 1), ('Bob', 0), ('Charlie', 1)");

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

    public function testBuilderReuseAfterCount()
    {
        // Configure builder
        $this->qb->from('users')->where('active', '=', 1);

        // First operation: count
        $count = $this->qb->count();
        $this->assertEquals(2, $count, 'Count should match active users');

        // Second operation: fetchAll
        // If regression exists, the 'where' clause will be missing, returning all 3 users
        $users = $this->qb->fetchAll();
        $this->assertCount(2, $users, 'Should still only fetch active users after count()');
        
        foreach ($users as $user) {
            $this->assertEquals(1, $user['active']);
        }
    }
    
    public function testBuilderReuseAfterValue()
    {
         // Configure builder
        $this->qb->from('users')->where('name', '=', 'Alice');

        // First operation: value
        $id = $this->qb->value('id');
        $this->assertEquals(1, $id);

        // Second operation: fetch
        // If regression exists, 'where name=Alice' might be lost
        $user = $this->qb->fetch();
        $this->assertEquals('Alice', $user->name, 'Should still fetch Alice after value()');
    }
}
