<?php

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Database\Contracts\ConnectionInterface;

class ResetTest extends TestCase
{
    private QueryBuilder $qb;

    protected function setUp(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, active INTEGER)');
        $pdo->exec("INSERT INTO users (name, active) VALUES ('Alice', 1), ('Bob', 0)");
        $pdo->exec('CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, published INTEGER)');
        $pdo->exec("INSERT INTO posts (title, published) VALUES ('Post 1', 1)");

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

    public function testResetClearsState()
    {
        // 1. Configure builder for first query
        $this->qb->from('users')->where('active', '=', 1);

        // 2. Run first query
        $count = $this->qb->count();
        $this->assertEquals(1, $count);

        // 3. Reset builder
        $this->qb->reset();

        // 4. Configure builder for second query (different table)
        $this->qb->from('posts')->where('published', '=', 1);

        // 5. Run second query
        // If reset didn't work, it might still have 'users' table or 'active' where clause
        $posts = $this->qb->fetchAll();

        $this->assertCount(1, $posts);
        $this->assertEquals('Post 1', $posts[0]['title']);
    }
}
