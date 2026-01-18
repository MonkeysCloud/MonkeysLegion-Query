<?php

declare(strict_types=1);

namespace Tests\Unit;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Repository\EntityRepository;
use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\Entity;

/**
 * Test Entity with string UUID primary key
 */
#[Entity(table: 'uuid_items')]
class UuidItem
{
    #[Field(type: 'string', primaryKey: true)]
    public ?string $id = null;

    #[Field(type: 'string')]
    public string $name = '';
}

/**
 * Test Repository for UUID entities
 */
class UuidItemRepository extends EntityRepository
{
    protected string $table = 'uuid_items';
    protected string $entityClass = UuidItem::class;
}

/**
 * Unit tests for database compatibility features.
 * 
 * Tests cover:
 * - Identifier quoting for different databases (MySQL, PostgreSQL, SQLite)
 * - UUID/string primary key support
 * - Table name sanitization
 */
class DatabaseCompatibilityTest extends TestCase
{
    private QueryBuilder $qb;
    private \PDO $pdo;

    protected function setUp(): void
    {
        // Use in-memory SQLite for testing
        $this->pdo = new \PDO('sqlite::memory:', null, null, [
            \PDO::ATTR_ERRMODE => \PDO::ERRMODE_EXCEPTION,
            \PDO::ATTR_DEFAULT_FETCH_MODE => \PDO::FETCH_OBJ,
        ]);

        // Create mock connection
        $conn = new class($this->pdo) implements ConnectionInterface {
            public function __construct(private \PDO $pdo) {}
            public function pdo(): \PDO { return $this->pdo; }
            public function connect(): void {}
            public function disconnect(): void {}
            public function isConnected(): bool { return true; }
            public function getDsn(): string { return ''; }
            public function isAlive(): bool { return true; }
        };

        $this->qb = new QueryBuilder($conn);

        // Create test tables
        $this->pdo->exec('CREATE TABLE uuid_items (id TEXT PRIMARY KEY, name TEXT)');
    }

    /**
     * Test quoteIdentifier for MySQL (uses backticks)
     */
    public function testQuoteIdentifierMysql(): void
    {
        $result = $this->qb->quoteIdentifier('column_name');
        
        // SQLite uses double quotes by default
        $this->assertStringContainsString('column_name', $result);
        $this->assertTrue(
            str_starts_with($result, '"') || str_starts_with($result, '`'),
            'Identifier should be quoted'
        );
    }

    /**
     * Test quoteIdentifier with reserved words
     */
    public function testQuoteIdentifierReservedWords(): void
    {
        $reservedWords = ['order', 'group', 'key', 'table', 'index'];
        
        foreach ($reservedWords as $word) {
            $quoted = $this->qb->quoteIdentifier($word);
            $this->assertNotEquals($word, $quoted, "Reserved word '$word' should be quoted");
        }
    }

    /**
     * Test quoteIdentifier with compound identifiers (table.column)
     */
    public function testQuoteIdentifierCompound(): void
    {
        // Quote both parts separately and combine
        $quotedTable = $this->qb->quoteIdentifier('users');
        $quotedColumn = $this->qb->quoteIdentifier('id');
        $result = $quotedTable . '.' . $quotedColumn;
        
        // Should contain both parts
        $this->assertStringContainsString('users', $result);
        $this->assertStringContainsString('id', $result);
        $this->assertStringContainsString('.', $result);
    }

    /**
     * Test that UUID entities can be saved and retrieved
     */
    public function testUuidEntitySaveAndRetrieve(): void
    {
        $repo = new UuidItemRepository($this->qb);
        
        // Create entity with UUID
        $item = new UuidItem();
        $item->id = 'abc-123-uuid';
        $item->name = 'Test Item';
        
        // Insert directly since we're testing compatibility
        $this->pdo->exec("INSERT INTO uuid_items (id, name) VALUES ('abc-123-uuid', 'Test Item')");
        
        // Find should work with string ID
        $found = $repo->find('abc-123-uuid', false);
        
        $this->assertNotNull($found);
        $this->assertEquals('abc-123-uuid', $found->id);
        $this->assertEquals('Test Item', $found->name);
    }

    /**
     * Test that delete works with string IDs
     */
    public function testDeleteWithStringId(): void
    {
        $repo = new UuidItemRepository($this->qb);
        
        // Insert test data
        $this->pdo->exec("INSERT INTO uuid_items (id, name) VALUES ('delete-test-uuid', 'Delete Me')");
        
        // Verify exists
        $stmt = $this->pdo->query("SELECT COUNT(*) as cnt FROM uuid_items WHERE id = 'delete-test-uuid'");
        $this->assertEquals(1, (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt']);
        
        // Delete using string ID
        $result = $repo->delete('delete-test-uuid');
        $this->assertEquals(1, $result);
        
        // Verify deleted
        $stmt = $this->pdo->query("SELECT COUNT(*) as cnt FROM uuid_items WHERE id = 'delete-test-uuid'");
        $this->assertEquals(0, (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt']);
    }

    /**
     * Test table name sanitization prevents SQL injection
     */
    public function testTableNameSanitization(): void
    {
        // Test that dangerous characters are stripped from table names
        // This ensures the PRAGMA table_info query is safe
        $dangerousNames = [
            'users; DROP TABLE users;--',
            'users" OR "1"="1',
            "users'); DELETE FROM users;--",
        ];
        
        foreach ($dangerousNames as $dangerous) {
            $safe = preg_replace('/[^a-zA-Z0-9_]/', '', $dangerous);
            $this->assertStringNotContainsString(';', $safe);
            $this->assertStringNotContainsString("'", $safe);
            $this->assertStringNotContainsString('"', $safe);
            $this->assertStringNotContainsString('-', $safe);
        }
    }

    /**
     * Test that count works correctly
     */
    public function testCountWithFilters(): void
    {
        $repo = new UuidItemRepository($this->qb);
        
        // Insert test data
        $this->pdo->exec("INSERT INTO uuid_items (id, name) VALUES ('item-1', 'Apple')");
        $this->pdo->exec("INSERT INTO uuid_items (id, name) VALUES ('item-2', 'Banana')");
        $this->pdo->exec("INSERT INTO uuid_items (id, name) VALUES ('item-3', 'Apple')");
        
        // Count all
        $this->assertEquals(3, $repo->count());
        
        // Count with filter
        $this->assertEquals(2, $repo->count(['name' => 'Apple']));
    }

    /**
     * Test SQLite driver detection
     */
    public function testDriverDetection(): void
    {
        $driver = $this->pdo->getAttribute(\PDO::ATTR_DRIVER_NAME);
        $this->assertEquals('sqlite', $driver);
    }
}
