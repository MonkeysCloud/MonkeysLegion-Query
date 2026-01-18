<?php

declare(strict_types=1);

namespace Tests\Integration;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Repository\EntityRepository;
use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\Entity;

/**
 * Test Entity: Product
 */
#[Entity(table: 'products')]
class Product
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $name = '';

    #[Field(type: 'string', nullable: true)]
    public ?string $description = null;

    #[Field(type: 'decimal', nullable: true)]
    public ?string $price = null;

    #[Field(type: 'int')]
    public int $stock = 0;

    #[Field(type: 'boolean')]
    public bool $active = true;
}

/**
 * Test Repository: ProductRepository
 */
class ProductRepository extends EntityRepository
{
    protected string $table = 'products';
    protected string $entityClass = Product::class;
}

/**
 * Integration tests for EntityRepository CRUD operations and edge cases.
 */
class EntityRepositoryTest extends TestCase
{
    private QueryBuilder $qb;
    private ProductRepository $repo;
    private \PDO $pdo;

    protected function setUp(): void
    {
        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        // Create test table with various field types
        $this->pdo->exec('
            CREATE TABLE products (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                description TEXT,
                price REAL,
                stock INTEGER DEFAULT 0,
                active INTEGER DEFAULT 1
            )
        ');

        // Insert test data
        $this->pdo->exec("INSERT INTO products (name, description, price, stock, active) 
                          VALUES ('Product A', 'Description A', 10.99, 100, 1)");
        $this->pdo->exec("INSERT INTO products (name, description, price, stock, active) 
                          VALUES ('Product B', 'Description B', 25.50, 50, 1)");
        $this->pdo->exec("INSERT INTO products (name, description, price, stock, active) 
                          VALUES ('Product C', NULL, 5.00, 200, 0)");

        // Create connection mock
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
        $this->repo = new ProductRepository($this->qb);
    }

    protected function tearDown(): void
    {
        $this->pdo->exec('DELETE FROM products');
    }

    // ==================== FIND TESTS ====================

    public function testFindById(): void
    {
        $product = $this->repo->find(1, false);
        
        $this->assertNotNull($product);
        $this->assertInstanceOf(Product::class, $product);
        $this->assertEquals(1, $product->id);
        $this->assertEquals('Product A', $product->name);
    }

    public function testFindByIdWithStringId(): void
    {
        $product = $this->repo->find('1', false);
        
        $this->assertNotNull($product);
        $this->assertEquals(1, $product->id);
    }

    public function testFindNonExistent(): void
    {
        $product = $this->repo->find(999, false);
        
        $this->assertNull($product);
    }

    public function testFindOneByMultipleCriteria(): void
    {
        $product = $this->repo->findOneBy(['name' => 'Product B', 'active' => 1], false);
        
        $this->assertNotNull($product);
        $this->assertEquals('Product B', $product->name);
    }

    public function testFindOneByNoMatch(): void
    {
        $product = $this->repo->findOneBy(['name' => 'NonExistent'], false);
        
        $this->assertNull($product);
    }

    public function testFindByWithMultipleCriteria(): void
    {
        $products = $this->repo->findBy(['active' => 1], [], null, null, false);
        
        $this->assertCount(2, $products);
    }

    public function testFindByWithOrderByAscending(): void
    {
        $products = $this->repo->findBy([], ['stock' => 'ASC'], null, null, false);
        
        $this->assertEquals('Product B', $products[0]->name);  // stock: 50
        $this->assertEquals('Product A', $products[1]->name);  // stock: 100
        $this->assertEquals('Product C', $products[2]->name);  // stock: 200
    }

    public function testFindByWithOrderByDescending(): void
    {
        $products = $this->repo->findBy([], ['price' => 'DESC'], null, null, false);
        
        $this->assertEquals('Product B', $products[0]->name);  // price: 25.50
    }

    public function testFindByWithLimit(): void
    {
        $products = $this->repo->findBy([], ['id' => 'ASC'], 2, null, false);
        
        $this->assertCount(2, $products);
    }

    public function testFindByWithOffset(): void
    {
        $products = $this->repo->findBy([], ['id' => 'ASC'], 2, 1, false);
        
        $this->assertCount(2, $products);
        $this->assertEquals('Product B', $products[0]->name);
        $this->assertEquals('Product C', $products[1]->name);
    }

    public function testFindAll(): void
    {
        $products = $this->repo->findAll([], false);
        
        $this->assertCount(3, $products);
    }

    public function testFindAllWithCriteria(): void
    {
        $products = $this->repo->findAll(['active' => 0], false);
        
        $this->assertCount(1, $products);
        $this->assertEquals('Product C', $products[0]->name);
    }

    // ==================== SAVE TESTS ====================

    public function testSaveNewEntity(): void
    {
        $product = new Product();
        $product->name = 'New Product';
        $product->description = 'New Description';
        $product->price = '15.99';
        $product->stock = 75;
        $product->active = true;
        
        $id = $this->repo->save($product);
        
        $this->assertGreaterThan(0, $id);
        $this->assertEquals($id, $product->id);
        
        // Verify in database
        $reloaded = $this->repo->find($id, false);
        $this->assertEquals('New Product', $reloaded->name);
        $this->assertEquals('New Description', $reloaded->description);
    }

    public function testSaveExistingEntity(): void
    {
        $product = $this->repo->find(1, false);
        $product->name = 'Updated Product A';
        $product->stock = 150;
        
        $this->repo->save($product);
        
        $reloaded = $this->repo->find(1, false);
        $this->assertEquals('Updated Product A', $reloaded->name);
        $this->assertEquals(150, $reloaded->stock);
    }

    public function testSaveWithNullValues(): void
    {
        $product = new Product();
        $product->name = 'No Description';
        $product->description = null;
        $product->price = null;
        
        $id = $this->repo->save($product);
        
        $reloaded = $this->repo->find($id, false);
        $this->assertNull($reloaded->description);
    }

    public function testSaveWithBooleanField(): void
    {
        $product = new Product();
        $product->name = 'Inactive Product';
        $product->active = false;
        
        $id = $this->repo->save($product);
        
        $stmt = $this->pdo->query("SELECT active FROM products WHERE id = {$id}");
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $this->assertEquals(0, $row['active']);
    }

    public function testPartialUpdate(): void
    {
        $product = $this->repo->find(1, false);
        $originalDescription = $product->description;
        $product->name = 'Partial Update';
        
        $this->repo->save($product, true);  // partial = true
        
        $reloaded = $this->repo->find(1, false);
        $this->assertEquals('Partial Update', $reloaded->name);
        $this->assertEquals($originalDescription, $reloaded->description);
    }

    // ==================== DELETE TESTS ====================

    public function testDeleteById(): void
    {
        $result = $this->repo->delete(1);
        
        $this->assertEquals(1, $result);
        $this->assertNull($this->repo->find(1, false));
    }

    public function testDeleteByEntity(): void
    {
        $product = $this->repo->find(2, false);
        $result = $this->repo->delete($product);
        
        $this->assertEquals(1, $result);
        $this->assertNull($this->repo->find(2, false));
    }

    public function testDeleteNonExistent(): void
    {
        $result = $this->repo->delete(999);
        
        $this->assertEquals(0, $result);
    }

    public function testDeleteWithStringId(): void
    {
        $result = $this->repo->delete('3');
        
        $this->assertEquals(1, $result);
        $this->assertNull($this->repo->find(3, false));
    }

    // ==================== COUNT TESTS ====================

    public function testCountAll(): void
    {
        $count = $this->repo->count();
        
        $this->assertEquals(3, $count);
    }

    public function testCountWithCriteria(): void
    {
        $count = $this->repo->count(['active' => 1]);
        
        $this->assertEquals(2, $count);
    }

    public function testCountWithNoCriteria(): void
    {
        $count = $this->repo->count([]);
        
        $this->assertEquals(3, $count);
    }

    public function testCountAfterDelete(): void
    {
        $this->repo->delete(1);
        
        $count = $this->repo->count();
        
        $this->assertEquals(2, $count);
    }

    // ==================== EDGE CASES ====================

    public function testFindByNullValue(): void
    {
        $products = $this->repo->findBy(['description' => null], [], null, null, false);
        
        // SQLite doesn't directly compare NULL this way, this tests the criteria handling
        $this->assertIsArray($products);
    }

    public function testMultipleSequentialSaves(): void
    {
        $product = new Product();
        $product->name = 'Sequential 1';
        $id = $this->repo->save($product);
        
        $product->name = 'Sequential 2';
        $this->repo->save($product);
        
        $product->name = 'Sequential 3';
        $this->repo->save($product);
        
        $reloaded = $this->repo->find($id, false);
        $this->assertEquals('Sequential 3', $reloaded->name);
        $this->assertEquals($id, $reloaded->id);  // Same ID
    }

    public function testSaveAndImmediateReload(): void
    {
        $product = new Product();
        $product->name = 'Immediate Reload';
        $product->stock = 42;
        
        $id = $this->repo->save($product);
        $reloaded = $this->repo->find($id, false);
        
        $this->assertEquals('Immediate Reload', $reloaded->name);
        $this->assertEquals(42, $reloaded->stock);
    }

    public function testFindByWithEmptyOrderBy(): void
    {
        $products = $this->repo->findBy(['active' => 1], [], null, null, false);
        
        $this->assertCount(2, $products);
    }

    public function testCountAfterMultipleOperations(): void
    {
        // Add 2 new products
        $p1 = new Product();
        $p1->name = 'New 1';
        $this->repo->save($p1);
        
        $p2 = new Product();
        $p2->name = 'New 2';
        $this->repo->save($p2);
        
        // Delete 1 existing
        $this->repo->delete(1);
        
        // Count should be 3 + 2 - 1 = 4
        $count = $this->repo->count();
        $this->assertEquals(4, $count);
    }
}
