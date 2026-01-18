<?php

declare(strict_types=1);

namespace Tests\Integration;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Repository\EntityRepository;
use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Entity\Attributes\Entity;

/**
 * Test Entity: Author
 */
#[Entity(table: 'authors')]
class Author
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $name = '';

    #[Field(type: 'string', nullable: true)]
    public ?string $email = null;

    /** @var Book[] */
    #[OneToMany(targetEntity: Book::class, mappedBy: 'author')]
    public array $books = [];
}

/**
 * Test Entity: Book
 */
#[Entity(table: 'books')]
class Book
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $title = '';

    #[Field(type: 'int', nullable: true)]
    public ?int $year = null;

    #[ManyToOne(targetEntity: Author::class, inversedBy: 'books')]
    public ?Author $author = null;
}

/**
 * Test Repository: AuthorRepository
 */
class AuthorRepository extends EntityRepository
{
    protected string $table = 'authors';
    protected string $entityClass = Author::class;
}

/**
 * Test Repository: BookRepository
 */
class BookRepository extends EntityRepository
{
    protected string $table = 'books';
    protected string $entityClass = Book::class;
}

/**
 * Integration tests for OneToMany and ManyToOne relationships.
 */
class OneToManyTest extends TestCase
{
    private QueryBuilder $qb;
    private AuthorRepository $authorRepo;
    private BookRepository $bookRepo;
    private \PDO $pdo;

    protected function setUp(): void
    {
        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        // Create test tables
        $this->pdo->exec('
            CREATE TABLE authors (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT
            )
        ');

        $this->pdo->exec('
            CREATE TABLE books (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                year INTEGER,
                author_id INTEGER,
                FOREIGN KEY (author_id) REFERENCES authors(id)
            )
        ');

        // Insert test data
        $this->pdo->exec("INSERT INTO authors (name, email) VALUES ('John Doe', 'john@example.com')");
        $this->pdo->exec("INSERT INTO authors (name, email) VALUES ('Jane Smith', 'jane@example.com')");
        $this->pdo->exec("INSERT INTO books (title, year, author_id) VALUES ('Book One', 2020, 1)");
        $this->pdo->exec("INSERT INTO books (title, year, author_id) VALUES ('Book Two', 2021, 1)");
        $this->pdo->exec("INSERT INTO books (title, year, author_id) VALUES ('Book Three', 2022, 2)");

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
        $this->authorRepo = new AuthorRepository($this->qb);
        $this->bookRepo = new BookRepository($this->qb);
    }

    protected function tearDown(): void
    {
        $this->pdo->exec('DELETE FROM books');
        $this->pdo->exec('DELETE FROM authors');
    }

    // ==================== FIND TESTS ====================

    public function testFindById(): void
    {
        $author = $this->authorRepo->find(1, false);
        
        $this->assertNotNull($author);
        $this->assertEquals(1, $author->id);
        $this->assertEquals('John Doe', $author->name);
        $this->assertEquals('john@example.com', $author->email);
    }

    public function testFindByIdReturnsNullForNonExistent(): void
    {
        $author = $this->authorRepo->find(999, false);
        
        $this->assertNull($author);
    }

    public function testFindOneBy(): void
    {
        $author = $this->authorRepo->findOneBy(['name' => 'Jane Smith'], false);
        
        $this->assertNotNull($author);
        $this->assertEquals(2, $author->id);
        $this->assertEquals('Jane Smith', $author->name);
    }

    public function testFindByWithCriteria(): void
    {
        $books = $this->bookRepo->findBy(['author_id' => 1], [], null, null, false);
        
        $this->assertCount(2, $books);
        $titles = array_map(fn($b) => $b->title, $books);
        $this->assertContains('Book One', $titles);
        $this->assertContains('Book Two', $titles);
    }

    public function testFindByWithOrderBy(): void
    {
        $books = $this->bookRepo->findBy([], ['year' => 'DESC'], null, null, false);
        
        $this->assertCount(3, $books);
        $this->assertEquals('Book Three', $books[0]->title);
        $this->assertEquals('Book Two', $books[1]->title);
        $this->assertEquals('Book One', $books[2]->title);
    }

    public function testFindByWithLimitAndOffset(): void
    {
        $books = $this->bookRepo->findBy([], ['id' => 'ASC'], 2, 0, false);
        
        $this->assertCount(2, $books);
        $this->assertEquals('Book One', $books[0]->title);
        $this->assertEquals('Book Two', $books[1]->title);
    }

    public function testFindAll(): void
    {
        $authors = $this->authorRepo->findAll([], false);
        
        $this->assertCount(2, $authors);
    }

    // ==================== SAVE TESTS ====================

    public function testSaveNewEntity(): void
    {
        $author = new Author();
        $author->name = 'New Author';
        $author->email = 'new@example.com';
        
        $id = $this->authorRepo->save($author);
        
        $this->assertGreaterThan(0, $id);
        $this->assertEquals($id, $author->id);
        
        // Verify in database
        $stmt = $this->pdo->query("SELECT * FROM authors WHERE id = {$id}");
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $this->assertEquals('New Author', $row['name']);
        $this->assertEquals('new@example.com', $row['email']);
    }

    public function testSaveExistingEntity(): void
    {
        $author = $this->authorRepo->find(1, false);
        $author->name = 'Updated Name';
        
        $this->authorRepo->save($author);
        
        // Verify update
        $reloaded = $this->authorRepo->find(1, false);
        $this->assertEquals('Updated Name', $reloaded->name);
    }

    public function testSaveWithNullableField(): void
    {
        $author = new Author();
        $author->name = 'No Email';
        $author->email = null;
        
        $id = $this->authorRepo->save($author);
        
        $reloaded = $this->authorRepo->find($id, false);
        $this->assertNull($reloaded->email);
    }

    // ==================== DELETE TESTS ====================

    public function testDeleteById(): void
    {
        $result = $this->authorRepo->delete(2);
        
        $this->assertEquals(1, $result);
        
        // Verify deleted
        $author = $this->authorRepo->find(2, false);
        $this->assertNull($author);
    }

    public function testDeleteByEntity(): void
    {
        $author = $this->authorRepo->find(2, false);
        $result = $this->authorRepo->delete($author);
        
        $this->assertEquals(1, $result);
        
        // Verify deleted
        $author = $this->authorRepo->find(2, false);
        $this->assertNull($author);
    }

    public function testDeleteNonExistentReturnsZero(): void
    {
        $result = $this->authorRepo->delete(999);
        
        $this->assertEquals(0, $result);
    }

    // ==================== COUNT TESTS ====================

    public function testCount(): void
    {
        $count = $this->authorRepo->count();
        
        $this->assertEquals(2, $count);
    }

    public function testCountWithCriteria(): void
    {
        $count = $this->bookRepo->count(['author_id' => 1]);
        
        $this->assertEquals(2, $count);
    }

    // ==================== RELATION TESTS ====================

    public function testLoadOneToManyRelation(): void
    {
        $author = $this->authorRepo->find(1, true);
        
        $this->assertNotNull($author);
        $this->assertIsArray($author->books);
        $this->assertCount(2, $author->books);
        
        $titles = array_map(fn($b) => $b->title, $author->books);
        $this->assertContains('Book One', $titles);
        $this->assertContains('Book Two', $titles);
    }

    public function testLoadManyToOneRelation(): void
    {
        $book = $this->bookRepo->find(1, true);
        
        $this->assertNotNull($book);
        $this->assertNotNull($book->author);
        $this->assertEquals('John Doe', $book->author->name);
    }

    public function testEmptyOneToManyReturnsEmptyArray(): void
    {
        // Create an author with no books
        $author = new Author();
        $author->name = 'No Books';
        $id = $this->authorRepo->save($author);
        
        $reloaded = $this->authorRepo->find($id, true);
        
        $this->assertIsArray($reloaded->books);
        $this->assertCount(0, $reloaded->books);
    }

    public function testManyToOneNullRelation(): void
    {
        // Create a book with no author
        $this->pdo->exec("INSERT INTO books (title, year, author_id) VALUES ('Orphan Book', 2023, NULL)");
        
        $book = $this->bookRepo->findOneBy(['title' => 'Orphan Book'], true);
        
        $this->assertNotNull($book);
        $this->assertNull($book->author);
    }

    // ==================== EDGE CASES ====================

    public function testFindByEmptyCriteria(): void
    {
        $authors = $this->authorRepo->findBy([], [], null, null, false);
        
        $this->assertCount(2, $authors);
    }

    public function testSavePreservesUnchangedFields(): void
    {
        $author = $this->authorRepo->find(1, false);
        $originalEmail = $author->email;
        $author->name = 'Changed Name';
        
        $this->authorRepo->save($author);
        
        $reloaded = $this->authorRepo->find(1, false);
        $this->assertEquals('Changed Name', $reloaded->name);
        $this->assertEquals($originalEmail, $reloaded->email);
    }

    public function testMultipleSavesOnSameEntity(): void
    {
        $author = new Author();
        $author->name = 'First Save';
        $id = $this->authorRepo->save($author);
        
        $author->name = 'Second Save';
        $this->authorRepo->save($author);
        
        $author->name = 'Third Save';
        $this->authorRepo->save($author);
        
        $reloaded = $this->authorRepo->find($id, false);
        $this->assertEquals('Third Save', $reloaded->name);
        
        // Should still be same record
        $this->assertEquals($id, $reloaded->id);
    }
}
