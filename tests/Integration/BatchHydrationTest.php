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
use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\JoinTable;
use MonkeysLegion\Entity\Attributes\Entity;

// ──────────────────────────────────────────────────────────────
//  Test entities (unique namespace-safe names to avoid collisions
//  with other integration test files)
// ──────────────────────────────────────────────────────────────

#[Entity(table: 'batch_publishers')]
class BatchPublisher
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $name = '';

    /** @var BatchArticle[] */
    #[OneToMany(targetEntity: BatchArticle::class, mappedBy: 'publisher')]
    public array $articles = [];

    /** @var BatchCategory[] */
    #[ManyToMany(
        targetEntity: BatchCategory::class,
        inversedBy: 'publishers',
        joinTable: new JoinTable(
            name: 'batch_publisher_category',
            joinColumn: 'publisher_id',
            inverseColumn: 'category_id'
        )
    )]
    public array $categories = [];
}

#[Entity(table: 'batch_articles')]
class BatchArticle
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $title = '';

    #[ManyToOne(targetEntity: BatchPublisher::class, inversedBy: 'articles')]
    public ?BatchPublisher $publisher = null;
}

#[Entity(table: 'batch_categories')]
class BatchCategory
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $label = '';

    /** @var BatchPublisher[] */
    #[ManyToMany(targetEntity: BatchPublisher::class, mappedBy: 'categories')]
    public array $publishers = [];
}

class BatchPublisherRepository extends EntityRepository
{
    protected string $table = 'batch_publishers';
    protected string $entityClass = BatchPublisher::class;
}

class BatchArticleRepository extends EntityRepository
{
    protected string $table = 'batch_articles';
    protected string $entityClass = BatchArticle::class;
}

class BatchCategoryRepository extends EntityRepository
{
    protected string $table = 'batch_categories';
    protected string $entityClass = BatchCategory::class;
}

/**
 * Integration tests for batched relation loading via findBy / findAll.
 *
 * These tests verify the batch hydration code paths that were added to
 * eliminate N+1 queries when loading ManyToOne, OneToMany, and ManyToMany
 * relations for collections of entities.
 */
class BatchHydrationTest extends TestCase
{
    private \PDO $pdo;
    private QueryBuilder $qb;
    private BatchPublisherRepository $publisherRepo;
    private BatchArticleRepository $articleRepo;
    private BatchCategoryRepository $categoryRepo;

    protected function setUp(): void
    {
        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $this->pdo->exec('
            CREATE TABLE batch_publishers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
        ');

        $this->pdo->exec('
            CREATE TABLE batch_articles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL,
                publisher_id INTEGER,
                FOREIGN KEY (publisher_id) REFERENCES batch_publishers(id)
            )
        ');

        $this->pdo->exec('
            CREATE TABLE batch_categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                label TEXT NOT NULL
            )
        ');

        $this->pdo->exec('
            CREATE TABLE batch_publisher_category (
                publisher_id INTEGER NOT NULL,
                category_id INTEGER NOT NULL,
                PRIMARY KEY (publisher_id, category_id),
                FOREIGN KEY (publisher_id) REFERENCES batch_publishers(id),
                FOREIGN KEY (category_id) REFERENCES batch_categories(id)
            )
        ');

        // Seed data: 3 publishers, 5 articles, 3 categories, pivot entries
        $this->pdo->exec("INSERT INTO batch_publishers (name) VALUES ('Pub A'), ('Pub B'), ('Pub C')");
        $this->pdo->exec("INSERT INTO batch_articles (title, publisher_id) VALUES
            ('Art 1', 1), ('Art 2', 1), ('Art 3', 2), ('Art 4', 2), ('Art 5', NULL)");
        $this->pdo->exec("INSERT INTO batch_categories (label) VALUES ('Cat X'), ('Cat Y'), ('Cat Z')");
        $this->pdo->exec("INSERT INTO batch_publisher_category (publisher_id, category_id) VALUES
            (1, 1), (1, 2), (2, 2), (2, 3)");

        $pdo  = $this->pdo;
        $conn = new class($pdo) implements ConnectionInterface {
            public function __construct(private \PDO $pdo) {}
            public function pdo(): \PDO { return $this->pdo; }
            public function connect(): void {}
            public function disconnect(): void {}
            public function isConnected(): bool { return true; }
            public function getDsn(): string { return ''; }
            public function isAlive(): bool { return true; }
        };

        $this->qb            = new QueryBuilder($conn);
        $this->publisherRepo = new BatchPublisherRepository($this->qb);
        $this->articleRepo   = new BatchArticleRepository($this->qb);
        $this->categoryRepo  = new BatchCategoryRepository($this->qb);
    }

    protected function tearDown(): void
    {
        $this->pdo->exec('DELETE FROM batch_publisher_category');
        $this->pdo->exec('DELETE FROM batch_articles');
        $this->pdo->exec('DELETE FROM batch_categories');
        $this->pdo->exec('DELETE FROM batch_publishers');
    }

    // ==================== findBy — ManyToOne batch ====================

    public function testFindByBatchesManyToOneRelations(): void
    {
        // findBy should batch-load the publisher relation for all articles
        $articles = $this->articleRepo->findBy([], ['id' => 'ASC'], null, null, true);

        $this->assertCount(5, $articles);

        // Articles 1 & 2 → Pub A
        $this->assertNotNull($articles[0]->publisher);
        $this->assertEquals('Pub A', $articles[0]->publisher->name);
        $this->assertNotNull($articles[1]->publisher);
        $this->assertEquals('Pub A', $articles[1]->publisher->name);

        // Articles 3 & 4 → Pub B
        $this->assertNotNull($articles[2]->publisher);
        $this->assertEquals('Pub B', $articles[2]->publisher->name);
        $this->assertNotNull($articles[3]->publisher);
        $this->assertEquals('Pub B', $articles[3]->publisher->name);

        // Article 5 → no publisher
        $this->assertNull($articles[4]->publisher);

        // Object identity: articles sharing same publisher should get the same instance
        $this->assertSame($articles[0]->publisher, $articles[1]->publisher);
        $this->assertSame($articles[2]->publisher, $articles[3]->publisher);
    }

    // ==================== findAll — OneToMany batch ====================

    public function testFindAllBatchesOneToManyRelations(): void
    {
        $publishers = $this->publisherRepo->findAll([], true);

        $this->assertCount(3, $publishers);

        // Sort by id for deterministic ordering
        usort($publishers, fn($a, $b) => $a->id <=> $b->id);

        // Pub A → 2 articles
        $this->assertIsArray($publishers[0]->articles);
        $this->assertCount(2, $publishers[0]->articles);

        // Pub B → 2 articles
        $this->assertIsArray($publishers[1]->articles);
        $this->assertCount(2, $publishers[1]->articles);

        // Pub C → 0 articles
        $this->assertIsArray($publishers[2]->articles);
        $this->assertCount(0, $publishers[2]->articles);
    }

    // ==================== findAll — ManyToMany batch ====================

    public function testFindAllBatchesManyToManyRelations(): void
    {
        $publishers = $this->publisherRepo->findAll([], true);

        usort($publishers, fn($a, $b) => $a->id <=> $b->id);

        // Pub A → Cat X, Cat Y
        $this->assertIsArray($publishers[0]->categories);
        $this->assertCount(2, $publishers[0]->categories);
        $catIds = array_map(fn($c) => $c->id, $publishers[0]->categories);
        sort($catIds);
        $this->assertEquals([1, 2], $catIds);

        // Pub B → Cat Y, Cat Z
        $this->assertIsArray($publishers[1]->categories);
        $this->assertCount(2, $publishers[1]->categories);
        $catIds = array_map(fn($c) => $c->id, $publishers[1]->categories);
        sort($catIds);
        $this->assertEquals([2, 3], $catIds);

        // Pub C → none
        $this->assertIsArray($publishers[2]->categories);
        $this->assertCount(0, $publishers[2]->categories);
    }

    // ==================== findBy with criteria — batch still works ====================

    public function testFindByWithCriteriaBatchesRelations(): void
    {
        // Only fetch articles belonging to publisher 1
        $articles = $this->articleRepo->findBy(
            ['publisher_id' => 1],
            ['id' => 'ASC'],
            null,
            null,
            true
        );

        $this->assertCount(2, $articles);
        $this->assertNotNull($articles[0]->publisher);
        $this->assertEquals('Pub A', $articles[0]->publisher->name);
        $this->assertSame($articles[0]->publisher, $articles[1]->publisher);
    }

    // ==================== findBy with limit — batch works for subset ====================

    public function testFindByWithLimitBatchesRelations(): void
    {
        $articles = $this->articleRepo->findBy([], ['id' => 'ASC'], 2, 0, true);

        $this->assertCount(2, $articles);
        // Both should have publisher loaded
        $this->assertNotNull($articles[0]->publisher);
        $this->assertNotNull($articles[1]->publisher);
    }

    // ==================== findAll with no relations loaded ====================

    public function testFindAllWithoutRelationsDoesNotLoad(): void
    {
        $articles = $this->articleRepo->findAll([], false);

        $this->assertCount(5, $articles);
        // Publisher should NOT be loaded when loadRelations=false
        foreach ($articles as $article) {
            $this->assertNull($article->publisher);
        }
    }
}
