<?php
declare(strict_types=1);

namespace Tests\Unit\Repository;

use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Query\Query\QueryBuilder;
use MonkeysLegion\Query\Repository\EntityHydrator;
use MonkeysLegion\Query\Repository\EntityRepository;
use PDO;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Tests\Support\TestConnection;
use Tests\Support\TestConnectionManager;

// ── Test Entity ──────────────────────────────────────────────────

class RepoTestUser
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public string $name;

    #[Field(type: 'string')]
    public string $email;

    #[Field(type: 'string')]
    public string $status;

    #[Field(type: 'integer')]
    public int $age;
}

// ── Test Repository ──────────────────────────────────────────────

class UserTestRepository extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = RepoTestUser::class;
}

#[CoversClass(EntityRepository::class)]
final class EntityRepositoryTest extends TestCase
{
    private UserTestRepository $repo;
    private PDO $pdo;

    protected function setUp(): void
    {
        QueryBuilder::clearStatementCache();
        EntityHydrator::clearCache();

        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        $this->pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, status TEXT, age INTEGER)');
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Alice', 'alice@test.com', 'active', 30)");
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Bob', 'bob@test.com', 'inactive', 25)");
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Charlie', 'charlie@test.com', 'active', 35)");

        $conn = new TestConnection($this->pdo);
        $manager = new TestConnectionManager($conn);
        $this->repo = new UserTestRepository($manager);
    }

    // ── Find ────────────────────────────────────────────────────

    public function testFind(): void
    {
        $user = $this->repo->find(1);
        self::assertNotNull($user);
        self::assertInstanceOf(RepoTestUser::class, $user);
        self::assertSame(1, $user->id);
        self::assertSame('Alice', $user->name);
    }

    public function testFindReturnsNullForMissing(): void
    {
        self::assertNull($this->repo->find(999));
    }

    public function testFindOrFail(): void
    {
        $user = $this->repo->findOrFail(1);
        self::assertSame('Alice', $user->name);
    }

    public function testFindOrFailThrows(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->repo->findOrFail(999);
    }

    public function testIdentityMapReturnsSameInstance(): void
    {
        $user1 = $this->repo->find(1);
        $user2 = $this->repo->find(1);

        self::assertSame($user1, $user2, 'Identity map should return same instance');
    }

    // ── FindBy ──────────────────────────────────────────────────

    public function testFindOneBy(): void
    {
        $user = $this->repo->findOneBy(['email' => 'bob@test.com']);
        self::assertNotNull($user);
        self::assertSame('Bob', $user->name);
    }

    public function testFindBy(): void
    {
        $users = $this->repo->findBy(['status' => 'active'], ['name' => 'ASC']);
        self::assertCount(2, $users);
        self::assertSame('Alice', $users[0]->name);
        self::assertSame('Charlie', $users[1]->name);
    }

    public function testFindByWithLimit(): void
    {
        $users = $this->repo->findBy([], ['id' => 'ASC'], limit: 2);
        self::assertCount(2, $users);
    }

    public function testFindAll(): void
    {
        $all = $this->repo->findAll();
        self::assertCount(3, $all);
    }

    // ── Batch Loading ───────────────────────────────────────────

    public function testFindByIds(): void
    {
        $users = $this->repo->findByIds([3, 1]);
        self::assertCount(2, $users);
        self::assertSame('Charlie', $users[0]->name);
        self::assertSame('Alice', $users[1]->name);
    }

    public function testFindByIdsUsesIdentityMap(): void
    {
        // Pre-load one entity
        $alice = $this->repo->find(1);

        // Now batch-load including the pre-loaded one
        $users = $this->repo->findByIds([1, 2]);
        self::assertSame($alice, $users[0], 'Should reuse identity map instance');
        self::assertCount(2, $users);
    }

    // ── Pagination ──────────────────────────────────────────────

    public function testPaginate(): void
    {
        $page = $this->repo->paginate(page: 1, perPage: 2);
        self::assertSame(3, $page['total']);
        self::assertCount(2, $page['data']);
        self::assertSame(1, $page['page']);
        self::assertSame(2, $page['perPage']);
        self::assertSame(2, $page['lastPage']);
    }

    public function testCursorPaginate(): void
    {
        // First page
        $page1 = $this->repo->cursorPaginate(cursor: null, perPage: 2);
        self::assertCount(2, $page1['data']);
        self::assertTrue($page1['hasMore']);
        self::assertNotNull($page1['nextCursor']);

        // Second page
        $page2 = $this->repo->cursorPaginate(cursor: $page1['nextCursor'], perPage: 2);
        self::assertCount(1, $page2['data']);
        self::assertFalse($page2['hasMore']);
    }

    // ── Create & Delete ─────────────────────────────────────────

    public function testCreate(): void
    {
        $user = $this->repo->create([
            'name'   => 'Diana',
            'email'  => 'diana@test.com',
            'status' => 'active',
            'age'    => 28,
        ]);

        self::assertInstanceOf(RepoTestUser::class, $user);
        self::assertSame(4, $this->repo->count());
    }

    public function testDelete(): void
    {
        $this->repo->delete(2);
        self::assertSame(2, $this->repo->count());
        self::assertNull($this->repo->find(2));
    }

    // ── Aggregates ──────────────────────────────────────────────

    public function testCount(): void
    {
        self::assertSame(3, $this->repo->count());
        self::assertSame(2, $this->repo->count(['status' => 'active']));
    }

    public function testExists(): void
    {
        self::assertTrue($this->repo->exists(['name' => 'Alice']));
        self::assertFalse($this->repo->exists(['name' => 'Nobody']));
    }

    // ── Identity Map Clear ──────────────────────────────────────

    public function testClear(): void
    {
        $this->repo->find(1);
        self::assertSame(1, $this->repo->getIdentityMap()->count());

        $this->repo->clear();
        self::assertSame(0, $this->repo->getIdentityMap()->count());
    }
}
