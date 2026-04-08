<?php
declare(strict_types=1);

namespace Tests\Unit\Query;

use MonkeysLegion\Database\Enums\DatabaseDriver;
use MonkeysLegion\Entity\Attributes\Entity;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Query\Attributes\Scope;
use MonkeysLegion\Query\Compiler\MySqlGrammar;
use MonkeysLegion\Query\Compiler\PostgresGrammar;
use MonkeysLegion\Query\Compiler\SqliteGrammar;
use MonkeysLegion\Query\Exceptions\EntityNotFoundException;
use MonkeysLegion\Query\Exceptions\HydrationException;
use MonkeysLegion\Query\Exceptions\QueryException;
use MonkeysLegion\Query\Query\QueryBuilder;
use MonkeysLegion\Query\Repository\EntityHydrator;
use MonkeysLegion\Query\Repository\EntityRepository;
use MonkeysLegion\Query\Repository\SoftDeletes;
use PHPUnit\Framework\TestCase;
use Tests\Support\TestConnection;
use Tests\Support\TestConnectionManager;

/**
 * Phase 6: API Completeness Tests
 *
 * Tests for all new functionality:
 *   - Typed exceptions
 *   - Grammar: JSON contains, date extract
 *   - QueryBuilder: whereDate, whereJsonContains, whereNotBetween, increment, decrement
 *   - EntityHydrator: BackedEnum support
 *   - EntityRepository: update, updateWhere, deleteWhere, firstOrCreate, updateOrCreate,
 *     chunk, pluck, aggregates, configurable PK
 *   - SoftDeletes trait
 */
class Phase6Test extends TestCase
{
    private \PDO $pdo;
    private TestConnectionManager $manager;

    protected function setUp(): void
    {
        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        $this->pdo->exec('
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT \'active\',
                age INTEGER NOT NULL DEFAULT 25,
                views INTEGER NOT NULL DEFAULT 0,
                tags TEXT DEFAULT NULL,
                deleted_at TEXT DEFAULT NULL
            )
        ');

        $this->pdo->exec("INSERT INTO users (name, email, status, age, views, tags) VALUES
            ('Alice', 'alice@test.com', 'active', 30, 100, '[\"php\",\"js\"]'),
            ('Bob', 'bob@test.com', 'inactive', 25, 50, '[\"python\"]'),
            ('Charlie', 'charlie@test.com', 'active', 35, 200, '[\"php\",\"go\"]'),
            ('Diana', 'diana@test.com', 'active', 28, 75, NULL),
            ('Eve', 'eve@test.com', 'banned', 22, 0, '[]')
        ");

        $conn = new TestConnection($this->pdo);
        $this->manager = new TestConnectionManager($conn);
    }

    private function qb(): QueryBuilder
    {
        return (new QueryBuilder($this->manager))->from('users');
    }

    // ══════════════════════════════════════════════════════════════
    // ── Typed Exceptions ─────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testQueryExceptionIncludesSql(): void
    {
        $previous = new \PDOException('table not found');
        $e = new QueryException('SELECT * FROM missing', ['a'], $previous);

        self::assertStringContainsString('SELECT * FROM missing', $e->getMessage());
        self::assertSame('SELECT * FROM missing', $e->sql);
        self::assertSame(['a'], $e->bindings);
        self::assertSame($previous, $e->getPrevious());
    }

    public function testEntityNotFoundExceptionMessage(): void
    {
        $e = new EntityNotFoundException('App\\User', 42);

        self::assertStringContainsString('App\\User', $e->getMessage());
        self::assertStringContainsString('42', $e->getMessage());
        self::assertSame('App\\User', $e->entityClass);
        self::assertSame(42, $e->id);
    }

    public function testHydrationExceptionMessage(): void
    {
        $e = new HydrationException('App\\User', 'missing property "name"');

        self::assertStringContainsString('App\\User', $e->getMessage());
        self::assertStringContainsString('missing property', $e->getMessage());
        self::assertSame('App\\User', $e->entityClass);
    }

    public function testFindOrFailThrowsEntityNotFoundException(): void
    {
        $repo = new P6UserRepo($this->manager);

        $this->expectException(EntityNotFoundException::class);
        $repo->findOrFail(999);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Grammar: JSON Contains ───────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testMySqlJsonContains(): void
    {
        $result = (new MySqlGrammar())->compileJsonContains('tags');
        self::assertSame('JSON_CONTAINS(tags, ?)', $result);
    }

    public function testPostgresJsonContains(): void
    {
        $result = (new PostgresGrammar())->compileJsonContains('tags');
        self::assertSame('tags::jsonb @> ?::jsonb', $result);
    }

    public function testSqliteJsonContains(): void
    {
        $result = (new SqliteGrammar())->compileJsonContains('data');
        self::assertSame('EXISTS(SELECT 1 FROM json_each(data) WHERE value = ?)', $result);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Grammar: Date Extract ────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testMySqlDateExtract(): void
    {
        self::assertSame('DATE(created_at)', (new MySqlGrammar())->compileDateExtract('created_at'));
    }

    public function testPostgresDateExtract(): void
    {
        self::assertSame('created_at::date', (new PostgresGrammar())->compileDateExtract('created_at'));
    }

    public function testSqliteDateExtract(): void
    {
        self::assertSame('DATE(created_at)', (new SqliteGrammar())->compileDateExtract('created_at'));
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: whereDate ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereDateCompiles(): void
    {
        $compiled = $this->qb()
            ->whereDate('created_at', '=', '2026-01-15')
            ->compile();

        self::assertStringContainsString('DATE(created_at)', $compiled['sql']);
        self::assertContains('2026-01-15', $compiled['bindings']);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: whereJsonContains ───────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereJsonContainsCompiles(): void
    {
        $compiled = $this->qb()
            ->whereJsonContains('tags', 'php')
            ->compile();

        // SQLite grammar: json_each
        self::assertStringContainsString('json_each', $compiled['sql']);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: whereNotBetween ────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereNotBetweenCompiles(): void
    {
        $compiled = $this->qb()
            ->whereNotBetween('age', 20, 30)
            ->compile();

        self::assertStringContainsString('NOT BETWEEN', $compiled['sql']);
        self::assertContains(20, $compiled['bindings']);
        self::assertContains(30, $compiled['bindings']);
    }

    public function testWhereNotBetweenExecution(): void
    {
        $results = $this->qb()
            ->whereNotBetween('age', 26, 34)
            ->orderBy('name')
            ->get();

        // Alice(30) IN range, Bob(25) OUT, Charlie(35) OUT, Diana(28) IN, Eve(22) OUT
        // NOT BETWEEN 26 AND 34: Bob(25), Charlie(35), Eve(22)
        self::assertCount(3, $results);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: increment / decrement ──────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testIncrementSingleColumn(): void
    {
        $affected = $this->qb()
            ->where('name', '=', 'Alice')
            ->increment('views', 10);

        self::assertSame(1, $affected);

        $row = $this->qb()->where('name', '=', 'Alice')->first();
        self::assertSame(110, (int) $row['views']); // 100 + 10
    }

    public function testDecrementSingleColumn(): void
    {
        $affected = $this->qb()
            ->where('name', '=', 'Charlie')
            ->decrement('views', 50);

        self::assertSame(1, $affected);

        $row = $this->qb()->where('name', '=', 'Charlie')->first();
        self::assertSame(150, (int) $row['views']); // 200 - 50
    }

    public function testIncrementWithExtraColumns(): void
    {
        $this->qb()
            ->where('name', '=', 'Bob')
            ->increment('views', 1, ['status' => 'active']);

        $row = $this->qb()->where('name', '=', 'Bob')->first();
        self::assertSame(51, (int) $row['views']);
        self::assertSame('active', $row['status']);
    }

    public function testIncrementBulk(): void
    {
        $affected = $this->qb()
            ->where('status', '=', 'active')
            ->increment('views', 5);

        self::assertSame(3, $affected); // Alice, Charlie, Diana
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityHydrator: BackedEnum ───────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testHydrateBackedEnum(): void
    {
        $hydrator = new EntityHydrator();
        EntityHydrator::clearCache();

        $entity = $hydrator->hydrate(P6EnumEntity::class, [
            'id' => 1,
            'status' => 'active',
        ]);

        self::assertInstanceOf(P6Status::class, $entity->status);
        self::assertSame(P6Status::Active, $entity->status);
    }

    public function testDehydrateBackedEnum(): void
    {
        $hydrator = new EntityHydrator();
        EntityHydrator::clearCache();

        $entity = new P6EnumEntity();
        $entity->id = 1;
        $entity->status = P6Status::Banned;

        $data = $hydrator->dehydrate($entity);
        self::assertSame('banned', $data['status']);
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: update ─────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testUpdateById(): void
    {
        $repo = new P6UserRepo($this->manager);

        $repo->update(1, ['name' => 'Alicia', 'age' => 31]);

        $user = $repo->find(1);
        self::assertSame('Alicia', $user->name);
        self::assertSame(31, $user->age);
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: updateWhere ────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testUpdateWhere(): void
    {
        $repo = new P6UserRepo($this->manager);

        $affected = $repo->updateWhere(
            ['status' => 'inactive'],
            ['status' => 'active'],
        );

        self::assertSame(1, $affected); // Bob was inactive

        // Bob should now be active
        $bob = $repo->findOneBy(['name' => 'Bob']);
        self::assertSame('active', $bob->status);
    }

    public function testUpdateWhereWithArrayCriteria(): void
    {
        $repo = new P6UserRepo($this->manager);

        $affected = $repo->updateWhere(
            ['status' => ['active', 'inactive']],
            ['views' => 0],
        );

        self::assertSame(4, $affected); // Alice, Bob, Charlie, Diana
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: deleteWhere ────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testDeleteWhere(): void
    {
        $repo = new P6UserRepo($this->manager);

        $affected = $repo->deleteWhere(['status' => 'banned']);
        self::assertSame(1, $affected); // Eve

        self::assertSame(4, $repo->count());
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: firstOrCreate ──────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testFirstOrCreateFindsExisting(): void
    {
        $repo = new P6UserRepo($this->manager);

        $user = $repo->firstOrCreate(
            ['email' => 'alice@test.com'],
            ['name' => 'Should Not Create'],
        );

        self::assertSame('Alice', $user->name); // Found existing
        self::assertSame(5, $repo->count()); // No new insert
    }

    public function testFirstOrCreateCreatesNew(): void
    {
        $repo = new P6UserRepo($this->manager);

        $user = $repo->firstOrCreate(
            ['email' => 'frank@test.com'],
            ['name' => 'Frank', 'status' => 'active', 'age' => 40, 'views' => 0],
        );

        self::assertSame('Frank', $user->name);
        self::assertSame(6, $repo->count());
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: updateOrCreate ─────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testUpdateOrCreateUpdatesExisting(): void
    {
        $repo = new P6UserRepo($this->manager);

        $user = $repo->updateOrCreate(
            ['email' => 'alice@test.com'],
            ['name' => 'Alicia Updated'],
        );

        self::assertSame('Alicia Updated', $user->name);
        self::assertSame(5, $repo->count()); // No new insert
    }

    public function testUpdateOrCreateCreatesNew(): void
    {
        $repo = new P6UserRepo($this->manager);

        $user = $repo->updateOrCreate(
            ['email' => 'grace@test.com'],
            ['name' => 'Grace', 'status' => 'active', 'age' => 33, 'views' => 0],
        );

        self::assertSame('Grace', $user->name);
        self::assertSame(6, $repo->count());
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: pluck ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testPluckSingleColumn(): void
    {
        $repo = new P6UserRepo($this->manager);
        $names = $repo->pluck('name');

        self::assertCount(5, $names);
        self::assertContains('Alice', $names);
    }

    public function testPluckWithKey(): void
    {
        $repo = new P6UserRepo($this->manager);
        $map = $repo->pluck('name', 'email');

        self::assertArrayHasKey('alice@test.com', $map);
        self::assertSame('Alice', $map['alice@test.com']);
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: aggregates ─────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testSum(): void
    {
        $repo = new P6UserRepo($this->manager);
        $total = $repo->sum('views');
        self::assertSame(425.0, $total); // 100+50+200+75+0
    }

    public function testSumWithCriteria(): void
    {
        $repo = new P6UserRepo($this->manager);
        $total = $repo->sum('views', ['status' => 'active']);
        self::assertSame(375.0, $total); // 100+200+75
    }

    public function testAvg(): void
    {
        $repo = new P6UserRepo($this->manager);
        $avg = $repo->avg('age');
        self::assertSame(28.0, $avg); // (30+25+35+28+22)/5
    }

    public function testMin(): void
    {
        $repo = new P6UserRepo($this->manager);
        self::assertEquals(22, $repo->min('age'));
    }

    public function testMax(): void
    {
        $repo = new P6UserRepo($this->manager);
        self::assertEquals(35, $repo->max('age'));
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: chunk ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testChunkProcessesAllEntities(): void
    {
        $repo = new P6UserRepo($this->manager);
        $allNames = [];

        $repo->chunk(2, function (array $entities) use (&$allNames) {
            foreach ($entities as $entity) {
                $allNames[] = $entity->name;
            }
        });

        self::assertCount(5, $allNames);
    }

    public function testChunkEarlyStop(): void
    {
        $repo = new P6UserRepo($this->manager);
        $seen = 0;

        $repo->chunk(2, function (array $entities) use (&$seen) {
            $seen += count($entities);
            return false; // stop after first batch
        });

        self::assertSame(2, $seen);
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: configurable PK ────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testConfigurablePrimaryKey(): void
    {
        // Create a table with uuid as PK
        $this->pdo->exec('CREATE TABLE items (uuid TEXT PRIMARY KEY, label TEXT NOT NULL)');
        $this->pdo->exec("INSERT INTO items VALUES ('abc-123', 'Widget')");

        $repo = new P6UuidRepo($this->manager);
        $item = $repo->find('abc-123');

        self::assertNotNull($item);
        self::assertSame('Widget', $item->label);
    }

    // ══════════════════════════════════════════════════════════════
    // ── SoftDeletes trait ────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testSoftDeleteExcludesDeletedRows(): void
    {
        // Soft-delete Eve
        $this->pdo->exec("UPDATE users SET deleted_at = '2026-01-01 00:00:00' WHERE name = 'Eve'");

        $repo = new P6SoftDeleteRepo($this->manager);
        $all = $repo->findAll();

        self::assertCount(4, $all);
        foreach ($all as $user) {
            self::assertNotSame('Eve', $user->name);
        }
    }

    public function testSoftDeleteMethod(): void
    {
        $repo = new P6SoftDeleteRepo($this->manager);
        $repo->softDelete(1); // Alice

        // Alice should be hidden
        $all = $repo->findAll();
        self::assertCount(4, $all);

        // Alice should still exist in the DB
        $row = $this->pdo->query("SELECT deleted_at FROM users WHERE id = 1")->fetch();
        self::assertNotNull($row['deleted_at']);
    }

    public function testRestoreMethod(): void
    {
        $this->pdo->exec("UPDATE users SET deleted_at = '2026-01-01 00:00:00' WHERE id = 1");

        $repo = new P6SoftDeleteRepo($this->manager);
        $repo->restore(1);

        // Alice should be visible again
        $user = $repo->find(1);
        self::assertNotNull($user);
        self::assertSame('Alice', $user->name);
    }

    public function testTrashedReturnsTrue(): void
    {
        $this->pdo->exec("UPDATE users SET deleted_at = '2026-01-01 00:00:00' WHERE id = 2");

        $repo = new P6SoftDeleteRepo($this->manager);
        self::assertTrue($repo->trashed(2));
        self::assertFalse($repo->trashed(1));
    }

    public function testWithTrashedIncludesAll(): void
    {
        $this->pdo->exec("UPDATE users SET deleted_at = '2026-01-01 00:00:00' WHERE id = 1");

        $repo = new P6SoftDeleteRepo($this->manager);

        // Without trashed: 4
        self::assertCount(4, $repo->findAll());

        // With trashed: 5
        self::assertCount(5, $repo->withTrashed()->findAll());
    }

    public function testForceDelete(): void
    {
        $repo = new P6SoftDeleteRepo($this->manager);
        $affected = $repo->forceDelete(5); // Eve

        self::assertSame(1, $affected);

        // Eve should be completely gone
        $row = $this->pdo->query("SELECT COUNT(*) as cnt FROM users WHERE id = 5")->fetch();
        self::assertSame(0, (int) $row['cnt']);
    }

    public function testCountTrashed(): void
    {
        $this->pdo->exec("UPDATE users SET deleted_at = '2026-01-01' WHERE id IN (1, 2)");

        $repo = new P6SoftDeleteRepo($this->manager);
        self::assertSame(2, $repo->countTrashed());
    }

    public function testRestoreWhere(): void
    {
        $this->pdo->exec("UPDATE users SET deleted_at = '2026-01-01' WHERE status = 'active'");

        $repo = new P6SoftDeleteRepo($this->manager);

        // Only 2 visible (Bob=inactive, Eve=banned)
        self::assertCount(2, $repo->findAll());

        // Restore active users
        $restored = $repo->restoreWhere(['status' => 'active']);
        self::assertSame(3, $restored);

        // Now all 5 visible again
        self::assertCount(5, $repo->findAll());
    }

    public function testOnlyTrashedReturnsOnlyDeleted(): void
    {
        $this->pdo->exec("UPDATE users SET deleted_at = '2026-01-01' WHERE id IN (1, 3)");

        $repo = new P6SoftDeleteRepo($this->manager);

        // Normal: 3 visible (Bob, Diana, Eve)
        self::assertCount(3, $repo->findAll());

        // onlyTrashed: 2 (Alice, Charlie)
        $trashed = $repo->onlyTrashed()->findAll();
        self::assertCount(2, $trashed);

        $names = array_map(fn($u) => $u->name, $trashed);
        self::assertContains('Alice', $names);
        self::assertContains('Charlie', $names);
    }
}

// ══════════════════════════════════════════════════════════════════
// ── Test Entities & Repositories ─────────────────────────────────
// ══════════════════════════════════════════════════════════════════

enum P6Status: string
{
    case Active = 'active';
    case Inactive = 'inactive';
    case Banned = 'banned';
}

class P6EnumEntity
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public P6Status $status;
}

#[Entity(table: 'users')]
class P6User
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

    #[Field(type: 'integer')]
    public int $views;
}

class P6UserRepo extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = P6User::class;
}

#[Entity(table: 'items')]
class P6UuidItem
{
    #[Field(type: 'string')]
    public string $uuid;

    #[Field(type: 'string')]
    public string $label;
}

class P6UuidRepo extends EntityRepository
{
    protected string $table = 'items';
    protected string $entityClass = P6UuidItem::class;
    protected string $primaryKey = 'uuid';
}

class P6SoftDeleteRepo extends EntityRepository
{
    use SoftDeletes;

    protected string $table = 'users';
    protected string $entityClass = P6User::class;
}
