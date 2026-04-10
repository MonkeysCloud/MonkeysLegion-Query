<?php
declare(strict_types=1);

namespace Tests\Performance;

use MonkeysLegion\Entity\Attributes\Entity;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use MonkeysLegion\Query\Attributes\Scope;
use MonkeysLegion\Query\Compiler\QueryCompiler;
use MonkeysLegion\Query\Query\QueryBuilder;
use MonkeysLegion\Query\RawExpression;
use MonkeysLegion\Query\Repository\EntityHydrator;
use MonkeysLegion\Query\Repository\EntityRepository;
use MonkeysLegion\Query\Repository\IdentityMap;
use MonkeysLegion\Query\Repository\RelationLoader;
use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Support\TestConnection;
use Tests\Support\TestConnectionManager;

/**
 * Performance benchmark tests — validates the architectural performance
 * guarantees of the query builder, compiler, hydrator, and repository.
 *
 * These tests assert concrete metrics:
 *   • Execution time budgets (generous upper bounds)
 *   • Cache hit verification (structural SQL, identity map, reflection)
 *   • Zero-overhead conditional methods
 *   • Correctness of batch strategies (N+1 elimination)
 *   • Constant-memory iteration (chunk/lazy)
 *
 * The time budgets are deliberately generous (5-10x headroom) so they
 * pass reliably on CI. If these tests fail, something regressed hard.
 */
final class PerformanceBenchmarkTest extends TestCase
{
    private PDO $pdo;
    private TestConnection $conn;
    private TestConnectionManager $manager;

    /** @var int Number of tracked SQL queries against the PDO. */
    private int $queryCount = 0;
    private PDO $trackedPdo;

    private const LARGE_ROW_COUNT = 500;

    protected function setUp(): void
    {
        QueryBuilder::clearStatementCache();
        QueryCompiler::clearCache();
        EntityHydrator::clearCache();
        RelationLoader::clearCache();

        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        // Schema
        $this->pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, status TEXT, age INTEGER)');
        $this->pdo->exec('CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, user_id INTEGER, status TEXT)');
        $this->pdo->exec('CREATE TABLE comments (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT, post_id INTEGER)');

        // Seed large dataset in a single transaction (fast)
        $this->pdo->exec('BEGIN TRANSACTION');
        for ($i = 1; $i <= self::LARGE_ROW_COUNT; $i++) {
            $status = $i % 5 === 0 ? 'inactive' : 'active';
            $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('User{$i}', 'user{$i}@test.com', '{$status}', " . (20 + ($i % 50)) . ")");
        }

        for ($i = 1; $i <= self::LARGE_ROW_COUNT; $i++) {
            for ($j = 1; $j <= 3; $j++) {
                $postId = ($i - 1) * 3 + $j;
                $this->pdo->exec("INSERT INTO posts (title, user_id, status) VALUES ('Post {$postId}', {$i}, 'published')");
            }
        }

        for ($postId = 1; $postId <= self::LARGE_ROW_COUNT * 3; $postId++) {
            $this->pdo->exec("INSERT INTO comments (body, post_id) VALUES ('Comment A on {$postId}', {$postId})");
            $this->pdo->exec("INSERT INTO comments (body, post_id) VALUES ('Comment B on {$postId}', {$postId})");
        }
        $this->pdo->exec('COMMIT');

        $this->conn = new TestConnection($this->pdo);
        $this->manager = new TestConnectionManager($this->conn);
    }

    private function qb(): QueryBuilder
    {
        return (new QueryBuilder($this->manager))->from('users');
    }

    // ══════════════════════════════════════════════════════════════
    // ── 1. Structural SQL Cache ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * Verify that identical structural queries (same clauses, different values)
     * produce identical SQL templates.
     * The second compile must reuse the cached SQL template.
     */
    public function testStructuralCacheHitRate(): void
    {
        $iterations = 1_000;

        // Cold run
        $this->qb()->where('status', '=', 'active')->compile();

        // Warm run: 1000 identical-structure queries
        $start = hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $this->qb()->where('status', '=', "value_{$i}")->compile();
        }
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        // 1000 cached compiles under 50ms
        self::assertLessThan(50.0, $elapsed, "1000 cached compiles took {$elapsed}ms");
    }

    /**
     * Structural cache must NOT collide on different IN cardinalities.
     */
    public function testStructuralCacheInCardinality(): void
    {
        $sql2 = $this->qb()->whereIn('id', [1, 2])->toSql();
        $sql3 = $this->qb()->whereIn('id', [1, 2, 3])->toSql();

        self::assertStringContainsString('(?, ?)', $sql2);
        self::assertStringContainsString('(?, ?, ?)', $sql3);
        self::assertNotSame($sql2, $sql3);
    }

    /**
     * Structural cache must produce consistent SQL across warm runs.
     */
    public function testStructuralCacheConsistency(): void
    {
        $sql1 = $this->qb()->where('status', '=', 'a')->where('age', '>', 20)->toSql();
        $sql2 = $this->qb()->where('status', '=', 'b')->where('age', '>', 99)->toSql();

        // Same structure, different values → same SQL template
        self::assertSame($sql1, $sql2);
    }

    // ══════════════════════════════════════════════════════════════
    // ── 2. Builder Instantiation Throughput ──────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * 10,000 builder creations + from() must be under 100ms.
     * Validates zero-allocation design at creation time.
     */
    public function testBuilderCreationThroughput(): void
    {
        $iterations = 10_000;

        $start = hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            (new QueryBuilder($this->manager))->from('users');
        }
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertLessThan(100.0, $elapsed, "10k builder creations took {$elapsed}ms");
    }

    // ══════════════════════════════════════════════════════════════
    // ── 3. Fluent Chain Compilation Throughput ────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * Complex query (6 clauses) × 1000 iterations: compile under 50ms.
     * Fluent methods are O(1) appends, not O(n) rebuilds.
     */
    public function testFluentChainThroughput(): void
    {
        $iterations = 1_000;

        $start = hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $this->qb()
                ->select(['id', 'name', 'email'])
                ->where('status', '=', 'active')
                ->where('age', '>', 25)
                ->orderBy('name')
                ->limit(10)
                ->offset(20)
                ->compile();
        }
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertLessThan(50.0, $elapsed, "1000 complex compiles took {$elapsed}ms");
    }

    // ══════════════════════════════════════════════════════════════
    // ── 4. N+1 Elimination — Correctness ─────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * Eager loading ManyToOne: all parent entities get their
     * related object set (not just FK int) after ->with().
     */
    public function testEagerLoadManyToOnePopulatesAllEntities(): void
    {
        $repo = new PerfPostRepo($this->manager);
        $posts = $repo->with(['user'])->findBy([], ['id' => 'ASC'], 100);

        self::assertCount(100, $posts);

        // Every post should have a fully-hydrated User object — not an FK int
        foreach ($posts as $post) {
            self::assertInstanceOf(PerfUser::class, $post->user, "Post '{$post->title}' should have user loaded as object");
        }
    }

    /**
     * Identity map ensures same FK → same PHP object instance in eager loading.
     * This means 100 posts by user #1 all share one User object.
     */
    public function testEagerLoadSharesIdentityMapInstances(): void
    {
        $repo = new PerfPostRepo($this->manager);
        $posts = $repo->with(['user'])->findBy([], ['id' => 'ASC'], 6);

        // User 1 wrote posts 1-3, User 2 wrote posts 4-6
        $post1 = $posts[0];
        $post2 = $posts[1];
        $post3 = $posts[2];

        // Same user FK → same object reference (identity map)
        self::assertSame($post1->user, $post2->user);
        self::assertSame($post2->user, $post3->user);
    }

    /**
     * Without ->with(), relations remain as FK integers (no hidden queries).
     */
    public function testNoEagerLoadKeepsFKAsInteger(): void
    {
        $repo = new PerfPostRepo($this->manager);
        $posts = $repo->findBy([], ['id' => 'ASC'], 10);

        // FK is raw int — no User object, no extra queries
        self::assertIsInt($posts[0]->user);
    }

    /**
     * OneToMany: all parent entities get their child collection populated.
     */
    public function testEagerLoadOneToManyPopulatesAllEntities(): void
    {
        $repo = new PerfPostRepo($this->manager);
        $posts = $repo->with(['comments'])->findBy([], ['id' => 'ASC'], 50);

        self::assertCount(50, $posts);

        // Each post got 2 comments seeded
        foreach ($posts as $post) {
            self::assertIsArray($post->comments);
            self::assertCount(2, $post->comments, "Post '{$post->title}' should have 2 comments");
            self::assertInstanceOf(PerfComment::class, $post->comments[0]);
        }
    }

    /**
     * Multi-relation eager load: user + comments simultaneously.
     */
    public function testEagerLoadMultipleRelationsSimultaneously(): void
    {
        $repo = new PerfPostRepo($this->manager);
        $posts = $repo->with(['user', 'comments'])->findBy([], ['id' => 'ASC'], 20);

        foreach ($posts as $post) {
            self::assertInstanceOf(PerfUser::class, $post->user);
            self::assertIsArray($post->comments);
            self::assertCount(2, $post->comments);
        }
    }

    // ══════════════════════════════════════════════════════════════
    // ── 5. Identity Map — Duplicate Hydration Prevention ─────────
    // ══════════════════════════════════════════════════════════════

    /**
     * find(1) called twice must return the same object instance.
     */
    public function testIdentityMapReturnsSameInstance(): void
    {
        $repo = new PerfUserRepo($this->manager);

        $user1 = $repo->find(1);
        $user2 = $repo->find(1);

        self::assertSame($user1, $user2, 'find(1) should return same object from identity map');
    }

    /**
     * findByIds with partial cache: previously found entities come from map.
     */
    public function testBatchLoadWithPartialCacheHit(): void
    {
        $repo = new PerfUserRepo($this->manager);

        $repo->find(1);  // warm identity map

        $users = $repo->findByIds([1, 2, 3]);

        self::assertCount(3, $users);
        // user 1 is from cache → same instance as before
        self::assertSame($repo->find(1), $users[0]);
    }

    // ══════════════════════════════════════════════════════════════
    // ── 6. Hydration Throughput ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * 500 hydrations from raw rows: under 50ms.
     */
    public function testHydrationThroughput(): void
    {
        $hydrator = new EntityHydrator();

        $rows = [];
        for ($i = 1; $i <= self::LARGE_ROW_COUNT; $i++) {
            $rows[] = ['id' => $i, 'name' => "User{$i}", 'email' => "u{$i}@t.com", 'status' => 'active', 'age' => 25];
        }

        $start = hrtime(true);
        foreach ($rows as $row) {
            $hydrator->hydrate(PerfUser::class, $row);
        }
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertLessThan(50.0, $elapsed, "500 hydrations took {$elapsed}ms");
    }

    /**
     * Warm hydration (reflection cached): 1000 hydrations under 30ms.
     */
    public function testHydrationCacheWarmup(): void
    {
        $hydrator = new EntityHydrator();
        $row = ['id' => 1, 'name' => 'Test', 'email' => 't@t.com', 'status' => 'a', 'age' => 25];

        // Cold pass (caches reflection)
        $hydrator->hydrate(PerfUser::class, $row);

        $start = hrtime(true);
        for ($i = 0; $i < 1_000; $i++) {
            $hydrator->hydrate(PerfUser::class, $row);
        }
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertLessThan(30.0, $elapsed, "1000 warm hydrations took {$elapsed}ms");
    }

    // ══════════════════════════════════════════════════════════════
    // ── 7. when() Zero-Overhead ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * when(false, ...) must NOT invoke the closure.
     */
    public function testWhenFalsyDoesNotInvokeClosure(): void
    {
        $closureCalled = false;

        $sqlWithWhen = $this->qb()
            ->where('status', '=', 'active')
            ->when(false, function () use (&$closureCalled) {
                $closureCalled = true;
            })
            ->toSql();

        $sqlWithout = $this->qb()
            ->where('status', '=', 'active')
            ->toSql();

        self::assertFalse($closureCalled);
        self::assertSame($sqlWithout, $sqlWithWhen);
    }

    /**
     * when(false) overhead: 10k calls with 2x when(false) must add < 20ms.
     */
    public function testWhenFalsyThroughput(): void
    {
        $baseline = hrtime(true);
        for ($i = 0; $i < 10_000; $i++) {
            $this->qb()->where('status', '=', 'active')->compile();
        }
        $baselineMs = (hrtime(true) - $baseline) / 1_000_000;

        $withWhen = hrtime(true);
        for ($i = 0; $i < 10_000; $i++) {
            $this->qb()
                ->where('status', '=', 'active')
                ->when(false, fn($q) => $q->where('age', '>', 25))
                ->when(false, fn($q) => $q->where('name', '=', 'x'))
                ->compile();
        }
        $withWhenMs = (hrtime(true) - $withWhen) / 1_000_000;

        $overhead = $withWhenMs - $baselineMs;

        self::assertLessThan(20.0, $overhead, "when(false) overhead: {$overhead}ms for 20k calls");
    }

    // ══════════════════════════════════════════════════════════════
    // ── 8. Chunk — Correct Batch Count ───────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * chunk(100) on 500 rows must yield exactly ceil(500/100) = 5 batches.
     */
    public function testChunkBatchCount(): void
    {
        $totalRows = 0;
        $batchCount = 0;

        $this->qb()->orderBy('id')->chunk(100, function (array $rows) use (&$totalRows, &$batchCount) {
            $totalRows += count($rows);
            $batchCount++;
        });

        self::assertSame(self::LARGE_ROW_COUNT, $totalRows);
        self::assertSame(5, $batchCount, "chunk(100) on 500 rows should produce 5 batches");
    }

    /**
     * lazy(100) must iterate all 500 rows one at a time.
     */
    public function testLazyIteratesAllRows(): void
    {
        $count = 0;
        foreach ($this->qb()->orderBy('id')->lazy(100) as $row) {
            $count++;
        }

        self::assertSame(self::LARGE_ROW_COUNT, $count);
    }

    /**
     * chunkById must process all rows correctly.
     */
    public function testChunkByIdProcessesAllRows(): void
    {
        $totalRows = 0;
        $this->qb()->chunkById(100, function (array $rows) use (&$totalRows) {
            $totalRows += count($rows);
        });

        self::assertSame(self::LARGE_ROW_COUNT, $totalRows);
    }

    // ══════════════════════════════════════════════════════════════
    // ── 9. tap() — Clone Safety ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * tap() callback receives a clone — mutations must not leak.
     */
    public function testTapMutationsDoNotLeak(): void
    {
        $original = $this->qb()->where('status', '=', 'active');

        $innerBindingCount = 0;
        $result = $original
            ->tap(function (QueryBuilder $q) use (&$innerBindingCount) {
                $q->where('name', '=', 'nobody');
                $innerBindingCount = $q->bindingCount;
            })
            ->get();

        // Inner clone had 2 bindings (status + name), outer has 1
        self::assertSame(2, $innerBindingCount);
        self::assertSame(1, $original->bindingCount);

        // Results are unaffected by tap mutation — returns active users (400)
        $activeCount = (int) $this->pdo->query("SELECT COUNT(*) FROM users WHERE status = 'active'")->fetchColumn();
        self::assertCount($activeCount, $result);
    }

    // ══════════════════════════════════════════════════════════════
    // ── 10. Scope Clone Throughput ───────────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * 10,000 scope clones: under 30ms.
     */
    public function testScopeCloneThroughput(): void
    {
        $repo = new PerfScopedPostRepo($this->manager);

        $start = hrtime(true);
        for ($i = 0; $i < 10_000; $i++) {
            $repo->withoutGlobalScope('onlyPublished');
        }
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertLessThan(30.0, $elapsed, "10k scope clones took {$elapsed}ms");
    }

    /**
     * Global scope should produce different SQL than no-scope.
     */
    public function testScopeProducesDifferentSql(): void
    {
        $repoNoScope = new PerfPostRepo($this->manager);
        $repoWithScope = new PerfScopedPostRepo($this->manager);

        $sqlNoScope = $repoNoScope->query()->toSql();
        $sqlWithScope = $repoWithScope->query()->toSql();

        self::assertStringNotContainsString('status', $sqlNoScope);
        self::assertStringContainsString('status = ?', $sqlWithScope);
    }

    // ══════════════════════════════════════════════════════════════
    // ── 11. Sub-Query Compilation Throughput ──────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * 1000 sub-query compilations: under 100ms.
     */
    public function testSubQueryCompilationThroughput(): void
    {
        $iterations = 1_000;

        $start = hrtime(true);
        for ($i = 0; $i < $iterations; $i++) {
            $this->qb()
                ->whereSubQuery('id', 'IN', fn(QueryBuilder $sq) => $sq
                    ->from('posts')
                    ->select(['user_id'])
                    ->where('status', '=', 'published')
                )
                ->compile();
        }
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertLessThan(100.0, $elapsed, "1000 sub-query compiles took {$elapsed}ms");
    }

    // ══════════════════════════════════════════════════════════════
    // ── 12. selectSub Execution ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * selectSub returns correct correlated sub-query results.
     */
    public function testSelectSubCorrelatedResults(): void
    {
        $results = $this->qb()
            ->select(['id', 'name'])
            ->selectSub(fn(QueryBuilder $sq) => $sq
                ->from('posts')
                ->select([new RawExpression('COUNT(*)')])
                ->whereRaw('posts.user_id = users.id'),
                as: 'post_count',
            )
            ->limit(5)
            ->get();

        self::assertCount(5, $results);
        // Each user has 3 posts
        foreach ($results as $row) {
            self::assertEquals(3, $row['post_count'], "User '{$row['name']}' should have 3 posts");
        }
    }

    // ══════════════════════════════════════════════════════════════
    // ── 13. Full Pipeline Benchmark ──────────────────────────────
    // ══════════════════════════════════════════════════════════════

    /**
     * End-to-end: build → compile → execute → hydrate 100 entities
     * with eager-loaded relation under 100ms total.
     */
    public function testFullPipelineThroughput(): void
    {
        $repo = new PerfPostRepo($this->manager);

        $start = hrtime(true);
        $posts = $repo->with(['user'])->findBy([], ['id' => 'ASC'], 100);
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertCount(100, $posts);
        self::assertInstanceOf(PerfUser::class, $posts[0]->user);
        self::assertLessThan(100.0, $elapsed, "Full pipeline (100 posts + users) took {$elapsed}ms");
    }

    /**
     * Full pipeline: 500 posts + users + comments. Must complete under 500ms.
     */
    public function testFullPipelineLargeDataset(): void
    {
        $repo = new PerfPostRepo($this->manager);

        $start = hrtime(true);
        $posts = $repo->with(['user', 'comments'])->findAll();
        $elapsed = (hrtime(true) - $start) / 1_000_000;

        self::assertCount(self::LARGE_ROW_COUNT * 3, $posts);
        self::assertInstanceOf(PerfUser::class, $posts[0]->user);
        self::assertCount(2, $posts[0]->comments);
        self::assertLessThan(500.0, $elapsed, "Full pipeline ({$this->countPosts()} posts + 2 relations) took {$elapsed}ms");
    }

    // ── Helpers ──────────────────────────────────────────────────

    private function countPosts(): int
    {
        return (int) $this->pdo->query('SELECT COUNT(*) FROM posts')->fetchColumn();
    }
}

// ══════════════════════════════════════════════════════════════════
// ── Performance Test Entities & Repos ────────────────────────────
// ══════════════════════════════════════════════════════════════════

#[Entity(table: 'users')]
class PerfUser
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

#[Entity(table: 'posts')]
class PerfPost
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public string $title;

    #[ManyToOne(targetEntity: PerfUser::class, inversedBy: 'posts')]
    public PerfUser|int $user;

    #[Field(type: 'string')]
    public string $status;

    #[OneToMany(targetEntity: PerfComment::class, mappedBy: 'post')]
    public array $comments = [];
}

#[Entity(table: 'comments')]
class PerfComment
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public string $body;

    #[ManyToOne(targetEntity: PerfPost::class, inversedBy: 'comments')]
    public PerfPost|int $post;
}

class PerfUserRepo extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = PerfUser::class;
}

class PerfPostRepo extends EntityRepository
{
    protected string $table = 'posts';
    protected string $entityClass = PerfPost::class;
}

class PerfScopedPostRepo extends EntityRepository
{
    protected string $table = 'posts';
    protected string $entityClass = PerfPost::class;

    #[Scope(isGlobal: true, name: 'onlyPublished')]
    public function onlyPublished(QueryBuilder $qb): QueryBuilder
    {
        return $qb->where('status', '=', 'published');
    }
}
