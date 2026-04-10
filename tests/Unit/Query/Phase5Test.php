<?php
declare(strict_types=1);

namespace Tests\Unit\Query;

use MonkeysLegion\Query\Attributes\Scope;
use MonkeysLegion\Query\Enums\WhereBoolean;
use MonkeysLegion\Query\Query\QueryBuilder;
use MonkeysLegion\Query\Repository\EntityHydrator;
use MonkeysLegion\Query\Repository\EntityRepository;
use MonkeysLegion\Query\Repository\RelationLoader;
use MonkeysLegion\Query\Repository\RelationMetadata;
use MonkeysLegion\Entity\Attributes\Entity;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToMany;
use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Support\TestConnection;
use Tests\Support\TestConnectionManager;

/**
 * Tests for Phase 5A-5C features:
 *
 * Phase 5A — Relation Loader & Eager Loading
 *   - RelationMetadata VO
 *   - RelationLoader: ManyToOne, OneToMany batch loading
 *   - EntityRepository::with() eager loading
 *   - Nested dot-notation
 *   - N+1 elimination verification via query count
 *
 * Phase 5B — Query Hardening
 *   - selectSub()
 *   - whereColumn()
 *   - whereSubQuery()
 *   - when() conditional
 *   - tap() debug hook
 *
 * Phase 5C — Scope Completion
 *   - withoutGlobalScope()
 *   - withoutGlobalScopes()
 */
final class Phase5Test extends TestCase
{
    private PDO $pdo;
    private TestConnection $conn;
    private TestConnectionManager $manager;

    protected function setUp(): void
    {
        QueryBuilder::clearStatementCache();
        EntityHydrator::clearCache();
        RelationLoader::clearCache();

        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);

        // Schema
        $this->pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, status TEXT)');
        $this->pdo->exec('CREATE TABLE posts (id INTEGER PRIMARY KEY AUTOINCREMENT, title TEXT, user_id INTEGER, status TEXT)');
        $this->pdo->exec('CREATE TABLE comments (id INTEGER PRIMARY KEY AUTOINCREMENT, body TEXT, post_id INTEGER)');

        // Users
        $this->pdo->exec("INSERT INTO users (name, status) VALUES ('Alice', 'active')");  // id=1
        $this->pdo->exec("INSERT INTO users (name, status) VALUES ('Bob', 'active')");    // id=2
        $this->pdo->exec("INSERT INTO users (name, status) VALUES ('Charlie', 'banned')"); // id=3

        // Posts
        $this->pdo->exec("INSERT INTO posts (title, user_id, status) VALUES ('Post A', 1, 'published')");  // id=1
        $this->pdo->exec("INSERT INTO posts (title, user_id, status) VALUES ('Post B', 1, 'published')");  // id=2
        $this->pdo->exec("INSERT INTO posts (title, user_id, status) VALUES ('Post C', 2, 'draft')");      // id=3

        // Comments
        $this->pdo->exec("INSERT INTO comments (body, post_id) VALUES ('Great!', 1)");   // id=1
        $this->pdo->exec("INSERT INTO comments (body, post_id) VALUES ('Nice.', 1)");    // id=2
        $this->pdo->exec("INSERT INTO comments (body, post_id) VALUES ('Cool.', 2)");    // id=3

        $this->conn = new TestConnection($this->pdo);
        $this->manager = new TestConnectionManager($this->conn);
    }

    private function qb(): QueryBuilder
    {
        return (new QueryBuilder($this->manager))->from('users');
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5A: RelationMetadata VO ────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testRelationMetadataProperties(): void
    {
        $meta = new RelationMetadata(
            propertyName: 'user',
            relationType: 'ManyToOne',
            targetEntity: Phase5User::class,
            foreignKey: 'user_id',
            mappedBy: null,
            inversedBy: 'posts',
        );

        self::assertSame('user', $meta->propertyName);
        self::assertSame('ManyToOne', $meta->relationType);
        self::assertSame(Phase5User::class, $meta->targetEntity);
        self::assertSame('user_id', $meta->foreignKey);
        self::assertNull($meta->mappedBy);
        self::assertSame('posts', $meta->inversedBy);
        self::assertFalse($meta->isCollection);
    }

    public function testRelationMetadataCollectionFlag(): void
    {
        $meta = new RelationMetadata(
            propertyName: 'posts',
            relationType: 'OneToMany',
            targetEntity: Phase5Post::class,
            foreignKey: 'user_id',
            isCollection: true,
        );

        self::assertTrue($meta->isCollection);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5A: RelationLoader Metadata Extraction ─────────────
    // ══════════════════════════════════════════════════════════════

    public function testRelationLoaderExtractsMetadata(): void
    {
        $hydrator = new EntityHydrator();
        $loader = new RelationLoader(
            $this->manager,
            $hydrator,
            new \MonkeysLegion\Query\Repository\IdentityMap(),
        );

        $meta = $loader->getRelationMetadata(Phase5Post::class);

        self::assertCount(2, $meta, 'Post has two relations: user (ManyToOne) + comments (OneToMany)');

        // ManyToOne: user
        $userMeta = $this->findByProp($meta, 'user');
        self::assertNotNull($userMeta);
        self::assertSame('ManyToOne', $userMeta->relationType);
        self::assertSame(Phase5User::class, $userMeta->targetEntity);
        self::assertSame('user_id', $userMeta->foreignKey);

        // OneToMany: comments
        $commentsMeta = $this->findByProp($meta, 'comments');
        self::assertNotNull($commentsMeta);
        self::assertSame('OneToMany', $commentsMeta->relationType);
        self::assertSame(Phase5Comment::class, $commentsMeta->targetEntity);
        self::assertTrue($commentsMeta->isCollection);
    }

    public function testRelationLoaderMetadataIsCached(): void
    {
        $hydrator = new EntityHydrator();
        $loader = new RelationLoader(
            $this->manager,
            $hydrator,
            new \MonkeysLegion\Query\Repository\IdentityMap(),
        );

        $meta1 = $loader->getRelationMetadata(Phase5Post::class);
        $meta2 = $loader->getRelationMetadata(Phase5Post::class);

        // Same array reference (from cache)
        self::assertSame($meta1, $meta2);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5A: Eager Loading via Repository::with() ──────────
    // ══════════════════════════════════════════════════════════════

    public function testWithReturnsClone(): void
    {
        $repo = new Phase5PostRepo($this->manager);
        $cloned = $repo->with(['user']);

        self::assertNotSame($repo, $cloned);
    }

    public function testEagerLoadManyToOne(): void
    {
        $repo = new Phase5PostRepo($this->manager);
        $posts = $repo->with(['user'])->findAll();

        self::assertCount(3, $posts);

        // Each post should have its user property set to a Phase5User object
        foreach ($posts as $post) {
            self::assertInstanceOf(Phase5User::class, $post->user, "Post '{$post->title}' should have user loaded");
        }

        // Alice wrote post A and B
        $postA = $this->findPostByTitle($posts, 'Post A');
        self::assertSame('Alice', $postA->user->name);

        $postB = $this->findPostByTitle($posts, 'Post B');
        self::assertSame('Alice', $postB->user->name);

        // Same object instance (identity map)
        self::assertSame($postA->user, $postB->user, 'Same user FK → same object instance');
    }

    public function testEagerLoadOneToMany(): void
    {
        $repo = new Phase5PostRepo($this->manager);
        $posts = $repo->with(['comments'])->findAll();

        $postA = $this->findPostByTitle($posts, 'Post A');
        self::assertIsArray($postA->comments);
        self::assertCount(2, $postA->comments, 'Post A has 2 comments');

        $postB = $this->findPostByTitle($posts, 'Post B');
        self::assertCount(1, $postB->comments, 'Post B has 1 comment');

        $postC = $this->findPostByTitle($posts, 'Post C');
        self::assertCount(0, $postC->comments, 'Post C has 0 comments');
    }

    public function testEagerLoadMultipleRelations(): void
    {
        $repo = new Phase5PostRepo($this->manager);
        $posts = $repo->with(['user', 'comments'])->findAll();

        $postA = $this->findPostByTitle($posts, 'Post A');
        self::assertInstanceOf(Phase5User::class, $postA->user);
        self::assertCount(2, $postA->comments);
    }

    public function testWithoutEagerLoadRelationsAreNotLoaded(): void
    {
        $repo = new Phase5PostRepo($this->manager);
        $posts = $repo->findAll();

        // Without ->with(), relations are FK IDs (int), not entity objects
        $postA = $this->findPostByTitle($posts, 'Post A');
        // user property holds the FK int value (1), not a User object
        self::assertSame(1, $postA->user);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5B: selectSub ──────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testSelectSub(): void
    {
        $results = $this->qb()
            ->select(['name'])
            ->selectSub(fn(QueryBuilder $sq) => $sq
                ->from('posts')
                ->select([new \MonkeysLegion\Query\RawExpression('COUNT(*)')])
                ->whereRaw('posts.user_id = users.id'),
                as: 'post_count',
            )
            ->get();

        self::assertCount(3, $results);
        // Alice has 2 posts
        $alice = $this->findRowByName($results, 'Alice');
        self::assertEquals(2, $alice['post_count']);

        // Bob has 1 post
        $bob = $this->findRowByName($results, 'Bob');
        self::assertEquals(1, $bob['post_count']);

        // Charlie has 0 posts
        $charlie = $this->findRowByName($results, 'Charlie');
        self::assertEquals(0, $charlie['post_count']);
    }

    public function testSelectSubCompilesSql(): void
    {
        $sql = $this->qb()
            ->select(['name'])
            ->selectSub(
                fn(QueryBuilder $sq) => $sq->from('posts')->select([new \MonkeysLegion\Query\RawExpression('COUNT(*)')]),
                as: 'post_count',
            )
            ->toSql();

        self::assertStringContainsString('(SELECT', $sql);
        self::assertStringContainsString(') AS post_count', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5B: whereColumn ────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereColumnCompiles(): void
    {
        $sql = $this->qb()
            ->whereColumn('users.id', '=', 'posts.user_id')
            ->toSql();

        self::assertStringContainsString('users.id = posts.user_id', $sql);
    }

    public function testWhereColumnWithOrBoolean(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->whereColumn('users.id', '>', 'users.parent_id', WhereBoolean::Or)
            ->toSql();

        self::assertStringContainsString('OR users.id > users.parent_id', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5B: whereSubQuery ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereSubQuery(): void
    {
        $results = $this->qb()
            ->whereSubQuery('id', 'IN', fn(QueryBuilder $sq) => $sq
                ->from('posts')
                ->select(['user_id'])
            )
            ->get();

        // Users 1 (Alice) and 2 (Bob) have posts; Charlie does not
        self::assertCount(2, $results);
        $names = array_column($results, 'name');
        self::assertContains('Alice', $names);
        self::assertContains('Bob', $names);
    }

    public function testWhereSubQueryCompilesSql(): void
    {
        $compiled = $this->qb()
            ->whereSubQuery('id', 'IN', fn(QueryBuilder $sq) => $sq
                ->from('posts')
                ->select(['user_id'])
                ->where('status', '=', 'published'),
            )
            ->compile();

        $sql = $compiled['sql'];
        self::assertStringContainsString('IN (SELECT', $sql);
        self::assertStringContainsString('posts', $sql);
        self::assertSame(['published'], $compiled['bindings']);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5B: when() ─────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhenTruthyAppliesCallback(): void
    {
        $status = 'active';
        $results = $this->qb()
            ->when($status !== null, fn(QueryBuilder $q) => $q->where('status', '=', $status))
            ->get();

        // Only active users: Alice, Bob
        self::assertCount(2, $results);
    }

    public function testWhenFalsySkipsCallback(): void
    {
        $status = null;
        $results = $this->qb()
            ->when($status !== null, fn(QueryBuilder $q) => $q->where('status', '=', $status))
            ->get();

        // No filter applied → all 3 users
        self::assertCount(3, $results);
    }

    public function testWhenFalsyWithFallback(): void
    {
        $status = null;
        $results = $this->qb()
            ->when(
                $status !== null,
                fn(QueryBuilder $q) => $q->where('status', '=', $status),
                fn(QueryBuilder $q) => $q->orderBy('name'),
            )
            ->get();

        // Fallback applied: ordered by name
        self::assertSame('Alice', $results[0]['name']);
        self::assertSame('Bob', $results[1]['name']);
        self::assertSame('Charlie', $results[2]['name']);
    }

    public function testWhenPassesConditionValue(): void
    {
        $count = 5;
        $applied = false;

        $this->qb()
            ->when($count, function (QueryBuilder $q, mixed $value) use (&$applied) {
                $applied = true;
                self::assertSame(5, $value);
                $q->limit($value);
            })
            ->compile();

        self::assertTrue($applied);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5B: tap() ──────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testTapDoesNotMutateBuilder(): void
    {
        $capturedSql = '';

        $results = $this->qb()
            ->where('status', '=', 'active')
            ->tap(function (QueryBuilder $q) use (&$capturedSql) {
                $capturedSql = $q->toSql();
                // Mutate the clone — should NOT affect the outer builder
                $q->where('name', '=', 'nobody');
            })
            ->get();

        // tap mutation didn't affect the original: still returns 2 active users
        self::assertCount(2, $results);
        self::assertNotEmpty($capturedSql);
        self::assertStringContainsString('status', $capturedSql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5C: withoutGlobalScope ─────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWithoutGlobalScopeBypassesSpecificScope(): void
    {
        $repo = new Phase5SoftDeletePostRepo($this->manager);

        // With scope: only 'published' posts
        $filtered = $repo->findAll();
        self::assertCount(2, $filtered);

        // Without scope: all 3 posts
        $all = $repo->withoutGlobalScope('publishedOnly')->findAll();
        self::assertCount(3, $all);
    }

    public function testWithoutGlobalScopeReturnsClone(): void
    {
        $repo = new Phase5SoftDeletePostRepo($this->manager);
        $cloned = $repo->withoutGlobalScope('publishedOnly');
        self::assertNotSame($repo, $cloned);
    }

    public function testWithoutGlobalScopesAllDisabled(): void
    {
        $repo = new Phase5SoftDeletePostRepo($this->manager);
        $all = $repo->withoutGlobalScopes()->findAll();
        self::assertCount(3, $all);
    }

    public function testOriginalRepoUnaffectedAfterScopeBypass(): void
    {
        $repo = new Phase5SoftDeletePostRepo($this->manager);

        // This creates a clone with scope disabled
        $repo->withoutGlobalScope('publishedOnly')->findAll();

        // Original repo still applies scope
        $filtered = $repo->findAll();
        self::assertCount(2, $filtered);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Phase 5C: scope with EntityRepository::count/exists ─────
    // ══════════════════════════════════════════════════════════════

    public function testScopeBypassAffectsCount(): void
    {
        $repo = new Phase5SoftDeletePostRepo($this->manager);

        self::assertSame(2, $repo->count());
        self::assertSame(3, $repo->withoutGlobalScopes()->count());
    }

    public function testScopeBypassAffectsExists(): void
    {
        $repo = new Phase5SoftDeletePostRepo($this->manager);

        // Draft post (id=3) is excluded by scope
        self::assertFalse($repo->exists(['id' => 3]));
        self::assertTrue($repo->withoutGlobalScopes()->exists(['id' => 3]));
    }

    // ══════════════════════════════════════════════════════════════
    // ── Helpers ──────────────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    private function findByProp(array $meta, string $name): ?RelationMetadata
    {
        foreach ($meta as $m) {
            if ($m->propertyName === $name) {
                return $m;
            }
        }
        return null;
    }

    private function findPostByTitle(array $posts, string $title): Phase5Post
    {
        foreach ($posts as $post) {
            if ($post->title === $title) {
                return $post;
            }
        }
        throw new \RuntimeException("Post '{$title}' not found.");
    }

    private function findRowByName(array $rows, string $name): array
    {
        foreach ($rows as $row) {
            if ($row['name'] === $name) {
                return $row;
            }
        }
        throw new \RuntimeException("Row with name '{$name}' not found.");
    }
}

// ══════════════════════════════════════════════════════════════════
// ── Test Entities ────────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════

#[Entity(table: 'users')]
class Phase5User
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public string $name;

    #[Field(type: 'string')]
    public string $status;
}

#[Entity(table: 'posts')]
class Phase5Post
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public string $title;

    #[ManyToOne(targetEntity: Phase5User::class, inversedBy: 'posts')]
    public Phase5User|int $user;

    #[Field(type: 'string')]
    public string $status;

    #[OneToMany(targetEntity: Phase5Comment::class, mappedBy: 'post')]
    public array $comments = [];
}

#[Entity(table: 'comments')]
class Phase5Comment
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public string $body;

    #[ManyToOne(targetEntity: Phase5Post::class, inversedBy: 'comments')]
    public Phase5Post|int $post;
}

// ══════════════════════════════════════════════════════════════════
// ── Test Repositories ────────────────────────────────────────────
// ══════════════════════════════════════════════════════════════════

class Phase5PostRepo extends EntityRepository
{
    protected string $table = 'posts';
    protected string $entityClass = Phase5Post::class;
}

class Phase5SoftDeletePostRepo extends EntityRepository
{
    protected string $table = 'posts';
    protected string $entityClass = Phase5Post::class;

    #[Scope(isGlobal: true, name: 'publishedOnly')]
    public function publishedOnly(QueryBuilder $qb): QueryBuilder
    {
        return $qb->where('status', '=', 'published');
    }
}
