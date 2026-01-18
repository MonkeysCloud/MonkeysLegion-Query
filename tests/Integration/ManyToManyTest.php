<?php

declare(strict_types=1);

namespace Tests\Integration;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Repository\EntityRepository;
use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\ManyToMany;
use MonkeysLegion\Entity\Attributes\JoinTable;
use MonkeysLegion\Entity\Attributes\Entity;


/**
 * Test Entity: Post - Owning side of ManyToMany
 */
#[Entity(table: 'posts')]
class Post
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $title = '';

    /** @var Tag[] */
    #[ManyToMany(
        targetEntity: Tag::class,
        inversedBy: 'posts',
        joinTable: new JoinTable(
            name: 'post_tag',
            joinColumn: 'post_id',
            inverseColumn: 'tag_id'
        )
    )]
    public array $tags = [];
}

/**
 * Test Entity: Tag - Inverse side of ManyToMany
 */
#[Entity(table: 'tags')]
class Tag
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $name = '';

    /** @var Post[] */
    #[ManyToMany(
        targetEntity: Post::class,
        mappedBy: 'tags'
    )]
    public array $posts = [];
}

/**
 * Test Repository: PostRepository
 */
class PostRepository extends EntityRepository
{
    protected string $table = 'posts';
    protected string $entityClass = Post::class;
}

/**
 * Test Repository: TagRepository
 */
class TagRepository extends EntityRepository
{
    protected string $table = 'tags';
    protected string $entityClass = Tag::class;
}

/**
 * Integration tests for ManyToMany relationships.
 *
 * Tests cover:
 * - Attaching relations
 * - Detaching relations
 * - Loading ManyToMany relations
 * - Deleting entities with ManyToMany cascade
 * - Finding entities by relation
 */
class ManyToManyTest extends TestCase
{
    private QueryBuilder $qb;
    private PostRepository $postRepo;
    private TagRepository $tagRepo;
    private \PDO $pdo;
    private bool $isSqlite = false;

    protected function setUp(): void
    {
        // Use in-memory SQLite for testing
        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
        $this->isSqlite = $this->pdo->getAttribute(\PDO::ATTR_DRIVER_NAME) === 'sqlite';

        // Create test tables - simulate MySQL-like structure
        $this->pdo->exec('
            CREATE TABLE posts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                title TEXT NOT NULL
            )
        ');

        $this->pdo->exec('
            CREATE TABLE tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL
            )
        ');

        $this->pdo->exec('
            CREATE TABLE post_tag (
                post_id INTEGER NOT NULL,
                tag_id INTEGER NOT NULL,
                PRIMARY KEY (post_id, tag_id),
                FOREIGN KEY (post_id) REFERENCES posts(id) ON DELETE CASCADE,
                FOREIGN KEY (tag_id) REFERENCES tags(id) ON DELETE CASCADE
            )
        ');

        // Insert base test data
        $this->pdo->exec("INSERT INTO posts (title) VALUES ('First Post'), ('Second Post')");
        $this->pdo->exec("INSERT INTO tags (name) VALUES ('PHP'), ('Testing'), ('ORM')");

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
        $this->postRepo = new PostRepository($this->qb);
        $this->tagRepo = new TagRepository($this->qb);
    }

    protected function tearDown(): void
    {
        // Clean up
        $this->pdo->exec('DELETE FROM post_tag');
        $this->pdo->exec('DELETE FROM posts');
        $this->pdo->exec('DELETE FROM tags');
    }

    /**
     * Test attaching a ManyToMany relation creates join table entry.
     */
    public function testAttachRelation(): void
    {
        $post = $this->postRepo->find(1, false);
        $this->assertNotNull($post, 'Post should exist');

        // Attach tag with ID 1 to post
        $result = $this->postRepo->attachRelation($post, 'tags', 1);
        $this->assertEquals(1, $result, 'Should affect 1 row');

        // Verify in database
        $stmt = $this->pdo->query('SELECT * FROM post_tag WHERE post_id = 1 AND tag_id = 1');
        $row = $stmt->fetch(\PDO::FETCH_ASSOC);
        $this->assertNotFalse($row, 'Join table entry should exist');
        $this->assertEquals(1, $row['post_id']);
        $this->assertEquals(1, $row['tag_id']);
    }

    /**
     * Test attaching multiple relations.
     */
    public function testAttachMultipleRelations(): void
    {
        $post = $this->postRepo->find(1, false);
        $this->assertNotNull($post);

        // Attach all three tags
        $this->postRepo->attachRelation($post, 'tags', 1);
        $this->postRepo->attachRelation($post, 'tags', 2);
        $this->postRepo->attachRelation($post, 'tags', 3);

        // Verify count
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM post_tag WHERE post_id = 1');
        $count = (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt'];
        $this->assertEquals(3, $count, 'Should have 3 tag associations');
    }

    /**
     * Test detaching a ManyToMany relation removes join table entry.
     */
    public function testDetachRelation(): void
    {
        // First attach
        $post = $this->postRepo->find(1, false);
        $this->postRepo->attachRelation($post, 'tags', 1);
        $this->postRepo->attachRelation($post, 'tags', 2);

        // Verify attached
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM post_tag WHERE post_id = 1');
        $this->assertEquals(2, (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt']);

        // Detach one
        $result = $this->postRepo->detachRelation($post, 'tags', 1);
        $this->assertEquals(1, $result, 'Should affect 1 row');

        // Verify only one remains
        $stmt = $this->pdo->query('SELECT * FROM post_tag WHERE post_id = 1');
        $rows = $stmt->fetchAll(\PDO::FETCH_ASSOC);
        $this->assertCount(1, $rows, 'Should have 1 tag association remaining');
        $this->assertEquals(2, $rows[0]['tag_id']);
    }

    /**
     * Test loading ManyToMany relations via loadRelations.
     */
    public function testLoadManyToManyRelation(): void
    {
        // Set up: Attach tags to post
        $post = $this->postRepo->find(1, false);
        $this->postRepo->attachRelation($post, 'tags', 1);
        $this->postRepo->attachRelation($post, 'tags', 2);

        // Load post with relations
        $postWithRelations = $this->postRepo->find(1, true);
        $this->assertNotNull($postWithRelations);
        
        // Check that tags property is populated
        $this->assertIsArray($postWithRelations->tags, 'Tags should be an array');
        $this->assertCount(2, $postWithRelations->tags, 'Should have 2 tags');

        // Verify tag instances
        $tagIds = array_map(fn($tag) => $tag->id, $postWithRelations->tags);
        sort($tagIds);
        $this->assertEquals([1, 2], $tagIds, 'Should have tags with IDs 1 and 2');
    }

    /**
     * Test loading inverse side of ManyToMany relation.
     */
    public function testLoadManyToManyInverseSide(): void
    {
        // Set up: Attach tags to multiple posts
        $post1 = $this->postRepo->find(1, false);
        $post2 = $this->postRepo->find(2, false);
        
        $this->postRepo->attachRelation($post1, 'tags', 1);
        $this->postRepo->attachRelation($post2, 'tags', 1);

        // Load tag with relations (inverse side)
        $tagWithRelations = $this->tagRepo->find(1, true);
        $this->assertNotNull($tagWithRelations);

        // Check that posts property is populated
        $this->assertIsArray($tagWithRelations->posts, 'Posts should be an array');
        $this->assertCount(2, $tagWithRelations->posts, 'Should have 2 posts');

        // Verify post instances
        $postIds = array_map(fn($post) => $post->id, $tagWithRelations->posts);
        sort($postIds);
        $this->assertEquals([1, 2], $postIds, 'Should have posts with IDs 1 and 2');
    }

    /**
     * Test that deleting entity cascades to join table.
     */
    public function testDeleteCascadesManyToMany(): void
    {
        // Set up: Attach tags to post
        $post = $this->postRepo->find(1, false);
        $this->postRepo->attachRelation($post, 'tags', 1);
        $this->postRepo->attachRelation($post, 'tags', 2);

        // Verify join table has entries
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM post_tag WHERE post_id = 1');
        $this->assertEquals(2, (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt']);

        // Delete post
        $result = $this->postRepo->delete(1);
        $this->assertEquals(1, $result, 'Should delete 1 post');

        // Verify join table entries are removed
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM post_tag WHERE post_id = 1');
        $count = (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt'];
        $this->assertEquals(0, $count, 'Join table entries should be cascaded/deleted');
    }

    /**
     * Test findByRelation for ManyToMany.
     */
    public function testFindByRelation(): void
    {
        // Set up: Link posts to a tag
        $post1 = $this->postRepo->find(1, false);
        $post2 = $this->postRepo->find(2, false);

        $this->postRepo->attachRelation($post1, 'tags', 1);
        $this->postRepo->attachRelation($post2, 'tags', 1);
        $this->postRepo->attachRelation($post1, 'tags', 2); // Post 1 also has tag 2

        // Find all posts with tag ID 1
        $posts = $this->postRepo->findByRelation('tags', 1);
        
        $this->assertIsArray($posts);
        $this->assertCount(2, $posts, 'Should find 2 posts with tag 1');

        $postIds = array_map(fn($post) => $post->id, $posts);
        sort($postIds);
        $this->assertEquals([1, 2], $postIds);
    }

    /**
     * Test that save() on entity preserves ManyToMany relations when tags are set.
     * Note: With sync behavior, save() syncs ManyToMany from entity collection.
     */
    public function testSaveDoesNotBreakManyToMany(): void
    {
        // Load post WITH relations so tags array is populated
        $post = $this->postRepo->find(1, true);
        
        // Get the tag to attach
        $tag = $this->tagRepo->find(1);
        $this->assertNotNull($tag);
        
        // Set up: Add tag to the collection  
        $post->tags = [$tag];
        $this->postRepo->save($post);

        // Update post title (keeping tags in the collection)
        $post->title = 'Updated Title';
        $this->postRepo->save($post);

        // Verify relation still exists
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM post_tag WHERE post_id = 1 AND tag_id = 1');
        $count = (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt'];
        $this->assertEquals(1, $count, 'ManyToMany relation should still exist after save');

        // Load and verify
        $postReloaded = $this->postRepo->find(1, true);
        $this->assertEquals('Updated Title', $postReloaded->title);
        $this->assertCount(1, $postReloaded->tags);
    }

    /**
     * Test creating new entity and attaching relations.
     */
    public function testCreateEntityAndAttachRelations(): void
    {
        // Create new post
        $newPost = new Post();
        $newPost->title = 'New Post';
        $id = $this->postRepo->save($newPost);

        $this->assertGreaterThan(0, $id, 'Should return new ID');

        // Attach relations
        $this->postRepo->attachRelation($newPost, 'tags', 1);
        $this->postRepo->attachRelation($newPost, 'tags', 3);

        // Verify
        $loadedPost = $this->postRepo->find($id, true);
        $this->assertCount(2, $loadedPost->tags);

        $tagIds = array_map(fn($tag) => $tag->id, $loadedPost->tags);
        sort($tagIds);
        $this->assertEquals([1, 3], $tagIds);
    }

    /**
     * Test deleting inverse side entity cascades properly.
     */
    public function testDeleteInverseSideCascades(): void
    {
        // Set up: Tag 1 is linked to both posts
        $post1 = $this->postRepo->find(1, false);
        $post2 = $this->postRepo->find(2, false);
        
        $this->postRepo->attachRelation($post1, 'tags', 1);
        $this->postRepo->attachRelation($post2, 'tags', 1);

        // Verify join table has 2 entries for tag 1
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM post_tag WHERE tag_id = 1');
        $this->assertEquals(2, (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt']);

        // Delete the tag
        $result = $this->tagRepo->delete(1);
        $this->assertEquals(1, $result);

        // Verify join table entries are removed
        $stmt = $this->pdo->query('SELECT COUNT(*) as cnt FROM post_tag WHERE tag_id = 1');
        $count = (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt'];
        $this->assertEquals(0, $count, 'Join table entries should be removed when tag is deleted');
    }

    /**
     * Test that empty ManyToMany loads as empty array (not null).
     */
    public function testEmptyManyToManyReturnsEmptyArray(): void
    {
        // Post 2 has no tags attached
        $post = $this->postRepo->find(2, true);
        
        $this->assertNotNull($post);
        $this->assertIsArray($post->tags, 'Tags should be an array even if empty');
        $this->assertCount(0, $post->tags, 'Should have no tags');
    }

    /**
     * Test that save() syncs ManyToMany collections without using attachRelation.
     */
    public function testSaveSyncsManyToManyCollection(): void
    {
        // Get tag entities
        $tag1 = $this->tagRepo->find(1);
        $tag2 = $this->tagRepo->find(2);
        $this->assertNotNull($tag1);
        $this->assertNotNull($tag2);

        // Create a new post with tags set directly on the entity
        $post = new Post();
        $post->title = 'Post with Collection';
        $post->tags = [$tag1, $tag2];

        // Save should sync the ManyToMany relations
        $postId = $this->postRepo->save($post);
        $this->assertNotNull($postId);

        // Reload and verify tags are attached
        $loaded = $this->postRepo->find($postId, true);
        $this->assertNotNull($loaded);
        $this->assertCount(2, $loaded->tags, 'Should have 2 tags after save');

        // Modify the collection (remove tag1, keep tag2)
        $loaded->tags = [$tag2];
        $this->postRepo->save($loaded);

        // Reload and verify only tag2 remains
        $reloaded = $this->postRepo->find($postId, true);
        $this->assertCount(1, $reloaded->tags, 'Should have 1 tag after removing from collection');
        $this->assertEquals($tag2->id, $reloaded->tags[0]->id);

        // Clear all tags
        $reloaded->tags = [];
        $this->postRepo->save($reloaded);

        // Reload and verify empty
        $empty = $this->postRepo->find($postId, true);
        $this->assertCount(0, $empty->tags, 'Should have no tags after clearing collection');
    }

    /**
     * Test that delete() removes entity and cascades to ManyToMany join table.
     */
    public function testDeleteRemovesManyToManyRelations(): void
    {
        // Create post with tags using save
        $tag1 = $this->tagRepo->find(1);
        $post = new Post();
        $post->title = 'Post to Delete';
        $post->tags = [$tag1];
        $postId = $this->postRepo->save($post);

        // Verify join table entry exists
        $stmt = $this->pdo->prepare('SELECT COUNT(*) as cnt FROM post_tag WHERE post_id = ?');
        $stmt->execute([$postId]);
        $this->assertEquals(1, (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt']);

        // Delete the post
        $this->postRepo->delete($postId);

        // Verify join table entry is removed (cascade)
        $stmt->execute([$postId]);
        $this->assertEquals(0, (int)$stmt->fetch(\PDO::FETCH_ASSOC)['cnt']);
    }
}

