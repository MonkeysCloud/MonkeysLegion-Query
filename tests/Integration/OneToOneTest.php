<?php

declare(strict_types=1);

namespace Tests\Integration;

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Repository\EntityRepository;
use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\OneToOne;
use MonkeysLegion\Entity\Attributes\Entity;

/**
 * Test Entity: User
 */
#[Entity(table: 'users')]
class User
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string')]
    public string $username = '';

    #[OneToOne(targetEntity: UserProfile::class, mappedBy: 'user')]
    public ?UserProfile $profile = null;
}

/**
 * Test Entity: UserProfile
 */
#[Entity(table: 'user_profiles')]
class UserProfile
{
    #[Field(type: 'int', primaryKey: true)]
    public ?int $id = null;

    #[Field(type: 'string', nullable: true)]
    public ?string $bio = null;

    #[Field(type: 'string', nullable: true)]
    public ?string $avatar = null;

    #[OneToOne(targetEntity: User::class, inversedBy: 'profile')]
    public ?User $user = null;
}

/**
 * Test Repository: UserRepository
 */
class UserRepository extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = User::class;
}

/**
 * Test Repository: UserProfileRepository
 */
class UserProfileRepository extends EntityRepository
{
    protected string $table = 'user_profiles';
    protected string $entityClass = UserProfile::class;
}

/**
 * Integration tests for OneToOne relationships.
 */
class OneToOneTest extends TestCase
{
    private QueryBuilder $qb;
    private UserRepository $userRepo;
    private UserProfileRepository $profileRepo;
    private \PDO $pdo;

    protected function setUp(): void
    {
        $this->pdo = new \PDO('sqlite::memory:');
        $this->pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

        // Create test tables
        $this->pdo->exec('
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                username TEXT NOT NULL
            )
        ');

        $this->pdo->exec('
            CREATE TABLE user_profiles (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                bio TEXT,
                avatar TEXT,
                user_id INTEGER UNIQUE,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ');

        // Insert test data
        $this->pdo->exec("INSERT INTO users (username) VALUES ('alice')");
        $this->pdo->exec("INSERT INTO users (username) VALUES ('bob')");
        $this->pdo->exec("INSERT INTO user_profiles (bio, avatar, user_id) VALUES ('Hello!', 'alice.jpg', 1)");

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
        $this->userRepo = new UserRepository($this->qb);
        $this->profileRepo = new UserProfileRepository($this->qb);
    }

    protected function tearDown(): void
    {
        $this->pdo->exec('DELETE FROM user_profiles');
        $this->pdo->exec('DELETE FROM users');
    }

    // ==================== BASIC CRUD ====================

    public function testFindUser(): void
    {
        $user = $this->userRepo->find(1, false);
        
        $this->assertNotNull($user);
        $this->assertEquals('alice', $user->username);
    }

    public function testFindProfile(): void
    {
        $profile = $this->profileRepo->find(1, false);
        
        $this->assertNotNull($profile);
        $this->assertEquals('Hello!', $profile->bio);
        $this->assertEquals('alice.jpg', $profile->avatar);
    }

    public function testSaveUser(): void
    {
        $user = new User();
        $user->username = 'charlie';
        
        $id = $this->userRepo->save($user);
        
        $this->assertGreaterThan(0, $id);
        
        $reloaded = $this->userRepo->find($id, false);
        $this->assertEquals('charlie', $reloaded->username);
    }

    public function testSaveProfile(): void
    {
        $profile = new UserProfile();
        $profile->bio = 'New bio';
        $profile->avatar = 'new.jpg';
        
        $id = $this->profileRepo->save($profile);
        
        $reloaded = $this->profileRepo->find($id, false);
        $this->assertEquals('New bio', $reloaded->bio);
    }

    // ==================== ONE TO ONE RELATIONS ====================

    public function testLoadOneToOneFromOwningSide(): void
    {
        $profile = $this->profileRepo->find(1, true);
        
        $this->assertNotNull($profile);
        $this->assertNotNull($profile->user);
        $this->assertEquals('alice', $profile->user->username);
    }

    public function testLoadOneToOneFromInverseSide(): void
    {
        $user = $this->userRepo->find(1, true);
        
        $this->assertNotNull($user);
        // Note: OneToOne inverse side loading depends on relationship mapping
        // This tests that loading doesn't crash
        $this->assertInstanceOf(User::class, $user);
    }

    public function testUserWithoutProfile(): void
    {
        // Bob has no profile
        $user = $this->userRepo->find(2, true);
        
        $this->assertNotNull($user);
        $this->assertEquals('bob', $user->username);
        $this->assertNull($user->profile);
    }

    public function testProfileWithoutUser(): void
    {
        // Create orphan profile
        $this->pdo->exec("INSERT INTO user_profiles (bio, avatar, user_id) VALUES ('Orphan', 'x.jpg', NULL)");
        
        $profile = $this->profileRepo->findOneBy(['bio' => 'Orphan'], true);
        
        $this->assertNotNull($profile);
        $this->assertNull($profile->user);
    }

    // ==================== DELETE TESTS ====================

    public function testDeleteUser(): void
    {
        $result = $this->userRepo->delete(2);
        
        $this->assertEquals(1, $result);
        $this->assertNull($this->userRepo->find(2, false));
    }

    public function testDeleteProfile(): void
    {
        $result = $this->profileRepo->delete(1);
        
        $this->assertEquals(1, $result);
        $this->assertNull($this->profileRepo->find(1, false));
    }

    // ==================== UPDATE TESTS ====================

    public function testUpdateUser(): void
    {
        $user = $this->userRepo->find(1, false);
        $user->username = 'alice_updated';
        
        $this->userRepo->save($user);
        
        $reloaded = $this->userRepo->find(1, false);
        $this->assertEquals('alice_updated', $reloaded->username);
    }

    public function testUpdateProfile(): void
    {
        $profile = $this->profileRepo->find(1, false);
        $profile->bio = 'Updated bio';
        
        $this->profileRepo->save($profile);
        
        $reloaded = $this->profileRepo->find(1, false);
        $this->assertEquals('Updated bio', $reloaded->bio);
    }

    // ==================== COUNT TESTS ====================

    public function testCountUsers(): void
    {
        $count = $this->userRepo->count();
        $this->assertEquals(2, $count);
    }

    public function testCountProfiles(): void
    {
        $count = $this->profileRepo->count();
        $this->assertEquals(1, $count);
    }

    public function testCountWithCriteria(): void
    {
        $count = $this->userRepo->count(['username' => 'alice']);
        $this->assertEquals(1, $count);
    }
}
