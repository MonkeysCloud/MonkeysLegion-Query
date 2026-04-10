<?php
declare(strict_types=1);

namespace Tests\Unit\Repository;

use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Query\Repository\EntityHydrator;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

// ── Test Entity ──────────────────────────────────────────────────

class TestUser
{
    #[Field(type: 'integer')]
    public int $id;

    #[Field(type: 'string')]
    public string $name;

    #[Field(type: 'string')]
    public string $email;

    #[Field(type: 'integer')]
    public int $age;

    #[Field(type: 'boolean')]
    public bool $active;

    #[Field(type: 'json', nullable: true)]
    public ?array $metadata = null;

    #[Field(type: 'datetime', nullable: true)]
    public ?\DateTimeImmutable $created_at = null;
}

#[CoversClass(EntityHydrator::class)]
final class EntityHydratorTest extends TestCase
{
    private EntityHydrator $hydrator;

    protected function setUp(): void
    {
        EntityHydrator::clearCache();
        $this->hydrator = new EntityHydrator();
    }

    public function testHydrateBasicFields(): void
    {
        $entity = $this->hydrator->hydrate(TestUser::class, [
            'id'    => 1,
            'name'  => 'Alice',
            'email' => 'alice@test.com',
            'age'   => 30,
            'active' => 1,
        ]);

        self::assertInstanceOf(TestUser::class, $entity);
        self::assertSame(1, $entity->id);
        self::assertSame('Alice', $entity->name);
        self::assertSame('alice@test.com', $entity->email);
        self::assertSame(30, $entity->age);
        self::assertTrue($entity->active);
    }

    public function testHydrateJsonField(): void
    {
        $entity = $this->hydrator->hydrate(TestUser::class, [
            'id'       => 1,
            'name'     => 'Alice',
            'email'    => 'a@b.com',
            'age'      => 30,
            'active'   => 1,
            'metadata' => '{"role":"admin"}',
        ]);

        self::assertSame(['role' => 'admin'], $entity->metadata);
    }

    public function testHydrateDatetimeField(): void
    {
        $entity = $this->hydrator->hydrate(TestUser::class, [
            'id'         => 1,
            'name'       => 'Alice',
            'email'      => 'a@b.com',
            'age'        => 30,
            'active'     => 1,
            'created_at' => '2026-01-15 10:30:00',
        ]);

        self::assertInstanceOf(\DateTimeImmutable::class, $entity->created_at);
        self::assertSame('2026-01-15', $entity->created_at->format('Y-m-d'));
    }

    public function testDehydrate(): void
    {
        $entity = new TestUser();
        $entity->id = 1;
        $entity->name = 'Alice';
        $entity->email = 'alice@test.com';
        $entity->age = 30;
        $entity->active = true;
        $entity->metadata = ['role' => 'admin'];

        $data = $this->hydrator->dehydrate($entity);

        self::assertSame(1, $data['id']);
        self::assertSame('Alice', $data['name']);
        self::assertSame('alice@test.com', $data['email']);
        self::assertSame(30, $data['age']);
        self::assertSame(1, $data['active']); // bool → int
        self::assertSame('{"role":"admin"}', $data['metadata']); // array → json
    }

    public function testGetSetPropertyValue(): void
    {
        $entity = new TestUser();
        $this->hydrator->setPropertyValue($entity, 'name', 'Bob');
        self::assertSame('Bob', $entity->name);

        $value = $this->hydrator->getPropertyValue($entity, 'name');
        self::assertSame('Bob', $value);
    }

    public function testGetEntityId(): void
    {
        $entity = new TestUser();
        $entity->id = 42;
        self::assertSame(42, $this->hydrator->getEntityId($entity));
    }

    public function testGetEntityIdReturnsNullWhenUnset(): void
    {
        $entity = new TestUser();
        self::assertNull($this->hydrator->getEntityId($entity));
    }

    public function testGetColumns(): void
    {
        $columns = $this->hydrator->getColumns(TestUser::class);
        self::assertContains('id', $columns);
        self::assertContains('name', $columns);
        self::assertContains('email', $columns);
    }

    public function testHydrateSkipsMissingColumns(): void
    {
        $entity = $this->hydrator->hydrate(TestUser::class, [
            'id'   => 1,
            'name' => 'Alice',
        ]);

        self::assertSame(1, $entity->id);
        self::assertSame('Alice', $entity->name);
        // email is not initialized — accessing would throw
    }

    public function testNullHandling(): void
    {
        $entity = $this->hydrator->hydrate(TestUser::class, [
            'id'       => 1,
            'name'     => 'Alice',
            'email'    => 'a@b.com',
            'age'      => 30,
            'active'   => 1,
            'metadata' => null,
        ]);

        self::assertNull($entity->metadata);
    }
}
