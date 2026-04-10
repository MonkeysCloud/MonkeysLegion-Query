<?php
declare(strict_types=1);

namespace Tests\Unit\Repository;

use MonkeysLegion\Query\Repository\IdentityMap;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(IdentityMap::class)]
final class IdentityMapTest extends TestCase
{
    public function testSetAndGet(): void
    {
        $map = new IdentityMap();
        $entity = new \stdClass();
        $entity->id = 1;

        $map->set('User', 1, $entity);

        self::assertTrue($map->has('User', 1));
        self::assertSame($entity, $map->get('User', 1));
    }

    public function testGetReturnsNullForUnknown(): void
    {
        $map = new IdentityMap();
        self::assertNull($map->get('User', 999));
        self::assertFalse($map->has('User', 999));
    }

    public function testSameIdDifferentClassDoesNotCollide(): void
    {
        $map = new IdentityMap();
        $user = new \stdClass();
        $order = new \stdClass();

        $map->set('User', 1, $user);
        $map->set('Order', 1, $order);

        self::assertSame($user, $map->get('User', 1));
        self::assertSame($order, $map->get('Order', 1));
        self::assertSame(2, $map->count());
    }

    public function testRemove(): void
    {
        $map = new IdentityMap();
        $entity = new \stdClass();

        $map->set('User', 1, $entity);
        $map->remove('User', 1);

        self::assertFalse($map->has('User', 1));
        self::assertNull($map->get('User', 1));
    }

    public function testIdentify(): void
    {
        $map = new IdentityMap();
        $entity = new \stdClass();

        $map->set('User', 42, $entity);

        $identity = $map->identify($entity);
        self::assertNotNull($identity);
        self::assertSame('User', $identity['class']);
        self::assertSame('42', $identity['id']);
    }

    public function testIdentifyReturnsNullForUntracked(): void
    {
        $map = new IdentityMap();
        self::assertNull($map->identify(new \stdClass()));
    }

    public function testAllOfClass(): void
    {
        $map = new IdentityMap();
        $e1 = new \stdClass();
        $e2 = new \stdClass();

        $map->set('User', 1, $e1);
        $map->set('User', 2, $e2);
        $map->set('Order', 1, new \stdClass());

        $users = $map->allOfClass('User');
        self::assertCount(2, $users);
    }

    public function testClear(): void
    {
        $map = new IdentityMap();
        $map->set('User', 1, new \stdClass());
        $map->set('User', 2, new \stdClass());

        $map->clear();

        self::assertSame(0, $map->count());
        self::assertFalse($map->has('User', 1));
    }

    public function testStringIds(): void
    {
        $map = new IdentityMap();
        $entity = new \stdClass();

        $map->set('User', 'uuid-123', $entity);
        self::assertTrue($map->has('User', 'uuid-123'));
        self::assertSame($entity, $map->get('User', 'uuid-123'));
    }
}
