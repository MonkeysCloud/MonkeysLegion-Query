<?php

declare(strict_types=1);

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Repository\HydrationContext;

/**
 * Tests for HydrationContext with WeakMap (P2-E).
 */
class HydrationContextTest extends TestCase
{
    public function testRegisterAndRetrieveInstance(): void
    {
        $ctx = new HydrationContext(3);

        $entity = new \stdClass();
        $entity->id = 1;

        $ctx->registerInstance('App\\Entity\\User', '1', $entity);

        $this->assertTrue($ctx->hasInstance('App\\Entity\\User', '1'));
        $this->assertSame($entity, $ctx->getInstance('App\\Entity\\User', '1'));
    }

    public function testHasInstanceReturnsFalseForMissing(): void
    {
        $ctx = new HydrationContext(2);

        $this->assertFalse($ctx->hasInstance('App\\Entity\\User', '999'));
    }

    public function testGetInstanceReturnsNullForMissing(): void
    {
        $ctx = new HydrationContext(2);

        $this->assertNull($ctx->getInstance('App\\Entity\\User', '999'));
    }

    public function testSetAndGetDepth(): void
    {
        $ctx = new HydrationContext(3);

        $entity = new \stdClass();
        $ctx->setDepth($entity, 2);

        $this->assertEquals(2, $ctx->getDepth($entity));
    }

    public function testDefaultDepthIsZero(): void
    {
        $ctx = new HydrationContext(3);

        $entity = new \stdClass();
        $this->assertEquals(0, $ctx->getDepth($entity));
    }

    public function testMaxDepthProperty(): void
    {
        $ctx = new HydrationContext(5);
        $this->assertEquals(5, $ctx->maxDepth);
    }

    public function testDefaultMaxDepth(): void
    {
        $ctx = new HydrationContext();
        $this->assertEquals(2, $ctx->maxDepth);
    }

    public function testMultipleEntitiesSameClass(): void
    {
        $ctx = new HydrationContext(2);

        $e1 = new \stdClass();
        $e2 = new \stdClass();

        $ctx->registerInstance('App\\Entity\\User', '1', $e1);
        $ctx->registerInstance('App\\Entity\\User', '2', $e2);

        $this->assertSame($e1, $ctx->getInstance('App\\Entity\\User', '1'));
        $this->assertSame($e2, $ctx->getInstance('App\\Entity\\User', '2'));
    }

    public function testDifferentClassesSameId(): void
    {
        $ctx = new HydrationContext(2);

        $user = new \stdClass();
        $post = new \stdClass();

        $ctx->registerInstance('App\\Entity\\User', '1', $user);
        $ctx->registerInstance('App\\Entity\\Post', '1', $post);

        $this->assertSame($user, $ctx->getInstance('App\\Entity\\User', '1'));
        $this->assertSame($post, $ctx->getInstance('App\\Entity\\Post', '1'));
    }

    public function testWeakMapAllowsGarbageCollection(): void
    {
        $ctx = new HydrationContext(2);

        $entity = new \stdClass();
        $ctx->setDepth($entity, 1);

        $this->assertEquals(1, $ctx->getDepth($entity));

        // After unsetting the reference, WeakMap should allow GC
        // (we can't easily verify GC happened, but we can verify the API works)
        unset($entity);

        // Create a new entity and verify it gets depth 0 (default)
        $newEntity = new \stdClass();
        $this->assertEquals(0, $ctx->getDepth($newEntity));
    }
}
