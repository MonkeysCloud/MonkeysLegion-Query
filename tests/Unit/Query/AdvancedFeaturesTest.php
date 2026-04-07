<?php
declare(strict_types=1);

namespace Tests\Unit\Query;

use MonkeysLegion\Database\Types\DatabaseDriver;
use MonkeysLegion\Query\Attributes\Scope;
use MonkeysLegion\Query\Query\CteBuilder;
use MonkeysLegion\Query\Query\VectorSearch;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(CteBuilder::class)]
#[CoversClass(VectorSearch::class)]
#[CoversClass(Scope::class)]
final class AdvancedFeaturesTest extends TestCase
{
    // ── CTE Builder ─────────────────────────────────────────────

    public function testStandardCte(): void
    {
        $cte = new CteBuilder();
        $cte->add('active_users', 'SELECT * FROM users WHERE status = ?', ['active']);

        self::assertTrue($cte->hasCtes());
        self::assertFalse($cte->isRecursive());
        self::assertSame(
            "WITH active_users AS (SELECT * FROM users WHERE status = ?)",
            $cte->toSql(),
        );
        self::assertSame(['active'], $cte->getBindings());
    }

    public function testRecursiveCte(): void
    {
        $cte = new CteBuilder();
        $cte->add(
            name: 'category_tree',
            sql: 'SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL '
                . 'UNION ALL '
                . 'SELECT c.id, c.parent_id, c.name FROM categories c JOIN category_tree ct ON c.parent_id = ct.id',
            recursive: true,
            columns: ['id', 'parent_id', 'name'],
        );

        $sql = $cte->toSql();
        self::assertTrue($cte->isRecursive());
        self::assertStringStartsWith('WITH RECURSIVE', $sql);
        self::assertStringContainsString('category_tree(id, parent_id, name)', $sql);
    }

    public function testMultipleCtes(): void
    {
        $cte = new CteBuilder();
        $cte->add('active', 'SELECT * FROM users WHERE status = ?', ['active']);
        $cte->add('recent', 'SELECT * FROM active WHERE created_at > ?', ['2026-01-01']);

        $sql = $cte->toSql();
        self::assertStringContainsString('active AS (', $sql);
        self::assertStringContainsString(', recent AS (', $sql);
        self::assertSame(['active', '2026-01-01'], $cte->getBindings());
    }

    public function testEmptyCteReturnsEmptyString(): void
    {
        $cte = new CteBuilder();
        self::assertFalse($cte->hasCtes());
        self::assertSame('', $cte->toSql());
    }

    // ── Vector Search ───────────────────────────────────────────

    public function testPostgresL2Distance(): void
    {
        $expr = VectorSearch::distance('embedding', [1.0, 2.0, 3.0], DatabaseDriver::PostgreSQL);
        self::assertStringContainsString('<->', $expr->toSql());
        self::assertStringContainsString('[1,2,3]', $expr->toSql());
    }

    public function testPostgresCosineDistance(): void
    {
        $expr = VectorSearch::distance('embedding', [1.0, 2.0], DatabaseDriver::PostgreSQL, 'cosine');
        self::assertStringContainsString('<=>', $expr->toSql());
    }

    public function testPostgresInnerProductDistance(): void
    {
        $expr = VectorSearch::distance('embedding', [1.0], DatabaseDriver::PostgreSQL, 'inner_product');
        self::assertStringContainsString('<#>', $expr->toSql());
    }

    public function testMySqlL2Distance(): void
    {
        $expr = VectorSearch::distance('embedding', [1.0, 2.0], DatabaseDriver::MySQL);
        self::assertStringContainsString('VEC_DISTANCE_L2', $expr->toSql());
    }

    public function testMySqlCosineDistance(): void
    {
        $expr = VectorSearch::distance('embedding', [1.0, 2.0], DatabaseDriver::MySQL, 'cosine');
        self::assertStringContainsString('VEC_DISTANCE_COSINE', $expr->toSql());
    }

    public function testSqliteFallback(): void
    {
        $expr = VectorSearch::distance('embedding', [1.0, 2.0], DatabaseDriver::SQLite);
        // SQLite doesn't have native vector support — returns placeholder
        self::assertNotEmpty($expr->toSql());
    }

    // ── Scope Attribute ─────────────────────────────────────────

    public function testScopeDefaultValues(): void
    {
        $scope = new Scope();
        self::assertFalse($scope->isGlobal);
        self::assertNull($scope->name);
    }

    public function testScopeGlobal(): void
    {
        $scope = new Scope(isGlobal: true, name: 'notDeleted');
        self::assertTrue($scope->isGlobal);
        self::assertSame('notDeleted', $scope->name);
    }

    public function testScopeAttributeOnMethod(): void
    {
        $ref = new \ReflectionMethod(ScopedRepo::class, 'active');
        $attrs = $ref->getAttributes(Scope::class);

        self::assertCount(1, $attrs);

        /** @var Scope $scope */
        $scope = $attrs[0]->newInstance();
        self::assertFalse($scope->isGlobal);
    }

    public function testGlobalScopeAttributeOnMethod(): void
    {
        $ref = new \ReflectionMethod(ScopedRepo::class, 'notDeleted');
        $attrs = $ref->getAttributes(Scope::class);

        /** @var Scope $scope */
        $scope = $attrs[0]->newInstance();
        self::assertTrue($scope->isGlobal);
        self::assertSame('softDelete', $scope->name);
    }
}

// ── Test fixture ──────────────────────────────────────────────

class ScopedRepo
{
    #[Scope]
    public function active(): void {}

    #[Scope(isGlobal: true, name: 'softDelete')]
    public function notDeleted(): void {}
}
