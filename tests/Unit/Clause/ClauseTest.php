<?php
declare(strict_types=1);

namespace Tests\Unit\Clause;

use MonkeysLegion\Query\Clause\GroupByClause;
use MonkeysLegion\Query\Clause\HavingClause;
use MonkeysLegion\Query\Clause\JoinClause;
use MonkeysLegion\Query\Clause\LimitOffsetClause;
use MonkeysLegion\Query\Clause\OrderByClause;
use MonkeysLegion\Query\Clause\SelectClause;
use MonkeysLegion\Query\Clause\UnionClause;
use MonkeysLegion\Query\Clause\WhereClause;
use MonkeysLegion\Query\Enums\JoinType;
use MonkeysLegion\Query\Enums\Operator;
use MonkeysLegion\Query\Enums\SortDirection;
use MonkeysLegion\Query\Enums\WhereBoolean;
use MonkeysLegion\Query\RawExpression;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(SelectClause::class)]
#[CoversClass(WhereClause::class)]
#[CoversClass(JoinClause::class)]
#[CoversClass(OrderByClause::class)]
#[CoversClass(GroupByClause::class)]
#[CoversClass(LimitOffsetClause::class)]
#[CoversClass(UnionClause::class)]
#[CoversClass(HavingClause::class)]
final class ClauseTest extends TestCase
{
    // ── SelectClause ────────────────────────────────────────────

    public function testSelectAllColumns(): void
    {
        $clause = new SelectClause();
        self::assertSame('*', $clause->toSql());
        self::assertSame([], $clause->getBindings());
    }

    public function testSelectSpecificColumns(): void
    {
        $clause = new SelectClause(['id', 'name', 'email']);
        self::assertSame('id, name, email', $clause->toSql());
    }

    public function testSelectDistinct(): void
    {
        $clause = new SelectClause(['status'], distinct: true);
        self::assertSame('DISTINCT status', $clause->toSql());
    }

    public function testSelectWithExpression(): void
    {
        $raw = new RawExpression('COUNT(*) AS total');
        $clause = new SelectClause(['id', $raw]);
        self::assertSame('id, COUNT(*) AS total', $clause->toSql());
    }

    // ── WhereClause ─────────────────────────────────────────────

    public function testWhereEqual(): void
    {
        $where = new WhereClause('status', Operator::Equal, 'active');
        self::assertSame('status = ?', $where->toSql());
        self::assertSame(['active'], $where->getBindings());
    }

    public function testWhereIsNull(): void
    {
        $where = new WhereClause('deleted_at', Operator::IsNull);
        self::assertSame('deleted_at IS NULL', $where->toSql());
        self::assertSame([], $where->getBindings());
    }

    public function testWhereIn(): void
    {
        $where = new WhereClause('id', Operator::In, [1, 2, 3]);
        self::assertSame('id IN (?, ?, ?)', $where->toSql());
        self::assertSame([1, 2, 3], $where->getBindings());
    }

    public function testWhereBetween(): void
    {
        $where = new WhereClause('age', Operator::Between, [18, 65]);
        self::assertSame('age BETWEEN ? AND ?', $where->toSql());
        self::assertSame([18, 65], $where->getBindings());
    }

    public function testWhereWithOrBoolean(): void
    {
        $where = new WhereClause('role', Operator::Equal, 'admin', WhereBoolean::Or);
        self::assertSame('role = ?', $where->toSql());
        self::assertSame(WhereBoolean::Or, $where->boolean);
    }

    public function testWhereExists(): void
    {
        $sub = new RawExpression('SELECT 1 FROM orders WHERE orders.user_id = users.id');
        $where = new WhereClause('', Operator::Exists, $sub);
        self::assertSame('EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)', $where->toSql());
        self::assertSame([], $where->getBindings());
    }

    // ── JoinClause ──────────────────────────────────────────────

    public function testInnerJoin(): void
    {
        $join = new JoinClause(
            type: JoinType::Inner,
            table: 'orders',
            alias: 'o',
            conditions: ['users.id = o.user_id'],
        );
        self::assertSame('INNER JOIN orders AS o ON users.id = o.user_id', $join->toSql());
    }

    public function testLeftJoinWithoutAlias(): void
    {
        $join = new JoinClause(
            type: JoinType::Left,
            table: 'profiles',
            conditions: ['users.id = profiles.user_id'],
        );
        self::assertSame('LEFT JOIN profiles ON users.id = profiles.user_id', $join->toSql());
    }

    public function testCrossJoinNoConditions(): void
    {
        $join = new JoinClause(type: JoinType::Cross, table: 'colors');
        self::assertSame('CROSS JOIN colors', $join->toSql());
    }

    // ── OrderByClause ───────────────────────────────────────────

    public function testOrderByAsc(): void
    {
        $order = new OrderByClause('name');
        self::assertSame('name ASC', $order->toSql());
    }

    public function testOrderByDesc(): void
    {
        $order = new OrderByClause('created_at', SortDirection::Desc);
        self::assertSame('created_at DESC', $order->toSql());
    }

    // ── GroupByClause ───────────────────────────────────────────

    public function testGroupBy(): void
    {
        $group = new GroupByClause(['status', 'country']);
        self::assertSame('status, country', $group->toSql());
        self::assertSame([], $group->getBindings());
    }

    // ── LimitOffsetClause ───────────────────────────────────────

    public function testLimitOnly(): void
    {
        $clause = new LimitOffsetClause(limit: 25);
        self::assertSame('LIMIT 25', $clause->toSql());
    }

    public function testLimitAndOffset(): void
    {
        $clause = new LimitOffsetClause(limit: 25, offset: 50);
        self::assertSame('LIMIT 25 OFFSET 50', $clause->toSql());
    }

    public function testOffsetOnly(): void
    {
        $clause = new LimitOffsetClause(offset: 10);
        self::assertSame('OFFSET 10', $clause->toSql());
    }

    public function testNullLimitAndOffset(): void
    {
        $clause = new LimitOffsetClause();
        self::assertSame('', $clause->toSql());
    }

    // ── UnionClause ─────────────────────────────────────────────

    public function testUnion(): void
    {
        $union = new UnionClause('SELECT id FROM archived_users');
        self::assertSame('UNION SELECT id FROM archived_users', $union->toSql());
    }

    public function testUnionAll(): void
    {
        $union = new UnionClause('SELECT id FROM archived_users', all: true);
        self::assertSame('UNION ALL SELECT id FROM archived_users', $union->toSql());
    }

    // ── HavingClause ────────────────────────────────────────────

    public function testHaving(): void
    {
        $having = new HavingClause('COUNT(*) > ?', [5]);
        self::assertSame('COUNT(*) > ?', $having->toSql());
        self::assertSame([5], $having->getBindings());
    }
}
