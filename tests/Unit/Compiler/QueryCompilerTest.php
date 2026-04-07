<?php
declare(strict_types=1);

namespace Tests\Unit\Compiler;

use MonkeysLegion\Query\Clause\GroupByClause;
use MonkeysLegion\Query\Clause\HavingClause;
use MonkeysLegion\Query\Clause\JoinClause;
use MonkeysLegion\Query\Clause\LimitOffsetClause;
use MonkeysLegion\Query\Clause\OrderByClause;
use MonkeysLegion\Query\Clause\SelectClause;
use MonkeysLegion\Query\Clause\WhereClause;
use MonkeysLegion\Query\Compiler\MySqlGrammar;
use MonkeysLegion\Query\Compiler\PostgresGrammar;
use MonkeysLegion\Query\Compiler\QueryCompiler;
use MonkeysLegion\Query\Compiler\SqliteGrammar;
use MonkeysLegion\Query\Enums\JoinType;
use MonkeysLegion\Query\Enums\Operator;
use MonkeysLegion\Query\Enums\SortDirection;
use MonkeysLegion\Query\Enums\WhereBoolean;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;

#[CoversClass(QueryCompiler::class)]
#[CoversClass(MySqlGrammar::class)]
#[CoversClass(PostgresGrammar::class)]
#[CoversClass(SqliteGrammar::class)]
final class QueryCompilerTest extends TestCase
{
    protected function setUp(): void
    {
        QueryCompiler::clearCache();
    }

    // ── SELECT ──────────────────────────────────────────────────

    public function testSimpleSelect(): void
    {
        $result = QueryCompiler::compileSelect(
            select: new SelectClause(['id', 'name']),
            from: 'users',
        );

        self::assertSame('SELECT id, name FROM users', $result['sql']);
        self::assertSame([], $result['bindings']);
    }

    public function testSelectWithWhere(): void
    {
        $result = QueryCompiler::compileSelect(
            select: new SelectClause(),
            from: 'users',
            wheres: [
                new WhereClause('status', Operator::Equal, 'active'),
                new WhereClause('age', Operator::GreaterThan, 18, WhereBoolean::And),
            ],
        );

        self::assertSame('SELECT * FROM users WHERE status = ? AND age > ?', $result['sql']);
        self::assertSame(['active', 18], $result['bindings']);
    }

    public function testSelectWithJoin(): void
    {
        $result = QueryCompiler::compileSelect(
            select: new SelectClause(['u.id', 'u.name', 'o.total']),
            from: 'users u',
            joins: [
                new JoinClause(JoinType::Left, 'orders', 'o', ['u.id = o.user_id']),
            ],
        );

        self::assertSame(
            'SELECT u.id, u.name, o.total FROM users u LEFT JOIN orders AS o ON u.id = o.user_id',
            $result['sql'],
        );
    }

    public function testSelectWithOrderGroupLimit(): void
    {
        $result = QueryCompiler::compileSelect(
            select: new SelectClause(['status', 'COUNT(*) AS cnt']),
            from: 'users',
            orders: [new OrderByClause('cnt', SortDirection::Desc)],
            groupBy: new GroupByClause(['status']),
            havings: [new HavingClause('COUNT(*) > ?', [5])],
            limit: new LimitOffsetClause(limit: 10, offset: 20),
        );

        self::assertSame(
            'SELECT status, COUNT(*) AS cnt FROM users GROUP BY status HAVING COUNT(*) > ? ORDER BY cnt DESC LIMIT 10 OFFSET 20',
            $result['sql'],
        );
        self::assertSame([5], $result['bindings']);
    }

    public function testSelectWithWhereIn(): void
    {
        $result = QueryCompiler::compileSelect(
            select: new SelectClause(),
            from: 'users',
            wheres: [new WhereClause('id', Operator::In, [1, 2, 3])],
        );

        self::assertSame('SELECT * FROM users WHERE id IN (?, ?, ?)', $result['sql']);
        self::assertSame([1, 2, 3], $result['bindings']);
    }

    public function testSelectWithOrWhere(): void
    {
        $result = QueryCompiler::compileSelect(
            select: new SelectClause(),
            from: 'users',
            wheres: [
                new WhereClause('role', Operator::Equal, 'admin'),
                new WhereClause('role', Operator::Equal, 'super_admin', WhereBoolean::Or),
            ],
        );

        self::assertSame('SELECT * FROM users WHERE role = ? OR role = ?', $result['sql']);
        self::assertSame(['admin', 'super_admin'], $result['bindings']);
    }

    public function testDistinctSelect(): void
    {
        $result = QueryCompiler::compileSelect(
            select: new SelectClause(['status'], distinct: true),
            from: 'orders',
        );

        self::assertSame('SELECT DISTINCT status FROM orders', $result['sql']);
    }

    // ── Structural Cache ────────────────────────────────────────

    public function testStructuralCacheProducesSameSql(): void
    {
        $buildQuery = fn(string $status) => QueryCompiler::compileSelect(
            select: new SelectClause(),
            from: 'users',
            wheres: [new WhereClause('status', Operator::Equal, $status)],
        );

        $result1 = $buildQuery('active');
        $result2 = $buildQuery('inactive');

        // Same SQL template, different bindings
        self::assertSame($result1['sql'], $result2['sql']);
        self::assertSame(['active'], $result1['bindings']);
        self::assertSame(['inactive'], $result2['bindings']);
    }

    // ── INSERT ──────────────────────────────────────────────────

    public function testInsertSingleRow(): void
    {
        $grammar = new MySqlGrammar();
        $result = QueryCompiler::compileInsert(
            table: 'users',
            columns: ['name', 'email'],
            rows: [['Alice', 'alice@example.com']],
            grammar: $grammar,
        );

        self::assertSame('INSERT INTO `users` (`name`, `email`) VALUES (?, ?)', $result['sql']);
        self::assertSame(['Alice', 'alice@example.com'], $result['bindings']);
    }

    public function testInsertMultipleRows(): void
    {
        $grammar = new MySqlGrammar();
        $result = QueryCompiler::compileInsert(
            table: 'users',
            columns: ['name', 'email'],
            rows: [
                ['Alice', 'alice@example.com'],
                ['Bob', 'bob@example.com'],
            ],
            grammar: $grammar,
        );

        self::assertSame('INSERT INTO `users` (`name`, `email`) VALUES (?, ?), (?, ?)', $result['sql']);
        self::assertSame(['Alice', 'alice@example.com', 'Bob', 'bob@example.com'], $result['bindings']);
    }

    public function testInsertPostgres(): void
    {
        $grammar = new PostgresGrammar();
        $result = QueryCompiler::compileInsert(
            table: 'users',
            columns: ['name', 'email'],
            rows: [['Alice', 'alice@example.com']],
            grammar: $grammar,
        );

        self::assertSame('INSERT INTO "users" ("name", "email") VALUES (?, ?)', $result['sql']);
    }

    // ── UPDATE ──────────────────────────────────────────────────

    public function testUpdateWithWhere(): void
    {
        $grammar = new MySqlGrammar();
        $result = QueryCompiler::compileUpdate(
            table: 'users',
            values: ['name' => 'Alice Updated', 'status' => 'active'],
            wheres: [new WhereClause('id', Operator::Equal, 42)],
            grammar: $grammar,
        );

        self::assertSame('UPDATE `users` SET `name` = ?, `status` = ? WHERE id = ?', $result['sql']);
        self::assertSame(['Alice Updated', 'active', 42], $result['bindings']);
    }

    // ── DELETE ──────────────────────────────────────────────────

    public function testDeleteWithWhere(): void
    {
        $grammar = new SqliteGrammar();
        $result = QueryCompiler::compileDelete(
            table: 'sessions',
            wheres: [new WhereClause('expired_at', Operator::LessThan, '2026-01-01')],
            grammar: $grammar,
        );

        self::assertSame('DELETE FROM "sessions" WHERE expired_at < ?', $result['sql']);
        self::assertSame(['2026-01-01'], $result['bindings']);
    }

    // ── UPSERT ──────────────────────────────────────────────────

    public function testUpsertMySql(): void
    {
        $grammar = new MySqlGrammar();
        $result = QueryCompiler::compileUpsert(
            table: 'users',
            columns: ['id', 'name', 'email'],
            values: [1, 'Alice', 'alice@example.com'],
            updateColumns: ['name', 'email'],
            grammar: $grammar,
        );

        self::assertStringContainsString('ON DUPLICATE KEY UPDATE', $result['sql']);
        self::assertStringContainsString('`name` = VALUES(`name`)', $result['sql']);
    }

    public function testUpsertPostgres(): void
    {
        $grammar = new PostgresGrammar();
        $result = QueryCompiler::compileUpsert(
            table: 'users',
            columns: ['id', 'name', 'email'],
            values: [1, 'Alice', 'alice@example.com'],
            updateColumns: ['name', 'email'],
            conflictTarget: 'id',
            grammar: $grammar,
        );

        self::assertStringContainsString('ON CONFLICT ("id") DO UPDATE SET', $result['sql']);
        self::assertStringContainsString('"name" = EXCLUDED."name"', $result['sql']);
    }

    public function testUpsertSqlite(): void
    {
        $grammar = new SqliteGrammar();
        $result = QueryCompiler::compileUpsert(
            table: 'users',
            columns: ['id', 'name', 'email'],
            values: [1, 'Alice', 'alice@example.com'],
            updateColumns: ['name', 'email'],
            conflictTarget: 'id',
            grammar: $grammar,
        );

        self::assertStringContainsString('ON CONFLICT ("id") DO UPDATE SET', $result['sql']);
        self::assertStringContainsString('"name" = excluded."name"', $result['sql']);
    }

    // ── Grammar: Identifier Quoting ─────────────────────────────

    public function testMySqlQuoteIdentifier(): void
    {
        $grammar = new MySqlGrammar();
        self::assertSame('`users`', $grammar->quoteIdentifier('users'));
        self::assertSame('`schema`.`users`', $grammar->quoteIdentifier('schema.users'));
    }

    public function testPostgresQuoteIdentifier(): void
    {
        $grammar = new PostgresGrammar();
        self::assertSame('"users"', $grammar->quoteIdentifier('users'));
        self::assertSame('"schema"."users"', $grammar->quoteIdentifier('schema.users'));
    }

    public function testSqliteQuoteIdentifier(): void
    {
        $grammar = new SqliteGrammar();
        self::assertSame('"users"', $grammar->quoteIdentifier('users'));
    }

    // ── Grammar: JSON Path ──────────────────────────────────────

    public function testMySqlJsonPath(): void
    {
        $grammar = new MySqlGrammar();
        self::assertSame("data->>'$.name'", $grammar->compileJsonPath('data', 'name'));
        self::assertSame("data->>'$.address.city'", $grammar->compileJsonPath('data', 'address.city'));
    }

    public function testPostgresJsonPath(): void
    {
        $grammar = new PostgresGrammar();
        self::assertSame("data->>'name'", $grammar->compileJsonPath('data', 'name'));
        self::assertSame("data#>>'{address,city}'", $grammar->compileJsonPath('data', 'address.city'));
    }

    public function testSqliteJsonPath(): void
    {
        $grammar = new SqliteGrammar();
        self::assertSame("json_extract(data, '$.name')", $grammar->compileJsonPath('data', 'name'));
    }

    // ── Grammar: RETURNING ──────────────────────────────────────

    public function testMySqlNoReturning(): void
    {
        $grammar = new MySqlGrammar();
        self::assertFalse($grammar->supportsReturning());
        self::assertSame('', $grammar->compileReturning(['id']));
    }

    public function testPostgresReturning(): void
    {
        $grammar = new PostgresGrammar();
        self::assertTrue($grammar->supportsReturning());
        self::assertSame('RETURNING *', $grammar->compileReturning([]));
        self::assertSame('RETURNING "id", "name"', $grammar->compileReturning(['id', 'name']));
    }

    public function testSqliteReturning(): void
    {
        $grammar = new SqliteGrammar();
        self::assertTrue($grammar->supportsReturning());
        self::assertSame('RETURNING *', $grammar->compileReturning(['*']));
    }

    // ── Grammar: LIMIT/OFFSET ───────────────────────────────────

    public function testSqliteLimitWithOffsetOnly(): void
    {
        $grammar = new SqliteGrammar();
        // SQLite requires LIMIT before OFFSET
        self::assertStringContainsString('LIMIT -1', $grammar->compileLimit(null, 10));
    }
}
