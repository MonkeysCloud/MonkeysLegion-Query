<?php
declare(strict_types=1);

namespace Tests\Unit\Query;

use MonkeysLegion\Database\Types\DatabaseDriver;
use MonkeysLegion\Query\Attributes\Scope;
use MonkeysLegion\Query\Clause\HavingClause;
use MonkeysLegion\Query\Clause\OrderByClause;
use MonkeysLegion\Query\Clause\WhereClause;
use MonkeysLegion\Query\Compiler\MySqlGrammar;
use MonkeysLegion\Query\Compiler\PostgresGrammar;
use MonkeysLegion\Query\Compiler\QueryCompiler;
use MonkeysLegion\Query\Compiler\SqliteGrammar;
use MonkeysLegion\Query\Enums\JoinType;
use MonkeysLegion\Query\Enums\Operator;
use MonkeysLegion\Query\Enums\SortDirection;
use MonkeysLegion\Query\Enums\WhereBoolean;
use MonkeysLegion\Query\Query\QueryBuilder;
use MonkeysLegion\Query\Query\VectorSearch;
use MonkeysLegion\Query\RawExpression;
use MonkeysLegion\Query\Repository\EntityHydrator;
use MonkeysLegion\Query\Repository\EntityRepository;
use MonkeysLegion\Entity\Attributes\Field;
use PDO;
use PHPUnit\Framework\TestCase;
use Tests\Support\TestConnection;
use Tests\Support\TestConnectionManager;

/**
 * Additional tests for the latest pulled changes covering:
 * - New enum cases & validation (Operator::Raw, Operator::Group, JoinType::Full)
 * - WhereClause group/raw rendering
 * - HavingClause boolean connector
 * - OrderByClause bindings
 * - Grammar: insertOrIgnore, truncate, lock
 * - QueryBuilder: orWhere*, whereGroup, whereExists, fullJoin, chunk, lazy, chunkById,
 *   lockForUpdate, sharedLock, returning, withCte, insertOrIgnore, truncate, orHaving,
 *   orWhereRaw, first/exists non-mutating
 * - VectorSearch: validation, parameterized bindings
 * - EntityRepository: refresh(), global scopes, paginate clone fix
 */
final class PulledChangesTest extends TestCase
{
    private PDO $pdo;
    private TestConnection $conn;
    private TestConnectionManager $manager;

    protected function setUp(): void
    {
        QueryBuilder::clearStatementCache();
        EntityHydrator::clearCache();

        $this->pdo = new PDO('sqlite::memory:');
        $this->pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $this->pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, status TEXT, age INTEGER)');
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Alice', 'alice@test.com', 'active', 30)");
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Bob', 'bob@test.com', 'inactive', 25)");
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Charlie', 'charlie@test.com', 'active', 35)");
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Diana', 'diana@test.com', 'active', 28)");
        $this->pdo->exec("INSERT INTO users (name, email, status, age) VALUES ('Eve', 'eve@test.com', 'banned', 22)");

        $this->conn = new TestConnection($this->pdo);
        $this->manager = new TestConnectionManager($this->conn);
    }

    private function qb(): QueryBuilder
    {
        return (new QueryBuilder($this->manager))->from('users');
    }

    // ══════════════════════════════════════════════════════════════
    // ── Enum Changes ─────────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testOperatorRawDoesNotRequireValue(): void
    {
        self::assertFalse(Operator::Raw->requiresValue());
    }

    public function testOperatorGroupDoesNotRequireValue(): void
    {
        self::assertFalse(Operator::Group->requiresValue());
    }

    public function testOperatorFromLooseThrowsOnUnknown(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Unknown SQL operator');
        Operator::fromLoose('INVALID_OP');
    }

    public function testJoinTypeFullOuterValue(): void
    {
        self::assertSame('FULL OUTER', JoinType::Full->value);
        self::assertSame('Full Outer Join', JoinType::Full->label());
    }

    public function testSortDirectionFromLooseThrowsOnBadInput(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        SortDirection::fromLoose('RANDOM');
    }

    // ══════════════════════════════════════════════════════════════
    // ── WhereClause: Raw & Group ─────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereClauseRawOperator(): void
    {
        $clause = new WhereClause(
            column: 'age > 18 AND status = ?',
            operator: Operator::Raw,
            value: null,
            boolean: WhereBoolean::And,
            bindings: ['active'],
        );

        self::assertSame('age > 18 AND status = ?', $clause->toSql());
        self::assertSame(['active'], $clause->getBindings());
    }

    public function testWhereClauseGroupOperator(): void
    {
        $nested = [
            new WhereClause('status', Operator::Equal, 'active', WhereBoolean::And),
            new WhereClause('age', Operator::GreaterThan, 25, WhereBoolean::Or),
        ];

        $clause = new WhereClause(
            column: '',
            operator: Operator::Group,
            value: $nested,
            boolean: WhereBoolean::And,
        );

        $sql = $clause->toSql();
        self::assertStringStartsWith('(', $sql);
        self::assertStringEndsWith(')', $sql);
        self::assertStringContainsString('status = ?', $sql);
        self::assertStringContainsString('OR age > ?', $sql);

        $bindings = $clause->getBindings();
        self::assertSame(['active', 25], $bindings);
    }

    // ══════════════════════════════════════════════════════════════
    // ── HavingClause: Boolean ────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testHavingClauseDefaultBoolean(): void
    {
        $clause = new HavingClause('COUNT(*) > ?', [5]);
        self::assertSame(WhereBoolean::And, $clause->boolean);
    }

    public function testHavingClauseOrBoolean(): void
    {
        $clause = new HavingClause('SUM(amount) > ?', [100], WhereBoolean::Or);
        self::assertSame(WhereBoolean::Or, $clause->boolean);
    }

    // ══════════════════════════════════════════════════════════════
    // ── OrderByClause: Bindings ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testOrderByClauseWithBindings(): void
    {
        $clause = new OrderByClause(
            column: 'embedding <-> ?::vector',
            direction: SortDirection::Asc,
            bindings: ['[1,2,3]'],
        );

        self::assertSame('embedding <-> ?::vector ASC', $clause->toSql());
        self::assertSame(['[1,2,3]'], $clause->getBindings());
    }

    // ══════════════════════════════════════════════════════════════
    // ── Grammar: insertOrIgnore ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testMySqlInsertOrIgnore(): void
    {
        $grammar = new MySqlGrammar();
        $sql = $grammar->compileInsertOrIgnore('users', ['name', 'email'], ['?', '?']);
        self::assertStringStartsWith('INSERT IGNORE INTO', $sql);
        self::assertStringContainsString('`name`', $sql);
    }

    public function testPostgresInsertOrIgnore(): void
    {
        $grammar = new PostgresGrammar();
        $sql = $grammar->compileInsertOrIgnore('users', ['name'], ['?']);
        self::assertStringContainsString('ON CONFLICT DO NOTHING', $sql);
    }

    public function testSqliteInsertOrIgnore(): void
    {
        $grammar = new SqliteGrammar();
        $sql = $grammar->compileInsertOrIgnore('users', ['name'], ['?']);
        self::assertStringStartsWith('INSERT OR IGNORE INTO', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── Grammar: truncate ────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testMySqlTruncate(): void
    {
        self::assertSame('TRUNCATE TABLE `users`', (new MySqlGrammar())->compileTruncate('users'));
    }

    public function testPostgresTruncate(): void
    {
        self::assertSame('TRUNCATE TABLE "users"', (new PostgresGrammar())->compileTruncate('users'));
    }

    public function testSqliteTruncateUsesDelete(): void
    {
        self::assertSame('DELETE FROM "users"', (new SqliteGrammar())->compileTruncate('users'));
    }

    // ══════════════════════════════════════════════════════════════
    // ── Grammar: lock ────────────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testMySqlLockForUpdate(): void
    {
        self::assertSame('FOR UPDATE', (new MySqlGrammar())->compileLock('update'));
    }

    public function testMySqlLockForShare(): void
    {
        self::assertSame('LOCK IN SHARE MODE', (new MySqlGrammar())->compileLock('share'));
    }

    public function testMySqlLockNoWait(): void
    {
        self::assertSame('FOR UPDATE NOWAIT', (new MySqlGrammar())->compileLock('update', true));
    }

    public function testPostgresLockForUpdate(): void
    {
        self::assertSame('FOR UPDATE', (new PostgresGrammar())->compileLock('update'));
    }

    public function testPostgresLockForShare(): void
    {
        self::assertSame('FOR SHARE', (new PostgresGrammar())->compileLock('share'));
    }

    public function testPostgresLockNoWait(): void
    {
        self::assertSame('FOR UPDATE NOWAIT', (new PostgresGrammar())->compileLock('update', true));
    }

    public function testSqliteLockReturnsEmpty(): void
    {
        self::assertSame('', (new SqliteGrammar())->compileLock('update'));
    }

    // ══════════════════════════════════════════════════════════════
    // ── Compiler: insertOrIgnore & truncate ──────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testCompilerInsertOrIgnore(): void
    {
        $compiled = QueryCompiler::compileInsertOrIgnore(
            'users',
            ['name', 'email'],
            [['Alice', 'alice@test.com']],
            new SqliteGrammar(),
        );

        self::assertStringStartsWith('INSERT OR IGNORE INTO', $compiled['sql']);
        self::assertSame(['Alice', 'alice@test.com'], $compiled['bindings']);
    }

    public function testCompilerInsertOrIgnoreMultiRow(): void
    {
        $compiled = QueryCompiler::compileInsertOrIgnore(
            'users',
            ['name'],
            [['Alice'], ['Bob']],
            new SqliteGrammar(),
        );

        self::assertStringContainsString('VALUES (?), (?)', $compiled['sql']);
        self::assertSame(['Alice', 'Bob'], $compiled['bindings']);
    }

    public function testCompilerTruncate(): void
    {
        $compiled = QueryCompiler::compileTruncate('users', new SqliteGrammar());
        self::assertSame('DELETE FROM "users"', $compiled['sql']);
        self::assertSame([], $compiled['bindings']);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: OR WHERE variants ──────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testOrWhere(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhere('role', '=', 'admin')
            ->toSql();

        self::assertStringContainsString('OR', $sql);
    }

    public function testOrWhereIn(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereIn('id', [1, 2, 3])
            ->toSql();

        self::assertStringContainsString('OR', $sql);
        self::assertStringContainsString('IN', $sql);
    }

    public function testOrWhereNotIn(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereNotIn('id', [99])
            ->toSql();

        self::assertStringContainsString('OR', $sql);
        self::assertStringContainsString('NOT IN', $sql);
    }

    public function testOrWhereBetween(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereBetween('age', 20, 30)
            ->toSql();

        self::assertStringContainsString('OR', $sql);
        self::assertStringContainsString('BETWEEN', $sql);
    }

    public function testOrWhereNull(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereNull('deleted_at')
            ->toSql();

        self::assertStringContainsString('OR', $sql);
        self::assertStringContainsString('IS NULL', $sql);
    }

    public function testOrWhereNotNull(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereNotNull('email')
            ->toSql();

        self::assertStringContainsString('OR', $sql);
        self::assertStringContainsString('IS NOT NULL', $sql);
    }

    public function testOrWhereRaw(): void
    {
        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereRaw('age > ?', [25])
            ->toSql();

        self::assertStringContainsString('OR', $sql);
        self::assertStringContainsString('age > ?', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: whereGroup ─────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereGroup(): void
    {
        $compiled = $this->qb()
            ->where('status', '=', 'active')
            ->whereGroup(fn(QueryBuilder $inner) => $inner
                ->where('age', '>', 25)
                ->orWhere('name', '=', 'Alice')
            )
            ->compile();

        $sql = $compiled['sql'];
        self::assertStringContainsString('(', $sql);
        self::assertStringContainsString(')', $sql);
        self::assertStringContainsString('age > ?', $sql);
        self::assertStringContainsString('OR name = ?', $sql);
        self::assertSame(['active', 25, 'Alice'], $compiled['bindings']);
    }

    public function testOrWhereGroup(): void
    {
        $compiled = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereGroup(fn(QueryBuilder $inner) => $inner
                ->where('age', '<', 20)
                ->where('name', '=', 'Bob')
            )
            ->compile();

        $sql = $compiled['sql'];
        self::assertStringContainsString('OR (', $sql);
    }

    public function testWhereGroupExecution(): void
    {
        $results = $this->qb()
            ->where('status', '=', 'active')
            ->whereGroup(fn(QueryBuilder $inner) => $inner
                ->where('age', '>', 30)
                ->orWhere('name', '=', 'Diana')
            )
            ->get();

        // Alice(30, active), Charlie(35, active), Diana(28, active)
        // After group: (age > 30 → Charlie) OR (name = Diana → Diana) → 2 results
        self::assertCount(2, $results);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: whereExists ────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereExistsCompiles(): void
    {
        $subQuery = new RawExpression('SELECT 1 FROM orders WHERE orders.user_id = users.id');

        $sql = $this->qb()
            ->whereExists($subQuery)
            ->toSql();

        self::assertStringContainsString('EXISTS', $sql);
    }

    public function testOrWhereExistsCompiles(): void
    {
        $subQuery = new RawExpression('SELECT 1 FROM orders WHERE orders.user_id = users.id');

        $sql = $this->qb()
            ->where('status', '=', 'active')
            ->orWhereExists($subQuery)
            ->toSql();

        self::assertStringContainsString('OR', $sql);
        self::assertStringContainsString('EXISTS', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: whereIn validation ─────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWhereInRejectsAssociativeArray(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('whereIn()');
        $this->qb()->whereIn('id', ['a' => 1, 'b' => 2]);
    }

    public function testWhereNotInRejectsAssociativeArray(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('whereNotIn()');
        $this->qb()->whereNotIn('id', ['a' => 1]);
    }

    public function testOrWhereInRejectsAssociativeArray(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->qb()->orWhereIn('id', ['x' => 1]);
    }

    public function testOrWhereNotInRejectsAssociativeArray(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->qb()->orWhereNotIn('id', ['x' => 1]);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: fullJoin ───────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testFullJoinCompiles(): void
    {
        $sql = $this->qb()
            ->fullJoin('orders', fn($j) => $j->on('users.id', '=', 'orders.user_id'), 'o')
            ->toSql();

        self::assertStringContainsString('FULL OUTER JOIN', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: orHaving ───────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testOrHavingCompiles(): void
    {
        $compiled = $this->qb()
            ->select([new RawExpression('status'), new RawExpression('COUNT(*) as cnt')])
            ->groupBy('status')
            ->having('COUNT(*) > ?', [1])
            ->orHaving('COUNT(*) < ?', [10])
            ->compile();

        $sql = $compiled['sql'];
        self::assertStringContainsString('HAVING', $sql);
        self::assertStringContainsString('OR', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: first() / exists() non-mutating ────────────
    // ══════════════════════════════════════════════════════════════

    public function testFirstDoesNotMutateBuilder(): void
    {
        $qb = $this->qb()->where('status', '=', 'active');

        // Call first() — should NOT affect the builder's limit
        $qb->first();

        // Use same builder for a full get() — should return all active (3 rows)
        $all = $qb->get();
        self::assertCount(3, $all);
    }

    public function testExistsDoesNotMutateBuilder(): void
    {
        $qb = $this->qb()->where('status', '=', 'active');

        $result = $qb->exists();
        self::assertTrue($result);

        // Builder should still be reusable without limit
        $all = $qb->get();
        self::assertCount(3, $all);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: chunk / lazy / chunkById ───────────────────
    // ══════════════════════════════════════════════════════════════

    public function testChunk(): void
    {
        $batches = [];
        $this->qb()->orderBy('id')->chunk(2, function (array $rows) use (&$batches) {
            $batches[] = count($rows);
        });

        // 5 rows: [2, 2, 1]
        self::assertSame([2, 2, 1], $batches);
    }

    public function testChunkEarlyStop(): void
    {
        $batches = [];
        $this->qb()->orderBy('id')->chunk(2, function (array $rows) use (&$batches) {
            $batches[] = count($rows);
            return false; // stop after first batch
        });

        self::assertSame([2], $batches);
    }

    public function testLazy(): void
    {
        $names = [];
        foreach ($this->qb()->orderBy('id')->lazy(2) as $row) {
            $names[] = $row['name'];
        }

        self::assertSame(['Alice', 'Bob', 'Charlie', 'Diana', 'Eve'], $names);
    }

    public function testChunkById(): void
    {
        $allNames = [];
        $this->qb()->chunkById(2, function (array $rows) use (&$allNames) {
            foreach ($rows as $row) {
                $allNames[] = $row['name'];
            }
        });

        self::assertCount(5, $allNames);
        self::assertSame('Alice', $allNames[0]);
        self::assertSame('Eve', $allNames[4]);
    }

    public function testChunkByIdEarlyStop(): void
    {
        $seen = 0;
        $this->qb()->chunkById(2, function (array $rows) use (&$seen) {
            $seen += count($rows);
            return false;
        });

        self::assertSame(2, $seen);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: lockForUpdate / sharedLock ─────────────────
    // ══════════════════════════════════════════════════════════════

    public function testLockForUpdateCompilesWithMySql(): void
    {
        // Build SQL using MySQL grammar (the default)
        $conn = new TestConnection($this->pdo, DatabaseDriver::MySQL);
        $manager = new TestConnectionManager($conn);

        $sql = (new QueryBuilder($manager))
            ->from('users')
            ->where('id', '=', 1)
            ->lockForUpdate()
            ->toSql();

        self::assertStringContainsString('FOR UPDATE', $sql);
    }

    public function testSharedLockCompilesWithMySql(): void
    {
        $conn = new TestConnection($this->pdo, DatabaseDriver::MySQL);
        $manager = new TestConnectionManager($conn);

        $sql = (new QueryBuilder($manager))
            ->from('users')
            ->sharedLock()
            ->toSql();

        self::assertStringContainsString('LOCK IN SHARE MODE', $sql);
    }

    public function testLockNoWait(): void
    {
        $conn = new TestConnection($this->pdo, DatabaseDriver::MySQL);
        $manager = new TestConnectionManager($conn);

        $sql = (new QueryBuilder($manager))
            ->from('users')
            ->lockForUpdate(noWait: true)
            ->toSql();

        self::assertStringContainsString('NOWAIT', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: withCte ────────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testWithCteCompiles(): void
    {
        $compiled = $this->qb()
            ->withCte('active_users', fn(QueryBuilder $inner) => $inner
                ->from('users')
                ->where('status', '=', 'active')
            )
            ->from('active_users')
            ->compile();

        $sql = $compiled['sql'];
        self::assertStringContainsString('WITH', $sql);
        self::assertStringContainsString('active_users AS', $sql);
        // CTE bindings should be prepended
        self::assertSame('active', $compiled['bindings'][0]);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: insertOrIgnore ─────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testInsertOrIgnoreExecution(): void
    {
        // First insert — should succeed
        $affected = $this->qb()->insertOrIgnore([
            'name' => 'Frank',
            'email' => 'frank@test.com',
            'status' => 'active',
            'age' => 40,
        ]);

        self::assertSame(1, $affected);
        self::assertSame(6, $this->qb()->count());
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: truncate ───────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testTruncateExecution(): void
    {
        self::assertSame(5, $this->qb()->count());
        $this->qb()->truncate();
        self::assertSame(0, $this->qb()->count());
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: insertMany validation ──────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testInsertManyRejectsAssociativeArray(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('insertMany()');
        $this->qb()->insertMany(['first' => ['name' => 'A'], 'second' => ['name' => 'B']]);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: toDebugSql enhancements ────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testToDebugSqlHandlesNullBinding(): void
    {
        $sql = $this->qb()
            ->whereNull('deleted_at')
            ->toDebugSql();

        // IS NULL doesn't produce a binding, but the query still runs
        self::assertStringContainsString('IS NULL', $sql);
    }

    public function testToDebugSqlHandlesBooleanBinding(): void
    {
        $sql = $this->qb()
            ->whereRaw('active = ?', [true])
            ->toDebugSql();

        self::assertStringContainsString('active = 1', $sql);
    }

    public function testToDebugSqlHandlesStringEscaping(): void
    {
        $sql = $this->qb()
            ->where('name', '=', "O'Brien")
            ->toDebugSql();

        self::assertStringContainsString("O\\'Brien", $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: reset clears new state ─────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testResetClearsLockAndCte(): void
    {
        $conn = new TestConnection($this->pdo, DatabaseDriver::MySQL);
        $manager = new TestConnectionManager($conn);

        $qb = (new QueryBuilder($manager))
            ->from('users')
            ->lockForUpdate()
            ->withCte('cte1', fn($inner) => $inner->from('users'))
            ->reset()
            ->from('users');

        $sql = $qb->toSql();
        self::assertStringNotContainsString('FOR UPDATE', $sql);
        self::assertStringNotContainsString('WITH', $sql);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: fromTable & grammar public access ──────────
    // ══════════════════════════════════════════════════════════════

    public function testFromTablePublicAccess(): void
    {
        $qb = $this->qb();
        self::assertSame('users', $qb->fromTable);
    }

    public function testGrammarPublicAccess(): void
    {
        $qb = $this->qb();
        // SQLite connection → SqliteGrammar
        self::assertInstanceOf(SqliteGrammar::class, $qb->grammar);
    }

    // ══════════════════════════════════════════════════════════════
    // ── QueryBuilder: bindingCount includes all clause types ─────
    // ══════════════════════════════════════════════════════════════

    public function testBindingCountIncludesAllSources(): void
    {
        $qb = $this->qb()
            ->select([new RawExpression('COALESCE(?, name)', ['default'])])
            ->where('status', '=', 'active')
            ->having('COUNT(*) > ?', [1]);

        // 1 (select raw) + 1 (where) + 1 (having) = 3
        self::assertSame(3, $qb->bindingCount);
    }

    // ══════════════════════════════════════════════════════════════
    // ── VectorSearch: validation ──────────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testVectorSearchRejectsEmptyVector(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('must not be empty');
        VectorSearch::distance('embedding', [], DatabaseDriver::PostgreSQL);
    }

    public function testVectorSearchRejectsNonNumericVector(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('not a numeric scalar');
        VectorSearch::distance('embedding', ['not_a_number'], DatabaseDriver::PostgreSQL);
    }

    public function testVectorSearchRejectsInvalidColumnName(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Invalid column identifier');
        VectorSearch::distance('DROP TABLE users; --', [1.0], DatabaseDriver::PostgreSQL);
    }

    public function testMySqlVectorSearchUsesParameterizedBindings(): void
    {
        $expr = VectorSearch::distance('embedding', [1.0, 2.0], DatabaseDriver::MySQL);
        self::assertStringContainsString('?', $expr->toSql());
        self::assertNotEmpty($expr->getBindings());
        self::assertSame('[1,2]', $expr->getBindings()[0]);
    }

    public function testPostgresVectorSearchUsesParameterizedBindings(): void
    {
        $expr = VectorSearch::distance('embedding', [4.0, 5.0], DatabaseDriver::PostgreSQL, 'cosine');
        self::assertStringContainsString('?::vector', $expr->toSql());
        self::assertSame(['[4,5]'], $expr->getBindings());
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: refresh() ──────────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testRefreshReloadsEntity(): void
    {
        $repo = new PulledChangesUserRepo($this->manager);

        $user = $repo->find(1);
        self::assertNotNull($user);
        self::assertSame('Alice', $user->name);

        // Externally modify the DB row
        $this->pdo->exec("UPDATE users SET name = 'Alicia' WHERE id = 1");

        // Refresh should pick up the change
        $refreshed = $repo->refresh($user);
        self::assertSame($user, $refreshed, 'Same instance should be returned');
        self::assertSame('Alicia', $refreshed->name);
    }

    public function testRefreshThrowsOnEntityWithoutId(): void
    {
        $repo = new PulledChangesUserRepo($this->manager);
        $entity = new PulledChangesUser();

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('without an ID');
        $repo->refresh($entity);
    }

    public function testRefreshThrowsOnDeletedEntity(): void
    {
        $repo = new PulledChangesUserRepo($this->manager);
        $user = $repo->find(1);

        $this->pdo->exec('DELETE FROM users WHERE id = 1');

        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('no longer exists');
        $repo->refresh($user);
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: global scopes ──────────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testGlobalScopeAppliedToQueries(): void
    {
        $repo = new SoftDeleteUserRepo($this->manager);

        // Eve is 'banned' — soft delete scope should only return status != 'banned'
        $all = $repo->findAll();

        // 4 active/inactive users, 1 banned — scope filters out banned
        self::assertCount(4, $all);
        foreach ($all as $user) {
            self::assertNotSame('banned', $user->status);
        }
    }

    public function testGlobalScopeAppliedToFind(): void
    {
        $repo = new SoftDeleteUserRepo($this->manager);

        // Eve (id=5) is banned — global scope should make find(5) return null
        $user = $repo->find(5);
        self::assertNull($user);
    }

    public function testGlobalScopeAppliedToCount(): void
    {
        $repo = new SoftDeleteUserRepo($this->manager);
        self::assertSame(4, $repo->count());
    }

    // ══════════════════════════════════════════════════════════════
    // ── EntityRepository: paginate with clone ────────────────────
    // ══════════════════════════════════════════════════════════════

    public function testPaginateConsistentCountAndData(): void
    {
        $repo = new PulledChangesUserRepo($this->manager);
        $page = $repo->paginate(1, 2);

        self::assertSame(5, $page['total']);
        self::assertCount(2, $page['data']);
        self::assertSame(1, $page['page']);
        self::assertSame(3, $page['lastPage']);  // ceil(5/2) = 3
    }

    public function testPaginateLastPageDivByZeroProtection(): void
    {
        $repo = new PulledChangesUserRepo($this->manager);
        // perPage=0 should not cause division by zero — max(1, 0) = 1
        $page = $repo->paginate(1, 0);
        self::assertSame(5, $page['lastPage']); // ceil(5/1) = 5
    }
}

// ══════════════════════════════════════════════════════════════════
// ── Test Entities & Repositories ─────────────────────────────────
// ══════════════════════════════════════════════════════════════════

class PulledChangesUser
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

class PulledChangesUserRepo extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = PulledChangesUser::class;
}

class SoftDeleteUserRepo extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = PulledChangesUser::class;

    #[Scope(isGlobal: true)]
    public function notBanned(QueryBuilder $qb): QueryBuilder
    {
        return $qb->where('status', '!=', 'banned');
    }
}
