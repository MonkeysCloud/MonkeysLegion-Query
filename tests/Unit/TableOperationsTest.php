<?php

use PHPUnit\Framework\TestCase;
use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Query\QueryBuilder;

class TableOperationsTest extends TestCase
{
    private QueryBuilder $qb;

    protected function setUp(): void
    {
        $pdo = new PDO('sqlite::memory:');
        $pdo->setAttribute(PDO::ATTR_ERRMODE, PDO::ERRMODE_EXCEPTION);
        $pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT)');
        $conn = new class($pdo) implements ConnectionInterface {
            public function __construct(private PDO $pdo) {}
            public function pdo(): PDO
            {
                return $this->pdo;
            }
            public function connect(): void {}
            public function disconnect(): void {}
            public function isConnected(): bool
            {
                return true;
            }
            public function getDsn(): string
            {
                return '';
            }
            public function isAlive(): bool
            {
                return true;
            }
        };
        $this->qb = new QueryBuilder($conn);
    }

    public function testSetAndGetTableMap()
    {
        $this->qb->setTableMap(['users' => 'user']);
        $this->assertEquals(['users' => 'user'], $this->qb->getTableMap());
    }

    public function testAddAndRemoveTableMapping()
    {
        $this->qb->addTableMapping('foo', 'bar');
        $this->assertTrue($this->qb->hasTableMapping('foo'));
        $this->qb->removeTableMapping('foo');
        $this->assertFalse($this->qb->hasTableMapping('foo'));
    }

    public function testClearTableMap()
    {
        $this->qb->setTableMap(['a' => 'b']);
        $this->qb->clearTableMap();
        $this->assertEmpty($this->qb->getTableMap());
    }

    public function testResolveTable()
    {
        $this->qb->setTableMap(['users' => 'user']);
        $this->assertEquals('user', $this->qb->resolveTable('users'));
        $this->assertEquals('other', $this->qb->resolveTable('other'));
    }

    public function testTablePrefix()
    {
        $this->qb->setTablePrefix('pre_');
        $this->assertEquals('pre_users', $this->qb->withPrefix('users'));
        $this->assertEquals('users', $this->qb->withoutPrefix('pre_users'));
    }

    public function testGetAndSetTable()
    {
        $this->qb->setTable('users', 'u');
        $this->assertEquals('users AS u', $this->qb->getTable());
        $this->assertEquals('users', $this->qb->getTableName());
        $this->assertEquals('u', $this->qb->getTableAlias());
    }

    public function testPartsManipulation()
    {
        $this->qb->setPart('foo', 'bar');
        $this->assertEquals('bar', $this->qb->getPart('foo'));
        $this->assertTrue($this->qb->hasPart('foo'));
        $this->qb->removePart('foo');
        $this->assertFalse($this->qb->hasPart('foo'));
    }

    public function testReset()
    {
        $this->qb->setPart('foo', 'bar');
        $this->qb->reset();
        $this->assertNull($this->qb->getPart('foo'));
        $this->assertEquals('*', $this->qb->getPart('select'));
    }

    public function testCounter()
    {
        $this->qb->setCounter(42);
        $this->assertEquals(42, $this->qb->getCounter());
        $this->qb->resetCounter();
        $this->assertEquals(0, $this->qb->getCounter());
    }

    public function testDuplicateAndClone()
    {
        $this->qb->setPart('foo', 'bar');
        $clone = $this->qb->duplicate();
        $this->assertNotSame($this->qb, $clone);
        $this->assertEquals($this->qb->getPart('foo'), $clone->getPart('foo'));
        $this->assertEquals($this->qb->getParts(), $clone->getParts());
        $this->assertEquals($this->qb->getParams(), $clone->getParams());
        $this->assertEquals($this->qb->getCounter(), $clone->getCounter());

        $cloned = $this->qb->clone();
        $this->assertEquals($clone->getParts(), $cloned->getParts());
    }

    public function testFreshReturnsNewInstance()
    {
        $fresh = $this->qb->fresh();
        $this->assertInstanceOf(QueryBuilder::class, $fresh);
        $this->assertNotSame($this->qb, $fresh);
    }

    public function testGetConnectionReturnsConnection()
    {
        $this->assertInstanceOf(ConnectionInterface::class, $this->qb->getConnection());
    }

    public function testResetBindings()
    {
        $this->qb->setPart('foo', 'bar');
        $this->qb->resetBindings();
        $this->assertEquals([], $this->qb->getParams());
        $this->assertEquals(0, $this->qb->getCounter());
    }

    public function testGetTableColumnsAndSchema()
    {
        $columns = $this->qb->getTableColumns('users');
        $this->assertContains('id', $columns);
        $schema = $this->qb->getTableSchema('users');
        $this->assertNotEmpty($schema);
        $this->assertArrayHasKey('name', $schema[0]);
    }

    public function testToArrayAndFromArray()
    {
        $arr = $this->qb->toArray();
        $this->assertArrayHasKey('parts', $arr);
        $this->qb->setPart('foo', 'bar');
        $arr2 = $this->qb->toArray();
        $qb2 = $this->qb->fresh()->fromArray($arr2);
        $this->assertEquals('bar', $qb2->getPart('foo'));
    }

    public function testToDebugString()
    {
        $str = $this->qb->toDebugString();
        $this->assertStringContainsString('QueryBuilder {', $str);
    }

    public function testLogDoesNotThrow()
    {
        $this->assertInstanceOf(QueryBuilder::class, $this->qb->log());
    }

    public function testWhenUnlessTap()
    {
        $this->qb->when(true, function ($qb) {
            $qb->setPart('when', 1);
        });
        $this->assertEquals(1, $this->qb->getPart('when'));

        $this->qb->unless(false, function ($qb) {
            $qb->setPart('unless', 2);
        });
        $this->assertEquals(2, $this->qb->getPart('unless'));

        $this->qb->tap(function ($qb) {
            $qb->setPart('tap', 3);
        });
        $this->assertEquals(3, $this->qb->getPart('tap'));
    }

    public function testMacroAndCall()
    {
        QueryBuilder::macro('fooMacro', function ($val) {
            $this->setPart('macro', $val);
            return $this;
        });
        $this->assertTrue(QueryBuilder::hasMacro('fooMacro'));
        $this->qb->fooMacro('bar');
        $this->assertEquals('bar', $this->qb->getPart('macro'));
    }

    public function testRemoveDistinctAndModifiers()
    {
        $this->qb->setPart('distinct', true);
        $this->qb->removeDistinct();
        $this->assertFalse($this->qb->getPart('distinct'));
        $this->qb->calcFoundRows();
        $this->qb->highPriority();
        $this->qb->smallResult();
        $this->qb->bigResult();
        $this->qb->bufferResult();
        $this->assertStringContainsString('SQL_CALC_FOUND_ROWS', $this->qb->getPart('modifiers'));
        $this->assertStringContainsString('HIGH_PRIORITY', $this->qb->getPart('modifiers'));
        $this->assertStringContainsString('SQL_SMALL_RESULT', $this->qb->getPart('modifiers'));
        $this->assertStringContainsString('SQL_BIG_RESULT', $this->qb->getPart('modifiers'));
        $this->assertStringContainsString('SQL_BUFFER_RESULT', $this->qb->getPart('modifiers'));
    }

    public function testDumpAndDd()
    {
        // Just ensure dump() returns $this and dd() exits (cannot test exit, so just call dump)
        $this->assertInstanceOf(QueryBuilder::class, $this->qb->dump());
    }
}
