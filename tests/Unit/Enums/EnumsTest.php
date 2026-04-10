<?php
declare(strict_types=1);

namespace Tests\Unit\Enums;

use MonkeysLegion\Query\Enums\JoinType;
use MonkeysLegion\Query\Enums\Operator;
use MonkeysLegion\Query\Enums\SortDirection;
use MonkeysLegion\Query\Enums\WhereBoolean;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

#[CoversClass(JoinType::class)]
#[CoversClass(Operator::class)]
#[CoversClass(SortDirection::class)]
#[CoversClass(WhereBoolean::class)]
final class EnumsTest extends TestCase
{
    // ── JoinType ────────────────────────────────────────────────

    public function testJoinTypeValues(): void
    {
        self::assertSame('INNER', JoinType::Inner->value);
        self::assertSame('LEFT', JoinType::Left->value);
        self::assertSame('RIGHT', JoinType::Right->value);
        self::assertSame('CROSS', JoinType::Cross->value);
    }

    public function testJoinTypeLabels(): void
    {
        self::assertSame('Inner Join', JoinType::Inner->label());
        self::assertSame('Left Join', JoinType::Left->label());
    }

    // ── SortDirection ───────────────────────────────────────────

    public function testSortDirectionValues(): void
    {
        self::assertSame('ASC', SortDirection::Asc->value);
        self::assertSame('DESC', SortDirection::Desc->value);
    }

    #[DataProvider('looseSortProvider')]
    public function testSortDirectionFromLoose(string $input, SortDirection $expected): void
    {
        self::assertSame($expected, SortDirection::fromLoose($input));
    }

    public static function looseSortProvider(): array
    {
        return [
            ['asc', SortDirection::Asc],
            ['DESC', SortDirection::Desc],
            [' Asc ', SortDirection::Asc],
        ];
    }

    public function testSortDirectionFromLooseThrowsOnInvalid(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        SortDirection::fromLoose('invalid');
    }

    // ── WhereBoolean ────────────────────────────────────────────

    public function testWhereBooleanValues(): void
    {
        self::assertSame('AND', WhereBoolean::And->value);
        self::assertSame('OR', WhereBoolean::Or->value);
    }

    // ── Operator ────────────────────────────────────────────────

    public function testOperatorValues(): void
    {
        self::assertSame('=', Operator::Equal->value);
        self::assertSame('IN', Operator::In->value);
        self::assertSame('IS NULL', Operator::IsNull->value);
    }

    public function testRequiresValue(): void
    {
        self::assertTrue(Operator::Equal->requiresValue());
        self::assertTrue(Operator::In->requiresValue());
        self::assertFalse(Operator::IsNull->requiresValue());
        self::assertFalse(Operator::IsNotNull->requiresValue());
    }

    public function testIsArrayOperator(): void
    {
        self::assertTrue(Operator::In->isArrayOperator());
        self::assertTrue(Operator::NotIn->isArrayOperator());
        self::assertTrue(Operator::Between->isArrayOperator());
        self::assertFalse(Operator::Equal->isArrayOperator());
        self::assertFalse(Operator::Like->isArrayOperator());
    }

    #[DataProvider('looseOperatorProvider')]
    public function testOperatorFromLoose(string $input, Operator $expected): void
    {
        self::assertSame($expected, Operator::fromLoose($input));
    }

    public static function looseOperatorProvider(): array
    {
        return [
            ['=', Operator::Equal],
            ['!=', Operator::NotEqual],
            ['<>', Operator::NotEqual],
            ['like', Operator::Like],
            ['NOT LIKE', Operator::NotLike],
            [' in ', Operator::In],
            ['IS NULL', Operator::IsNull],
            ['between', Operator::Between],
        ];
    }
}
