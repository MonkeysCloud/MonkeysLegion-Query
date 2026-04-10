<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Query;

use MonkeysLegion\Database\Types\DatabaseDriver;
use MonkeysLegion\Query\Contracts\ExpressionInterface;
use MonkeysLegion\Query\RawExpression;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Vector similarity search builder.
 *
 * Pushes vector distance computation to the database engine:
 *   - PostgreSQL: pgvector `<->` (L2), `<=>` (cosine), `<#>` (inner product) — bound as a typed parameter
 *   - MySQL 9.x: VEC_DISTANCE_L2(), VEC_DISTANCE_COSINE() — bound as JSON-encoded parameter
 *   - SQLite: Throws UnsupportedDriverException (no suitable extension in stdlib)
 *
 * All user-supplied vector values are passed as bound parameters; no interpolation occurs.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class VectorSearch
{
    /**
     * Build a nearest-neighbor ORDER BY expression.
     *
     * @param string            $column   Vector column name (must be a valid SQL identifier).
     * @param list<float>       $vector   Query vector.
     * @param DatabaseDriver    $driver   Target database driver.
     * @param string            $metric   Distance metric: 'l2', 'cosine', 'inner_product'.
     *
     * @return ExpressionInterface Order-by expression for nearest neighbors.
     *
     * @throws \InvalidArgumentException On invalid column name or non-float vector values.
     * @throws \RuntimeException         On unsupported driver (SQLite).
     */
    public static function distance(
        string $column,
        array $vector,
        DatabaseDriver $driver,
        string $metric = 'l2',
    ): ExpressionInterface {
        self::validateIdentifier($column);
        self::validateVector($vector);

        return match ($driver) {
            DatabaseDriver::PostgreSQL => self::pgvectorDistance($column, $vector, $metric),
            DatabaseDriver::MySQL      => self::mysqlDistance($column, $vector, $metric),
            DatabaseDriver::SQLite     => throw new \RuntimeException(
                'Vector search is not supported on SQLite. Use PostgreSQL with pgvector or MySQL 9.x.',
            ),
        };
    }

    /**
     * PostgreSQL pgvector distance expression.
     * Uses a bound parameter cast to ::vector to avoid SQL injection.
     *
     * @param list<float> $vector
     */
    private static function pgvectorDistance(string $column, array $vector, string $metric): ExpressionInterface
    {
        $operator = match ($metric) {
            'cosine'        => '<=>',
            'inner_product' => '<#>',
            default         => '<->',  // l2
        };

        // Bind the vector as '[x,y,z]'::vector — parameterized, not interpolated.
        $vectorStr = '[' . implode(',', $vector) . ']';

        return new RawExpression("{$column} {$operator} ?::vector", [$vectorStr]);
    }

    /**
     * MySQL 9.x vector distance function.
     * Binds the vector as a JSON-encoded string parameter.
     *
     * @param list<float> $vector
     */
    private static function mysqlDistance(string $column, array $vector, string $metric): ExpressionInterface
    {
        $func = match ($metric) {
            'cosine' => 'VEC_DISTANCE_COSINE',
            default  => 'VEC_DISTANCE_L2',
        };

        $vectorJson = json_encode($vector, JSON_THROW_ON_ERROR);

        return new RawExpression("{$func}({$column}, ?)", [$vectorJson]);
    }

    /**
     * Validate that a column name is a safe SQL identifier.
     *
     * @throws \InvalidArgumentException
     */
    private static function validateIdentifier(string $column): void
    {
        if (!preg_match('/^[a-zA-Z_][a-zA-Z0-9_.]*$/', $column)) {
            throw new \InvalidArgumentException(
                "Invalid column identifier for VectorSearch: '{$column}'.",
            );
        }
    }

    /**
     * Validate that every element in the vector is a scalar numeric value.
     *
     * @throws \InvalidArgumentException
     */
    private static function validateVector(array $vector): void
    {
        if ($vector === []) {
            throw new \InvalidArgumentException('Vector must not be empty.');
        }

        foreach ($vector as $i => $value) {
            if (!is_int($value) && !is_float($value)) {
                throw new \InvalidArgumentException(
                    "Vector element at index {$i} is not a numeric scalar.",
                );
            }
        }
    }
}
