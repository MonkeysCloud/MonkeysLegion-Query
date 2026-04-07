<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Enums;

/**
 * MonkeysLegion Framework — Query Package
 *
 * SQL comparison operators with validation.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
enum Operator: string
{
    case Equal              = '=';
    case NotEqual           = '!=';
    case LessThan           = '<';
    case LessThanOrEqual    = '<=';
    case GreaterThan        = '>';
    case GreaterThanOrEqual = '>=';
    case Like               = 'LIKE';
    case NotLike            = 'NOT LIKE';
    case In                 = 'IN';
    case NotIn              = 'NOT IN';
    case Between            = 'BETWEEN';
    case NotBetween         = 'NOT BETWEEN';
    case IsNull             = 'IS NULL';
    case IsNotNull          = 'IS NOT NULL';
    case Exists             = 'EXISTS';
    case NotExists          = 'NOT EXISTS';
    /** Raw SQL fragment — value already contains the full expression. */
    case Raw                = 'RAW';
    /** Grouped sub-conditions wrapped in parentheses. */
    case Group              = 'GROUP';

    /**
     * Whether this operator expects a value (vs. IS NULL which does not).
     */
    public function requiresValue(): bool
    {
        return match ($this) {
            self::IsNull, self::IsNotNull, self::Raw, self::Group => false,
            default => true,
        };
    }

    /**
     * Whether this operator works with an array of values.
     */
    public function isArrayOperator(): bool
    {
        return match ($this) {
            self::In, self::NotIn, self::Between, self::NotBetween => true,
            default                                                 => false,
        };
    }

    /**
     * Create from a loose string (case-insensitive, trims whitespace).
     *
     * @throws \InvalidArgumentException When the operator string is not recognised.
     */
    public static function fromLoose(string $value): self
    {
        $normalized = strtoupper(trim($value));

        // Handle common aliases
        $known = match ($normalized) {
            '='              => self::Equal,
            '!=', '<>'       => self::NotEqual,
            '<'              => self::LessThan,
            '<='             => self::LessThanOrEqual,
            '>'              => self::GreaterThan,
            '>='             => self::GreaterThanOrEqual,
            'LIKE'           => self::Like,
            'NOT LIKE'       => self::NotLike,
            'IN'             => self::In,
            'NOT IN'         => self::NotIn,
            'BETWEEN'        => self::Between,
            'NOT BETWEEN'    => self::NotBetween,
            'IS NULL'        => self::IsNull,
            'IS NOT NULL'    => self::IsNotNull,
            'EXISTS'         => self::Exists,
            'NOT EXISTS'     => self::NotExists,
            default          => null,
        };

        if ($known !== null) {
            return $known;
        }

        try {
            return self::from($normalized);
        } catch (\ValueError) {
            $valid = implode(', ', array_column(self::cases(), 'value'));
            throw new \InvalidArgumentException(
                "Unknown SQL operator '{$value}'. Valid operators: {$valid}",
            );
        }
    }
}
