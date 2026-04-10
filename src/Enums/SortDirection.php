<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Enums;

/**
 * MonkeysLegion Framework — Query Package
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
enum SortDirection: string
{
    case Asc  = 'ASC';
    case Desc = 'DESC';

    /**
     * Create from a loose string (case-insensitive).
     *
     * @throws \InvalidArgumentException When the direction string is not 'ASC' or 'DESC'.
     */
    public static function fromLoose(string $value): self
    {
        return match (strtoupper(trim($value))) {
            'ASC'   => self::Asc,
            'DESC'  => self::Desc,
            default => throw new \InvalidArgumentException(
                "Invalid sort direction '{$value}'. Expected 'ASC' or 'DESC'.",
            ),
        };
    }
}
