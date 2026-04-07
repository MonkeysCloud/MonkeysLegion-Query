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
     */
    public static function fromLoose(string $value): self
    {
        return match (strtoupper(trim($value))) {
            'ASC'  => self::Asc,
            'DESC' => self::Desc,
            default => self::Asc,
        };
    }
}
