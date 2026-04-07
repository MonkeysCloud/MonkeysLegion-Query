<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Enums;

/**
 * MonkeysLegion Framework — Query Package
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
enum JoinType: string
{
    case Inner = 'INNER';
    case Left  = 'LEFT';
    case Right = 'RIGHT';
    case Cross = 'CROSS';

    /**
     * Human-readable label.
     */
    public function label(): string
    {
        return match ($this) {
            self::Inner => 'Inner Join',
            self::Left  => 'Left Join',
            self::Right => 'Right Join',
            self::Cross => 'Cross Join',
        };
    }
}
