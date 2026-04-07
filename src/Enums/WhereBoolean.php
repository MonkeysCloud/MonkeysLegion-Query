<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Enums;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Boolean connector for WHERE clause conditions.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
enum WhereBoolean: string
{
    case And = 'AND';
    case Or  = 'OR';
}
