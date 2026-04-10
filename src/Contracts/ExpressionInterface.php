<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Contracts;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Represents any SQL fragment that can be compiled to a string
 * with associated parameter bindings.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
interface ExpressionInterface
{
    /**
     * Compile the expression into a raw SQL fragment.
     */
    public function toSql(): string;

    /**
     * Get the parameter bindings for this expression.
     *
     * @return list<mixed>
     */
    public function getBindings(): array;
}
