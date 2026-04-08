<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Compiler;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Driver-specific SQL dialect contract.
 * Each supported database (MySQL/MariaDB, PostgreSQL, SQLite) provides
 * an implementation to handle quoting, upserts, and dialect quirks.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
interface GrammarInterface
{
    /**
     * Quote an identifier (table or column name) for this driver.
     */
    public function quoteIdentifier(string $identifier): string;

    /**
     * Compile a LIMIT/OFFSET fragment.
     */
    public function compileLimit(?int $limit, ?int $offset): string;

    /**
     * Compile an UPSERT (INSERT ... ON CONFLICT/DUPLICATE KEY).
     *
     * @param string                         $table         Target table.
     * @param list<string>                   $columns       Column names.
     * @param list<string>                   $placeholders  Value placeholders (? marks).
     * @param list<string>                   $updateColumns Columns to update on conflict.
     * @param string|list<string>|null       $conflictTarget Conflict columns (PG) or null (MySQL).
     */
    public function compileUpsert(
        string $table,
        array $columns,
        array $placeholders,
        array $updateColumns,
        string|array|null $conflictTarget = null,
    ): string;

    /**
     * Compile a JSON path access expression.
     *
     * @param string $column   JSON column name.
     * @param string $path     JSON path (e.g. '$.name' or 'address.city').
     */
    public function compileJsonPath(string $column, string $path): string;

    /**
     * Whether RETURNING is supported for INSERT/UPDATE/DELETE.
     */
    public function supportsReturning(): bool;

    /**
     * Compile a RETURNING clause.
     *
     * @param list<string> $columns Columns to return.
     */
    public function compileReturning(array $columns): string;

    /**
     * Compile an INSERT OR IGNORE / INSERT IGNORE statement.
     *
     * @param string       $table
     * @param list<string> $columns
     * @param list<string> $placeholders
     */
    public function compileInsertOrIgnore(string $table, array $columns, array $placeholders): string;

    /**
     * Compile a TRUNCATE TABLE statement.
     */
    public function compileTruncate(string $table): string;

    /**
     * Compile a pessimistic locking modifier.
     *
     * @param 'update'|'share' $mode     Lock mode.
     * @param bool             $noWait   Whether to add NOWAIT.
     */
    public function compileLock(string $mode, bool $noWait = false): string;

    /**
     * Compile a JSON contains/membership check.
     *
     * @param string $column      JSON column name.
     * @param string $placeholder Bind placeholder (usually '?').
     */
    public function compileJsonContains(string $column, string $placeholder = '?'): string;

    /**
     * Compile a DATE extraction expression.
     *
     * @param string $column Column or expression containing a datetime.
     */
    public function compileDateExtract(string $column): string;
}
