<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Compiler;

/**
 * MonkeysLegion Framework — Query Package
 *
 * SQLite grammar.
 * Uses double-quote quoting (ANSI standard), ON CONFLICT ... DO UPDATE
 * for upserts (SQLite 3.24+), and json_extract() for JSON access.
 * Supports RETURNING (SQLite 3.35+).
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class SqliteGrammar implements GrammarInterface
{
    #[\Override]
    public function quoteIdentifier(string $identifier): string
    {
        if (str_contains($identifier, '.')) {
            return implode('.', array_map(
                fn(string $part) => '"' . str_replace('"', '""', $part) . '"',
                explode('.', $identifier),
            ));
        }

        return '"' . str_replace('"', '""', $identifier) . '"';
    }

    #[\Override]
    public function compileLimit(?int $limit, ?int $offset): string
    {
        $sql = '';

        if ($limit !== null) {
            $sql .= "LIMIT {$limit}";
        }

        if ($offset !== null) {
            // SQLite requires LIMIT before OFFSET; if only OFFSET, use LIMIT -1
            if ($limit === null) {
                $sql .= 'LIMIT -1 ';
            }
            $sql .= ($sql !== '' ? ' ' : '') . "OFFSET {$offset}";
        }

        return $sql;
    }

    #[\Override]
    public function compileUpsert(
        string $table,
        array $columns,
        array $placeholders,
        array $updateColumns,
        string|array|null $conflictTarget = null,
    ): string {
        $quotedTable = $this->quoteIdentifier($table);
        $quotedCols  = implode(', ', array_map(fn(string $c) => $this->quoteIdentifier($c), $columns));
        $values      = implode(', ', $placeholders);

        // Conflict target
        $conflict = '';
        if ($conflictTarget !== null) {
            $targets = is_array($conflictTarget)
                ? implode(', ', array_map(fn(string $c) => $this->quoteIdentifier($c), $conflictTarget))
                : $this->quoteIdentifier($conflictTarget);
            $conflict = "({$targets})";
        }

        $updates = implode(', ', array_map(
            fn(string $c) => $this->quoteIdentifier($c) . ' = excluded.' . $this->quoteIdentifier($c),
            $updateColumns,
        ));

        return "INSERT INTO {$quotedTable} ({$quotedCols}) VALUES ({$values}) ON CONFLICT {$conflict} DO UPDATE SET {$updates}";
    }

    #[\Override]
    public function compileJsonPath(string $column, string $path): string
    {
        // SQLite: json_extract(column, '$.path')
        $jsonPath = str_starts_with($path, '$') ? $path : '$.' . $path;
        return "json_extract({$column}, '{$jsonPath}')";
    }

    #[\Override]
    public function supportsReturning(): bool
    {
        return true;
    }

    #[\Override]
    public function compileReturning(array $columns): string
    {
        if ($columns === [] || $columns === ['*']) {
            return 'RETURNING *';
        }

        $quoted = array_map(fn(string $c) => $this->quoteIdentifier($c), $columns);
        return 'RETURNING ' . implode(', ', $quoted);
    }

    #[\Override]
    public function compileInsertOrIgnore(string $table, array $columns, array $placeholders): string
    {
        $quotedTable = $this->quoteIdentifier($table);
        $quotedCols  = implode(', ', array_map(fn(string $c) => $this->quoteIdentifier($c), $columns));
        $values      = implode(', ', $placeholders);

        return "INSERT OR IGNORE INTO {$quotedTable} ({$quotedCols}) VALUES ({$values})";
    }

    #[\Override]
    public function compileTruncate(string $table): string
    {
        // SQLite does not have TRUNCATE; DELETE FROM is equivalent
        return 'DELETE FROM ' . $this->quoteIdentifier($table);
    }

    #[\Override]
    public function compileLock(string $mode, bool $noWait = false): string
    {
        // SQLite does not support row-level locking modifiers
        return '';
    }
}
