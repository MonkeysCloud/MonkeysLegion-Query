<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Compiler;

/**
 * MonkeysLegion Framework — Query Package
 *
 * PostgreSQL grammar.
 * Uses double-quote quoting, ON CONFLICT ... DO UPDATE for upserts,
 * supports RETURNING, and -> / ->> for JSON path access.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class PostgresGrammar implements GrammarInterface
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
            fn(string $c) => $this->quoteIdentifier($c) . ' = EXCLUDED.' . $this->quoteIdentifier($c),
            $updateColumns,
        ));

        return "INSERT INTO {$quotedTable} ({$quotedCols}) VALUES ({$values}) ON CONFLICT {$conflict} DO UPDATE SET {$updates}";
    }

    #[\Override]
    public function compileJsonPath(string $column, string $path): string
    {
        // PostgreSQL: column->>'key' for top-level, column#>>'{path,to,key}' for nested
        $parts = explode('.', str_replace('$.', '', $path));

        if (count($parts) === 1) {
            return "{$column}->>'{$parts[0]}'";
        }

        $pgPath = "'{" . implode(',', $parts) . "}'";
        return "{$column}#>>{$pgPath}";
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

        return "INSERT INTO {$quotedTable} ({$quotedCols}) VALUES ({$values}) ON CONFLICT DO NOTHING";
    }

    #[\Override]
    public function compileTruncate(string $table): string
    {
        return 'TRUNCATE TABLE ' . $this->quoteIdentifier($table);
    }

    #[\Override]
    public function compileLock(string $mode, bool $noWait = false): string
    {
        $sql = $mode === 'share' ? 'FOR SHARE' : 'FOR UPDATE';
        if ($noWait) {
            $sql .= ' NOWAIT';
        }
        return $sql;
    }
}
