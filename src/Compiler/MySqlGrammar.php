<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Compiler;

/**
 * MonkeysLegion Framework — Query Package
 *
 * MySQL / MariaDB grammar.
 * Uses backtick quoting, ON DUPLICATE KEY UPDATE for upserts,
 * and ->> for JSON path access.
 *
 * MariaDB 10.5+ supports RETURNING but MySQL does not;
 * this grammar conservatively returns false for supportsReturning().
 * Use PostgresGrammar for RETURNING-dependent workflows.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class MySqlGrammar implements GrammarInterface
{
    #[\Override]
    public function quoteIdentifier(string $identifier): string
    {
        // Handle dotted identifiers (schema.table or table.column)
        if (str_contains($identifier, '.')) {
            return implode('.', array_map(
                fn(string $part) => '`' . str_replace('`', '``', $part) . '`',
                explode('.', $identifier),
            ));
        }

        return '`' . str_replace('`', '``', $identifier) . '`';
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

        $updates = implode(', ', array_map(
            fn(string $c) => $this->quoteIdentifier($c) . ' = VALUES(' . $this->quoteIdentifier($c) . ')',
            $updateColumns,
        ));

        return "INSERT INTO {$quotedTable} ({$quotedCols}) VALUES ({$values}) ON DUPLICATE KEY UPDATE {$updates}";
    }

    #[\Override]
    public function compileJsonPath(string $column, string $path): string
    {
        // MySQL 5.7+ / MariaDB 10.2+: column->>'$.path'
        $jsonPath = str_starts_with($path, '$') ? $path : '$.'. $path;
        return "{$column}->>'{$jsonPath}'";
    }

    #[\Override]
    public function supportsReturning(): bool
    {
        return false;
    }

    #[\Override]
    public function compileReturning(array $columns): string
    {
        return '';
    }

    #[\Override]
    public function compileInsertOrIgnore(string $table, array $columns, array $placeholders): string
    {
        $quotedTable = $this->quoteIdentifier($table);
        $quotedCols  = implode(', ', array_map(fn(string $c) => $this->quoteIdentifier($c), $columns));
        $values      = implode(', ', $placeholders);

        return "INSERT IGNORE INTO {$quotedTable} ({$quotedCols}) VALUES ({$values})";
    }

    #[\Override]
    public function compileTruncate(string $table): string
    {
        return 'TRUNCATE TABLE ' . $this->quoteIdentifier($table);
    }

    #[\Override]
    public function compileLock(string $mode, bool $noWait = false): string
    {
        $sql = $mode === 'share' ? 'LOCK IN SHARE MODE' : 'FOR UPDATE';
        if ($noWait) {
            $sql .= ' NOWAIT';
        }
        return $sql;
    }
}
