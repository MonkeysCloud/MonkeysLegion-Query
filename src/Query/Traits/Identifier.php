<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

/**
 * Provides database identifier parsing and quoting functionality.
 * 
 * Handles proper parsing and escaping of table and column identifiers
 * including support for schema-qualified names.
 */
trait Identifier
{
    /**
     * Parse a possibly-qualified identifier like:
     *   ml_mail.domains, `ml_mail`.`domains`, `domains`, domains
     * Returns [schema|null, table]
     */
    public function parseQualified(string $ref): array
    {
        $ref = trim($ref);
        if (preg_match('~^\s*(?:`?([A-Za-z0-9_]+)`?\.)?`?([A-Za-z0-9_]+)`?\s*$~', $ref, $m)) {
            $schema = $m[1] ?? null;
            $table  = $m[2];
            return [$schema, $table];
        }
        return [null, $ref];
    }

    /**
     * Quote an identifier (table or column name)
     */
    public function quoteIdent(string $ident): string
    {
        $q = '`' . str_replace('`', '``', $ident) . '`';
        return $q;
    }

    /** Quote qualified name preserving schema when present. */
    public function quoteQualified(?string $schema, string $table): string
    {
        return $schema
            ? $this->quoteIdent($schema) . '.' . $this->quoteIdent($table)
            : $this->quoteIdent($table);
    }

    /** 
     * Parse "schema.table" or "table" (with/without backticks).
     */
    protected function parseQualifiedRef(string $ref): array
    {
        $ref = trim($ref);
        // Matches: `schema`.`table`, schema.`table`, `schema`.table, schema.table, `table`, table
        if (preg_match('/^\s*(?:`?(\w+)`?\.)?`?(\w+)`?\s*$/', $ref, $m)) {
            $schema = $m[1] ?? null;
            $table  = $m[2];
            return [$schema, $table];
        }
        // Fallback: treat entire ref as table
        return [null, $ref];
    }
}
