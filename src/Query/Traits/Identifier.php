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

    /**
     * Gets the appropriate quote character for the current database driver.
     */
    protected function getQuoteChar(): string
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        return match($driver) {
            'mysql' => '`',
            'pgsql', 'sqlite' => '"',
            'sqlsrv', 'dblib' => '[',  // SQL Server uses brackets
            default => '`',
        };
    }

    /**
     * Gets the closing quote character (for SQL Server brackets).
     */
    protected function getCloseQuoteChar(): string
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        return match($driver) {
            'sqlsrv', 'dblib' => ']',
            default => $this->getQuoteChar(),
        };
    }

    /**
     * Quotes an identifier using database-specific quote characters.
     */
    public function quoteIdentifier(string $ident): string
    {
        $open = $this->getQuoteChar();
        $close = $this->getCloseQuoteChar();

        // Escape the quote character if it appears in the identifier
        if ($open === $close) {
            $escaped = str_replace($open, $open . $open, $ident);
        } else {
            // For brackets, escape differently
            $escaped = str_replace($close, $close . $close, $ident);
        }

        return $open . $escaped . $close;
    }

    /**
     * Quotes multiple identifiers.
     */
    public function quoteIdentifiers(array $identifiers): array
    {
        return array_map(fn($id) => $this->quoteIdentifier($id), $identifiers);
    }

    /**
     * Parses a fully qualified identifier: database.schema.table or schema.table or table.
     * Returns [database|null, schema|null, table].
     */
    public function parseFullyQualified(string $ref): array
    {
        $ref = trim($ref);

        // Remove backticks/quotes for parsing
        $ref = preg_replace('/[`"\[\]]/', '', $ref);

        $parts = explode('.', $ref);
        $count = count($parts);

        return match($count) {
            3 => [$parts[0], $parts[1], $parts[2]],  // database.schema.table
            2 => [null, $parts[0], $parts[1]],        // schema.table
            1 => [null, null, $parts[0]],             // table
            default => [null, null, $ref],
        };
    }

    /**
     * Quotes a fully qualified identifier.
     *
     * @param string|null $database Database name
     * @param string|null $schema Schema name
     * @param string $table Table name
     */
    public function quoteFullyQualified(?string $database, ?string $schema, string $table): string
    {
        $parts = [];

        if ($database !== null) {
            $parts[] = $this->quoteIdentifier($database);
        }

        if ($schema !== null) {
            $parts[] = $this->quoteIdentifier($schema);
        }

        $parts[] = $this->quoteIdentifier($table);

        return implode('.', $parts);
    }

    /**
     * Parses a column reference with optional table prefix.
     * Examples: "users.id", "u.name", "email"
     * Returns [table|null, column].
     */
    public function parseColumnRef(string $ref): array
    {
        $ref = trim($ref);

        // Handle quoted identifiers
        if (preg_match('/^(?:`?([A-Za-z0-9_]+)`?\.)?`?([A-Za-z0-9_*]+)`?$/', $ref, $m)) {
            $table = $m[1] ?? null;
            $column = $m[2];
            return [$table, $column];
        }

        return [null, $ref];
    }

    /**
     * Quotes a column reference (table.column).
     */
    public function quoteColumnRef(?string $table, string $column): string
    {
        if ($table !== null) {
            return $this->quoteIdentifier($table) . '.' . $this->quoteIdentifier($column);
        }

        return $this->quoteIdentifier($column);
    }

    /**
     * Parses and quotes a column reference in one call.
     */
    public function parseAndQuoteColumn(string $ref): string
    {
        [$table, $column] = $this->parseColumnRef($ref);
        return $this->quoteColumnRef($table, $column);
    }

    /**
     * Parses an identifier with optional alias.
     * Examples: "users AS u", "email as user_email", "name"
     * Returns [identifier, alias|null].
     */
    public function parseAlias(string $ref): array
    {
        $ref = trim($ref);

        // Match: identifier AS alias or identifier alias
        if (preg_match('/^(.+?)\s+(?:AS\s+)?([A-Za-z0-9_]+)$/i', $ref, $m)) {
            return [trim($m[1]), trim($m[2])];
        }

        return [$ref, null];
    }

    /**
     * Builds an identifier with alias.
     */
    public function withAlias(string $identifier, ?string $alias): string
    {
        if ($alias === null) {
            return $identifier;
        }

        return "$identifier AS " . $this->quoteIdentifier($alias);
    }

    /**
     * Parses, quotes, and adds alias to an identifier.
     */
    public function parseQuoteAndAlias(string $ref): string
    {
        [$identifier, $alias] = $this->parseAlias($ref);

        // Parse the identifier part (might be qualified)
        [$schema, $table] = $this->parseQualified($identifier);
        $quoted = $this->quoteQualified($schema, $table);

        return $this->withAlias($quoted, $alias);
    }

    /**
     * Checks if identifier contains a wildcard.
     */
    public function hasWildcard(string $ref): bool
    {
        return str_contains($ref, '*');
    }

    /**
     * Quotes a wildcard reference (e.g., "users.*").
     */
    public function quoteWildcard(string $ref): string
    {
        if (!$this->hasWildcard($ref)) {
            return $this->quoteIdentifier($ref);
        }

        // Handle table.* format
        if (preg_match('/^([A-Za-z0-9_]+)\.\*$/', $ref, $m)) {
            return $this->quoteIdentifier($m[1]) . '.*';
        }

        // Just * by itself
        if ($ref === '*') {
            return '*';
        }

        return $ref;
    }

    /**
     * Checks if identifier needs quoting (contains special characters).
     */
    public function needsQuoting(string $ident): bool
    {
        // Check for spaces, special chars, or reserved words
        return !preg_match('/^[A-Za-z][A-Za-z0-9_]*$/', $ident)
            || $this->isReservedWord($ident);
    }

    /**
     * Conditionally quotes an identifier only if needed.
     */
    public function quoteIfNeeded(string $ident): string
    {
        if ($this->needsQuoting($ident)) {
            return $this->quoteIdentifier($ident);
        }

        return $ident;
    }

    /**
     * Common SQL reserved words.
     */
    protected array $reservedWords = [
        'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'FROM', 'WHERE', 'JOIN',
        'LEFT', 'RIGHT', 'INNER', 'OUTER', 'ON', 'AS', 'AND', 'OR', 'NOT',
        'IN', 'BETWEEN', 'LIKE', 'IS', 'NULL', 'TRUE', 'FALSE', 'ORDER',
        'BY', 'GROUP', 'HAVING', 'LIMIT', 'OFFSET', 'UNION', 'ALL', 'DISTINCT',
        'COUNT', 'SUM', 'AVG', 'MIN', 'MAX', 'CASE', 'WHEN', 'THEN', 'ELSE',
        'END', 'CREATE', 'ALTER', 'DROP', 'TABLE', 'INDEX', 'VIEW', 'DATABASE',
        'SCHEMA', 'PRIMARY', 'KEY', 'FOREIGN', 'REFERENCES', 'CONSTRAINT',
        'UNIQUE', 'DEFAULT', 'CHECK', 'CASCADE', 'SET', 'GRANT', 'REVOKE',
    ];

    /**
     * Checks if a word is a SQL reserved word.
     */
    public function isReservedWord(string $word): bool
    {
        return in_array(strtoupper($word), $this->reservedWords, true);
    }

    /**
     * Adds custom reserved words.
     */
    public function addReservedWords(array $words): self
    {
        $this->reservedWords = array_merge(
            $this->reservedWords,
            array_map('strtoupper', $words)
        );
        return $this;
    }

    /**
     * Checks if a reference is a function call.
     * Examples: COUNT(*), SUM(amount), NOW()
     */
    public function isFunction(string $ref): bool
    {
        return preg_match('/^[A-Z_][A-Z0-9_]*\s*\(/i', trim($ref));
    }

    /**
     * Checks if a reference is a literal value.
     */
    public function isLiteral(string $ref): bool
    {
        $ref = trim($ref);

        // Check for numbers
        if (is_numeric($ref)) {
            return true;
        }

        // Check for quoted strings
        if (preg_match('/^[\'"].*[\'"]$/', $ref)) {
            return true;
        }

        // Check for NULL, TRUE, FALSE
        if (in_array(strtoupper($ref), ['NULL', 'TRUE', 'FALSE'], true)) {
            return true;
        }

        return false;
    }

    /**
     * Parses a complex expression and quotes only the identifiers.
     * Example: "users.name = 'John'" -> "`users`.`name` = 'John'"
     */
    public function quoteExpression(string $expression): string
    {
        // This is a simplified version - complex expressions need more sophisticated parsing
        $tokens = preg_split('/(\s+|,|\(|\)|=|<|>|!|\+|-|\*|\/|\||&)/', $expression, -1, PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY);

        $result = [];
        foreach ($tokens as $token) {
            $trimmed = trim($token);

            // Skip if empty, whitespace, operator, or literal
            if (empty($trimmed) || ctype_space($token) || $this->isLiteral($trimmed) || $this->isFunction($trimmed)) {
                $result[] = $token;
                continue;
            }

            // Check if it's an operator or punctuation
            if (preg_match('/^[=<>!+\-*\/|&(),]$/', $trimmed)) {
                $result[] = $token;
                continue;
            }

            // Check if it's a reserved word
            if ($this->isReservedWord($trimmed)) {
                $result[] = $token;
                continue;
            }

            // Assume it's an identifier - quote it
            [$table, $column] = $this->parseColumnRef($trimmed);
            $result[] = $this->quoteColumnRef($table, $column);
        }

        return implode('', $result);
    }

    /**
     * Validates an identifier name.
     *
     * @throws \InvalidArgumentException if invalid
     */
    public function validateIdentifier(string $ident): bool
    {
        // Check for SQL injection attempts
        if (preg_match('/[;\'"\\\\]/', $ident)) {
            throw new \InvalidArgumentException("Invalid identifier: contains dangerous characters");
        }

        // Check max length (MySQL: 64, PostgreSQL: 63)
        if (strlen($ident) > 64) {
            throw new \InvalidArgumentException("Identifier too long: max 64 characters");
        }

        // Must start with letter or underscore
        if (!preg_match('/^[A-Za-z_]/', $ident)) {
            throw new \InvalidArgumentException("Identifier must start with letter or underscore");
        }

        return true;
    }

    /**
     * Sanitizes an identifier (removes invalid characters).
     */
    public function sanitizeIdentifier(string $ident): string
    {
        // Remove dangerous characters
        $sanitized = preg_replace('/[^\w]/', '_', $ident);

        // Ensure it starts with letter or underscore
        if (preg_match('/^[0-9]/', $sanitized)) {
            $sanitized = '_' . $sanitized;
        }

        // Truncate to max length
        return substr($sanitized, 0, 64);
    }

    /**
     * Quotes a list of columns, preserving wildcards.
     */
    public function quoteColumns(array $columns): array
    {
        return array_map(function($col) {
            if ($this->hasWildcard($col)) {
                return $this->quoteWildcard($col);
            }
            return $this->parseAndQuoteColumn($col);
        }, $columns);
    }

    /**
     * Builds a column list for SELECT.
     */
    public function buildColumnList(array $columns): string
    {
        if (empty($columns) || in_array('*', $columns, true)) {
            return '*';
        }

        $quoted = $this->quoteColumns($columns);
        return implode(', ', $quoted);
    }

    /**
     * Parses and quotes a comma-separated column list.
     */
    public function parseColumnList(string $columnList): string
    {
        $columns = array_map('trim', explode(',', $columnList));
        return $this->buildColumnList($columns);
    }

    /**
     * Removes quotes from an identifier.
     */
    public function unquoteIdentifier(string $ident): string
    {
        $ident = trim($ident);

        // Remove MySQL backticks
        if (preg_match('/^`(.+)`$/', $ident, $m)) {
            return str_replace('``', '`', $m[1]);
        }

        // Remove PostgreSQL/SQLite double quotes
        if (preg_match('/^"(.+)"$/', $ident, $m)) {
            return str_replace('""', '"', $m[1]);
        }

        // Remove SQL Server brackets
        if (preg_match('/^\[(.+)\]$/', $ident, $m)) {
            return str_replace(']]', ']', $m[1]);
        }

        return $ident;
    }

    /**
     * Removes quotes from a qualified identifier.
     */
    public function unquoteQualified(string $ref): string
    {
        [$schema, $table] = $this->parseQualified($ref);

        $table = $this->unquoteIdentifier($table);

        if ($schema !== null) {
            $schema = $this->unquoteIdentifier($schema);
            return "$schema.$table";
        }

        return $table;
    }

    /**
     * Converts identifier to lowercase (PostgreSQL style).
     */
    public function toLowerIdentifier(string $ident): string
    {
        return strtolower($this->unquoteIdentifier($ident));
    }

    /**
     * Converts identifier to uppercase (Oracle style).
     */
    public function toUpperIdentifier(string $ident): string
    {
        return strtoupper($this->unquoteIdentifier($ident));
    }

    /**
     * Normalizes identifier case based on database driver.
     */
    public function normalizeCase(string $ident): string
    {
        $driver = $this->conn->pdo()->getAttribute(\PDO::ATTR_DRIVER_NAME);

        return match($driver) {
            'pgsql' => $this->toLowerIdentifier($ident),
            'oracle' => $this->toUpperIdentifier($ident),
            default => $ident,
        };
    }

    /**
     * Splits a qualified identifier into parts without quoting.
     */
    public function splitQualified(string $ref): array
    {
        return $this->parseFullyQualified($ref);
    }

    /**
     * Joins identifier parts with proper quoting.
     */
    public function joinIdentifiers(array $parts): string
    {
        $quoted = array_map(fn($p) => $this->quoteIdentifier($p), array_filter($parts));
        return implode('.', $quoted);
    }

    /**
     * Compares two identifiers (case-insensitive).
     */
    public function identifiersEqual(string $id1, string $id2): bool
    {
        $unquoted1 = $this->unquoteIdentifier($id1);
        $unquoted2 = $this->unquoteIdentifier($id2);

        return strcasecmp($unquoted1, $unquoted2) === 0;
    }

    /**
     * Gets the unqualified name from a qualified identifier.
     * Example: "schema.users" -> "users"
     */
    public function getUnqualifiedName(string $ref): string
    {
        [$schema, $table] = $this->parseQualified($ref);
        return $table;
    }

    /**
     * Gets the schema/qualifier from a qualified identifier.
     * Example: "schema.users" -> "schema"
     */
    public function getQualifier(string $ref): ?string
    {
        [$schema, $table] = $this->parseQualified($ref);
        return $schema;
    }
}
