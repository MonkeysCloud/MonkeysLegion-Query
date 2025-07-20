<?php
declare(strict_types=1);

namespace MonkeysLegion\Query;

use MonkeysLegion\Database\MySQL\Connection;
use MonkeysLegion\Entity\Hydrator;
use PDO;

/**
 * QueryBuilder — a fluent SQL builder supporting SELECT/INSERT/UPDATE/DELETE
 * with joins, conditions, grouping, ordering and pagination.
 */
final class QueryBuilder
{
    /** @var array<string,mixed> */
    private array $parts = [
        'select'   => '*',
        'distinct' => false,
        'from'     => '',
        'joins'    => [],
        'where'    => [],
        'groupBy'  => [],
        'having'   => [],
        'orderBy'  => [],
        'limit'    => null,
        'offset'   => null,
        'custom'   => null,
    ];

    /** @var array<string,mixed> */
    private array $params = [];

    /** @var int */
    private int $counter = 0;

    /**
     * Constructor.
     *
     * @param Connection $conn Database connection instance.
     */
    public function __construct(private Connection $conn) {}

    /**
     * Resets the query builder to its initial state.
     */
    public function select(string|array $columns = ['*']): self
    {
        $this->parts['select'] = is_array($columns)
            ? implode(', ', $columns)
            : $columns;
        return $this;
    }

    /**
     * Adds DISTINCT to the SELECT statement.
     */
    public function distinct(): self
    {
        $this->parts['distinct'] = true;
        return $this;
    }

    /**
     * Sets the FROM clause.
     *
     * @param string $table The table name.
     * @param string|null $alias Optional alias for the table.
     */
    public function from(string $table, ?string $alias = null): self
    {
        $this->parts['from'] = $alias ? "$table AS $alias" : $table;
        return $this;
    }

    /**
     * Adds a JOIN clause.
     *
     * @param string $table The table to join.
     * @param string $alias The alias for the joined table.
     * @param string $first The first column for the join condition.
     * @param string $operator The operator for the join condition (e.g., '=', '<>', etc.).
     * @param string $second The second column for the join condition.
     * @param string $type The type of join (INNER, LEFT, RIGHT).
     */
    public function join(
        string $table,
        string $alias,
        string $first,
        string $operator,
        string $second,
        string $type = 'INNER'
    ): self {
        $this->parts['joins'][] = strtoupper($type)
            . " JOIN $table AS $alias ON $first $operator $second";
        return $this;
    }

    /**
     * Adds an INNER JOIN clause.
     *
     * @param string $table The table to join.
     * @param string $alias The alias for the joined table.
     * @param string $first The first column for the join condition.
     * @param string $operator The operator for the join condition (e.g., '=', '<>', etc.).
     * @param string $second The second column for the join condition.
     */
    public function leftJoin(string $table, string $alias, string $first, string $operator, string $second): self
    {
        return $this->join($table, $alias, $first, $operator, $second, 'LEFT');
    }

    /**
     * Adds a RIGHT JOIN clause.
     *
     * @param string $table The table to join.
     * @param string $alias The alias for the joined table.
     * @param string $first The first column for the join condition.
     * @param string $operator The operator for the join condition (e.g., '=', '<>', etc.).
     * @param string $second The second column for the join condition.
     */
    public function rightJoin(string $table, string $alias, string $first, string $operator, string $second): self
    {
        return $this->join($table, $alias, $first, $operator, $second, 'RIGHT');
    }

    /**
     * Adds a FULL JOIN clause.
     *
     * @param string $column
     * @param string $operator The operator for the join condition (e.g., '=', '<>', etc.).
     * @param mixed $value
     * @return QueryBuilder
     */
    public function where(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = [
            'type' => count($this->parts['where']) ? 'AND' : '',
            'expr' => "$column $operator $placeholder",
        ];
        return $this;
    }

    /**
     * Adds an AND condition to the WHERE clause.
     *
     * @param string $column The column name.
     * @param string $operator The operator (e.g., '=', '<>', etc.).
     * @param mixed $value The value to compare against.
     */
    public function andWhere(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type' => 'AND', 'expr' => "$column $operator $placeholder"];
        return $this;
    }

    /**
     * Adds an OR condition to the WHERE clause.
     *
     * @param string $column The column name.
     * @param string $operator The operator (e.g., '=', '<>', etc.).
     * @param mixed $value The value to compare against.
     */
    public function orWhere(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type' => 'OR', 'expr' => "$column $operator $placeholder"];
        return $this;
    }

    /**
     * Adds a GROUP BY clause.
     *
     * @param string ...$columns The columns to group by.
     */
    public function groupBy(string ...$columns): self
    {
        $this->parts['groupBy'] = array_unique([...$this->parts['groupBy'], ...$columns]);
        return $this;
    }

    /**
     * Adds a HAVING clause.
     *
     * @param string $column The column name.
     * @param string $operator The operator (e.g., '=', '<>', etc.).
     * @param mixed $value The value to compare against.
     */
    public function having(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['having'][] = "$column $operator $placeholder";
        return $this;
    }

    /**
     * Adds an ORDER BY clause.
     *
     * @param string $column The column to order by.
     * @param string $direction The direction of the order (ASC or DESC).
     */
    public function orderBy(string $column, string $direction = 'ASC'): self
    {
        $this->parts['orderBy'][] = "$column " . strtoupper($direction);
        return $this;
    }

    /**
     * Sets the LIMIT for the query.
     *
     * @param int $limit The maximum number of rows to return.
     */
    public function limit(int $limit): self
    {
        $this->parts['limit'] = max(0, $limit);
        return $this;
    }

    /**
     * Sets the OFFSET for the query.
     *
     * @param int $offset The number of rows to skip before starting to return rows.
     */
    public function offset(int $offset): self
    {
        $this->parts['offset'] = max(0, $offset);
        return $this;
    }

    /**
     * Sets a custom SQL statement.
     *
     * @param string $sql The custom SQL query.
     * @param array $params Optional parameters to bind to the query.
     */
    public function custom(string $sql, array $params = []): self
    {
        $this->parts['custom'] = $sql;
        $this->params          = $params;
        return $this;
    }

    /**
     * Inserts a new row into the specified table.
     *
     * @param string $table The table name.
     * @param array $data The data to insert, as an associative array.
     * @return int The ID of the inserted row.
     * @throws \RuntimeException If the insert fails.
     */
    public function insert(string $table, array $data): int
    {
        $cols = implode(', ', array_keys($data));
        $phs  = implode(', ', array_map(fn (string $k) => ":$k", array_keys($data)));
        $sql  = "INSERT INTO $table ($cols) VALUES ($phs)";

        $stmt  = $this->conn->pdo()->prepare($sql);
        $bound = array_combine(
            array_map(fn (string $k) => ":$k", array_keys($data)),
            $data
        );

        if (!$stmt->execute($bound)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Insert failed: $state/$code – $msg");
        }

        $id = (int) $this->conn->pdo()->lastInsertId();
        $this->reset();
        return $id;
    }

    /**
     * Updates rows in the specified table.
     *
     * @param string $table The table name.
     * @param array $data The data to update, as an associative array.
     * @return QueryBuilder
     */
    public function update(string $table, array $data): self
    {
        $sets = implode(', ', array_map(fn (string $k) => "$k = :$k", array_keys($data)));
        $this->parts['custom'] = "UPDATE $table SET $sets";
        $this->params          = array_combine(
            array_map(fn (string $k) => ":$k", array_keys($data)),
            $data
        );
        return $this;
    }

    /**
     * Deletes rows from the specified table.
     *
     * @param string $table The table name.
     * @return QueryBuilder
     */
    public function delete(string $table): self
    {
        $this->parts['custom'] = "DELETE FROM $table";
        return $this;
    }

    /**
     * Executes the query and returns the number of affected rows.
     *
     * @return int The number of rows affected by the query.
     */
    public function execute(): int
    {
        $sql  = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);
        $count = $stmt->rowCount();
        $this->reset();
        return $count;
    }

    /**
     * Fetches all results as an array of objects.
     *
     * @param string $class The class name to instantiate for each row.
     * @return array An array of objects representing the rows.
     * @throws \ReflectionException
     */
    public function fetchAll(string $class = 'stdClass'): array
    {
        $sql  = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);

        // Retrieve raw rows as associative arrays
        $rows = $stmt->fetchAll(PDO::FETCH_ASSOC);
        $this->reset();

        // If an entity class is requested, hydrate strictly
        if ($class !== 'stdClass' && class_exists($class)) {
            return array_map(
                fn(array $r) => Hydrator::hydrate($class, $r),
                $rows
            );
        }

        // Fallback: return raw associative rows
        return $rows;
    }

    /**
     * Fetches a single row as an object.
     *
     * @param string $class The class name to instantiate for the row.
     * @return object|false An object representing the row, or false if no row was found.
     * @throws \ReflectionException
     */
    public function fetch(string $class = 'stdClass'): object|false
    {
        $sql  = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);

        // Fetch raw row as associative array
        $row = $stmt->fetch(PDO::FETCH_ASSOC);
        $this->reset();

        if (! $row) {
            return false;
        }

        // If an entity class is requested, hydrate strictly
        if ($class !== 'stdClass' && class_exists($class)) {
            return Hydrator::hydrate($class, $row);
        }

        // Fallback: cast to object
        return (object) $row;
    }

    /**
     * Returns the SQL query as a string.
     *
     * @return string The SQL query.
     */
    public function toSql(): string
    {
        if ($this->parts['custom']) {
            return $this->parts['custom'];
        }

        $sql = 'SELECT ' .
            ($this->parts['distinct'] ? 'DISTINCT ' : '') .
            $this->parts['select'] .
            ' FROM ' . $this->parts['from'];

        if ($this->parts['joins']) {
            $sql .= ' ' . implode(' ', $this->parts['joins']);
        }

        if ($this->parts['where']) {
            $clauses = [];
            foreach ($this->parts['where'] as $i => $w) {
                $prefix   = $i && $w['type'] ? ' ' . $w['type'] . ' ' : '';
                $clauses[] = $prefix . $w['expr'];
            }
            $sql .= ' WHERE ' . implode('', $clauses);
        }

        if ($this->parts['groupBy']) {
            $sql .= ' GROUP BY ' . implode(', ', $this->parts['groupBy']);
        }

        if ($this->parts['having']) {
            $sql .= ' HAVING ' . implode(' AND ', $this->parts['having']);
        }

        if ($this->parts['orderBy']) {
            $sql .= ' ORDER BY ' . implode(', ', $this->parts['orderBy']);
        }

        if ($this->parts['limit'] !== null) {
            $sql .= ' LIMIT ' . $this->parts['limit'];
        }

        if ($this->parts['offset'] !== null) {
            $sql .= ' OFFSET ' . $this->parts['offset'];
        }

        return $sql;
    }

    /**
     * Adds a parameter to the query and returns its placeholder.
     *
     * @param mixed $value The value to bind to the query.
     * @return string The placeholder for the parameter.
     */
    private function addParam(mixed $value): string
    {
        $key              = ':p' . $this->counter++;
        $this->params[$key] = $value;
        return $key;
    }

    /**
     * Resets the query builder to its initial state.
     */
    private function reset(): void
    {
        $this->parts = [
            'select'   => '*',
            'distinct' => false,
            'from'     => '',
            'joins'    => [],
            'where'    => [],
            'groupBy'  => [],
            'having'   => [],
            'orderBy'  => [],
            'limit'    => null,
            'offset'   => null,
            'custom'   => null,
        ];
        $this->params  = [];
        $this->counter = 0;
    }
}
