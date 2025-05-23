<?php
declare(strict_types=1);

namespace MonkeysLegion\Query;

use MonkeysLegion\Database\MySQL\Connection;
use PDO;

final class QueryBuilder
{
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

    private array $params = [];
    private int   $counter = 0;

    public function __construct(private Connection $conn) {}

    public function select(string|array $columns = ['*']): self
    {
        $this->parts['select'] = is_array($columns)
            ? implode(', ', $columns)
            : $columns;
        return $this;
    }

    public function distinct(): self
    {
        $this->parts['distinct'] = true;
        return $this;
    }

    public function from(string $table, ?string $alias = null): self
    {
        $this->parts['from'] = $alias
            ? "$table AS $alias"
            : $table;
        return $this;
    }

    public function join(string $table, string $alias, string $first, string $operator, string $second, string $type = 'INNER'): self
    {
        $this->parts['joins'][] = strtoupper($type)
            . " JOIN $table AS $alias ON $first $operator $second";
        return $this;
    }

    public function leftJoin(string $table, string $alias, string $first, string $operator, string $second): self
    {
        return $this->join($table, $alias, $first, $operator, $second, 'LEFT');
    }

    public function rightJoin(string $table, string $alias, string $first, string $operator, string $second): self
    {
        return $this->join($table, $alias, $first, $operator, $second, 'RIGHT');
    }

    public function where(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = [
            'type' => count($this->parts['where']) ? 'OR' : 'AND',
            'expr' => "$column $operator $placeholder"
        ];
        return $this;
    }

    public function andWhere(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type'=>'AND','expr'=>"$column $operator $placeholder"];
        return $this;
    }

    public function orWhere(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['where'][] = ['type'=>'OR','expr'=>"$column $operator $placeholder"];
        return $this;
    }

    public function groupBy(string ...$columns): self
    {
        $this->parts['groupBy'] = array_merge($this->parts['groupBy'], $columns);
        return $this;
    }

    public function having(string $column, string $operator, mixed $value): self
    {
        $placeholder = $this->addParam($value);
        $this->parts['having'][] = "$column $operator $placeholder";
        return $this;
    }

    public function orderBy(string $column, string $direction = 'ASC'): self
    {
        $this->parts['orderBy'][] = "$column " . strtoupper($direction);
        return $this;
    }

    public function limit(int $limit): self
    {
        $this->parts['limit'] = $limit;
        return $this;
    }

    public function offset(int $offset): self
    {
        $this->parts['offset'] = $offset;
        return $this;
    }

    public function custom(string $sql, array $params = []): self
    {
        $this->parts['custom'] = $sql;
        $this->params = $params;
        return $this;
    }

    public function insert(string $table, array $data): int
    {
        $cols = implode(', ', array_keys($data));
        $phs  = implode(', ', array_map(fn($k)=>":$k", array_keys($data)));
        $sql  = "INSERT INTO $table ($cols) VALUES ($phs)";
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($data);
        return (int)$this->conn->pdo()->lastInsertId();
    }

    public function update(string $table, array $data): self
    {
        $sets = implode(', ', array_map(fn($k)=>"$k = :$k", array_keys($data)));
        $this->parts['custom'] = "UPDATE $table SET $sets";
        $this->params = $data;
        return $this;
    }

    public function delete(string $table): self
    {
        $this->parts['custom'] = "DELETE FROM $table";
        return $this;
    }

    public function execute(): int
    {
        $sql  = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);
        return $stmt->rowCount();
    }

    public function fetchAll(string $class='stdClass'): array
    {
        $sql  = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);
        return $stmt->fetchAll(PDO::FETCH_CLASS, $class);
    }

    public function fetch(string $class='stdClass'): object|false
    {
        $sql  = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);
        $stmt->execute($this->params);
        return $stmt->fetchObject($class);
    }

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
            foreach ($this->parts['where'] as $i=>$w) {
                $prefix = $i ? ' '.$w['type'].' ' : '';
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

    private function addParam(mixed $value): string
    {
        $key = ':p'.$this->counter++;
        $this->params[$key] = $value;
        return $key;
    }
}