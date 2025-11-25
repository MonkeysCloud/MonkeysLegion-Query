<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

use PDO;

/**
 * Provides SQL aggregate function operations for the query builder.
 * 
 * Implements methods for COUNT, SUM, AVG, MIN, MAX and row existence check.
 * 
 * @property array $parts Query parts storage
 * @property array $params Query parameters
 * @property \MonkeysLegion\Database\Contracts\ConnectionInterface $conn Database connection
 */
trait AggregateOperations
{
    /**
     * @throws \Throwable
     */
    public function count(): int
    {
        // Duplicate and resolve tables before SQL generation
        $countQb = $this->duplicate();
        $this->reset();
        
        try {
            $countQb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log('[qb.count] preflightResolveTables FAILED: ' . $e->getMessage());
            throw $e;
        }

        $hasGroupingOrDistinct = !empty($countQb->parts['groupBy']) || !empty($countQb->parts['distinct']);

        try {
            if ($hasGroupingOrDistinct) {
                // Build inner query without limit/offset/order
                $countQb->parts['limit']   = null;
                $countQb->parts['offset']  = null;
                $countQb->parts['orderBy'] = [];
                $innerSql    = $countQb->toSql();
                $innerParams = $countQb->params;
                $sql  = "SELECT COUNT(*) AS cnt FROM ($innerSql) AS count_subquery";
                $stmt = $this->conn->pdo()->prepare($sql);
                $ok = $stmt->execute($innerParams);

                if (!$ok) {
                    [$state, $code, $msg] = $stmt->errorInfo();
                    error_log("[qb.count] OUTER errorInfo: $state/$code – $msg");
                }

                $row = $stmt->fetch(PDO::FETCH_ASSOC);
                $cnt = (int)($row['cnt'] ?? 0);

                return $cnt;
            }

            // SIMPLE count
            $countQb->parts['select']  = 'COUNT(*) AS cnt';
            $countQb->parts['orderBy'] = [];
            $countQb->parts['limit']   = null;
            $countQb->parts['offset']  = null;

            $sql = $countQb->toSql();

            $stmt = $this->conn->pdo()->prepare($sql);
            $ok = $stmt->execute($countQb->params);

            if (!$ok) {
                [$state, $code, $msg] = $stmt->errorInfo();
                error_log("[qb.count] SIMPLE errorInfo: $state/$code – $msg");
            }

            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            $cnt = (int)($row['cnt'] ?? 0);

            return $cnt;
        } catch (\PDOException $e) {
            error_log('[qb.count] PDOException: ' . $e->getMessage() . ' code=' . $e->getCode());
            throw $e;
        } catch (\Throwable $e) {
            error_log('[qb.count] Throwable: ' . $e->getMessage() . ' code=' . $e->getCode());
            throw $e;
        }
    }

    /**
     * Gets the sum of a column.
     */
    public function sum(string $column): float
    {
        return $this->aggregate('SUM', $column);
    }

    /**
     * Gets the average of a column.
     */
    public function avg(string $column): float
    {
        return $this->aggregate('AVG', $column);
    }

    /**
     * Gets the minimum value of a column.
     */
    public function min(string $column): mixed
    {
        return $this->aggregateRaw('MIN', $column);
    }

    /**
     * Gets the maximum value of a column.
     */
    public function max(string $column): mixed
    {
        return $this->aggregateRaw('MAX', $column);
    }

    /**
     * Checks if any rows exist.
     */
    public function exists(): bool
    {
        $originalSelect = $this->parts['select'];
        $originalLimit = $this->parts['limit'];
        $originalOrderBy = $this->parts['orderBy'];
        $originalOffset = $this->parts['offset'];

        $this->parts['select'] = '1';
        $this->parts['limit'] = 1;
        $this->parts['orderBy'] = [];  // Not needed for existence check
        $this->parts['offset'] = null;  // Not needed for existence check

        $sql = $this->toSql();
        $stmt = $this->conn->pdo()->prepare($sql);

        if (!$stmt->execute($this->params)) {
            [$state, $code, $msg] = $stmt->errorInfo();
            throw new \RuntimeException("Exists query failed: $state/$code – $msg");
        }

        $result = $stmt->fetch(PDO::FETCH_ASSOC);

        $this->parts['select'] = $originalSelect;
        $this->parts['limit'] = $originalLimit;
        $this->parts['orderBy'] = $originalOrderBy;
        $this->parts['offset'] = $originalOffset;

        return $result !== false;
    }

    /**
     * Checks if no rows exist (opposite of exists).
     */
    public function doesntExist(): bool
    {
        return !$this->exists();
    }

    /**
     * Gets a count of distinct values.
     */
    public function countDistinct(string $column): int
    {
        $qb = $this->duplicate();
        $this->reset();

        try {
            $qb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log("[qb.countDistinct] preflightResolveTables FAILED: {$e->getMessage()}");
            throw $e;
        }

        $qb->parts['select'] = "COUNT(DISTINCT $column) AS cnt";
        $qb->parts['orderBy'] = [];
        $qb->parts['limit'] = null;
        $qb->parts['offset'] = null;

        try {
            $sql = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("Count distinct query failed: $state/$code – $msg");
            }

            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            return (int)($row['cnt'] ?? 0);
        } catch (\PDOException $e) {
            error_log("[qb.countDistinct] PDOException: {$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Gets the sum of distinct values.
     */
    public function sumDistinct(string $column): float
    {
        return $this->aggregate('SUM', "DISTINCT $column");
    }

    /**
     * Gets the average of distinct values.
     */
    public function avgDistinct(string $column): float
    {
        return $this->aggregate('AVG', "DISTINCT $column");
    }

    /**
     * Gets the standard deviation of a column.
     */
    public function stdDev(string $column): float
    {
        return $this->aggregate('STDDEV', $column);
    }

    /**
     * Gets the population standard deviation of a column.
     */
    public function stdDevPop(string $column): float
    {
        return $this->aggregate('STDDEV_POP', $column);
    }

    /**
     * Gets the sample standard deviation of a column.
     */
    public function stdDevSamp(string $column): float
    {
        return $this->aggregate('STDDEV_SAMP', $column);
    }

    /**
     * Gets the variance of a column.
     */
    public function variance(string $column): float
    {
        return $this->aggregate('VARIANCE', $column);
    }

    /**
     * Gets the population variance of a column.
     */
    public function varPop(string $column): float
    {
        return $this->aggregate('VAR_POP', $column);
    }

    /**
     * Gets the sample variance of a column.
     */
    public function varSamp(string $column): float
    {
        return $this->aggregate('VAR_SAMP', $column);
    }

    /**
     * Gets a GROUP_CONCAT result (MySQL specific).
     *
     * @param string $column Column to concatenate
     * @param string $separator Separator between values (default: ',')
     * @param bool $distinct Whether to use DISTINCT
     */
    public function groupConcat(string $column, string $separator = ',', bool $distinct = false): ?string
    {
        $qb = $this->duplicate();
        $this->reset();

        try {
            $qb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log("[qb.groupConcat] preflightResolveTables FAILED: {$e->getMessage()}");
            throw $e;
        }

        $distinctKeyword = $distinct ? 'DISTINCT ' : '';
        $sepPlaceholder = $qb->addParam($separator);

        $qb->parts['select'] = "GROUP_CONCAT({$distinctKeyword}{$column} SEPARATOR {$sepPlaceholder}) AS result";
        $qb->parts['orderBy'] = [];
        $qb->parts['limit'] = null;
        $qb->parts['offset'] = null;

        try {
            $sql = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("Group concat query failed: $state/$code – $msg");
            }

            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            return $row['result'] ?? null;
        } catch (\PDOException $e) {
            error_log("[qb.groupConcat] PDOException: {$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Executes a custom aggregate function.
     *
     * @param string $function The aggregate function (e.g., 'COUNT', 'SUM', 'CUSTOM_FUNC')
     * @param string $expression The column or expression to aggregate
     * @return mixed
     * @throws \Throwable
     */
    public function aggregateCustom(string $function, string $expression = '*'): mixed
    {
        return $this->aggregateRaw($function, $expression);
    }

    /**
     * Increments a column's value by a given amount (useful with aggregate).
     * Returns the new aggregated value.
     */
    public function increment(string $column, int|float $amount = 1): float
    {
        return $this->sum($column) + $amount;
    }

    /**
     * Decrements a column's value by a given amount (useful with aggregate).
     * Returns the new aggregated value.
     */
    public function decrement(string $column, int|float $amount = 1): float
    {
        return $this->sum($column) - $amount;
    }

    /**
     * Counts rows where a condition is true (MySQL).
     *
     * Example: countWhere('status', '=', 'active')
     * Results in: COUNT(CASE WHEN status = ? THEN 1 END)
     */
    public function countWhere(string $column, string $operator, mixed $value): int
    {
        $placeholder = $this->addParam($value);
        $expression = "CASE WHEN $column $operator $placeholder THEN 1 END";

        $qb = $this->duplicate();
        $this->reset();

        try {
            $qb->preflightResolveTables();
        } catch (\Throwable $e) {
            error_log("[qb.countWhere] preflightResolveTables FAILED: {$e->getMessage()}");
            throw $e;
        }

        $qb->parts['select'] = "COUNT($expression) AS cnt";
        $qb->parts['orderBy'] = [];
        $qb->parts['limit'] = null;
        $qb->parts['offset'] = null;

        try {
            $sql = $qb->toSql();
            $stmt = $this->conn->pdo()->prepare($sql);

            if (!$stmt->execute($qb->params)) {
                [$state, $code, $msg] = $stmt->errorInfo();
                throw new \RuntimeException("Conditional count query failed: $state/$code – $msg");
            }

            $row = $stmt->fetch(PDO::FETCH_ASSOC);
            return (int)($row['cnt'] ?? 0);
        } catch (\PDOException $e) {
            error_log("[qb.countWhere] PDOException: {$e->getMessage()}");
            throw $e;
        }
    }

    /**
     * Sums values where a condition is true.
     */
    public function sumWhere(string $column, string $whereColumn, string $operator, mixed $value): float
    {
        $placeholder = $this->addParam($value);
        $expression = "CASE WHEN $whereColumn $operator $placeholder THEN $column ELSE 0 END";
        return $this->aggregate('SUM', $expression);
    }
}
