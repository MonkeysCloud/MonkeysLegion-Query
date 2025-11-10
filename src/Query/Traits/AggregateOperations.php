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
}
