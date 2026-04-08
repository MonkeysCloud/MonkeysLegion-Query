<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Exceptions;

/**
 * Thrown when a database query fails (SQL error, constraint violation, etc.).
 * Wraps the original PDOException with SQL context.
 */
class QueryException extends \RuntimeException
{
    public function __construct(
        public readonly string $sql,
        public readonly array $bindings,
        \Throwable $previous,
    ) {
        $message = $previous->getMessage() . " (SQL: {$sql})";
        parent::__construct($message, (int) $previous->getCode(), $previous);
    }
}
