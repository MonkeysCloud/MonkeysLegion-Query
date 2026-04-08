<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Exceptions;

/**
 * Thrown when entity hydration fails (missing properties, type mismatches, etc.).
 */
class HydrationException extends \RuntimeException
{
    public function __construct(
        public readonly string $entityClass,
        string $reason,
        ?\Throwable $previous = null,
    ) {
        parent::__construct("Hydration failed for {$entityClass}: {$reason}", 0, $previous);
    }
}
