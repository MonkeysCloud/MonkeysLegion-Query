<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Exceptions;

/**
 * Thrown when an entity is not found by ID (findOrFail, refresh, etc.).
 * Maps naturally to HTTP 404 in controller error handlers.
 */
class EntityNotFoundException extends \RuntimeException
{
    public function __construct(
        public readonly string $entityClass,
        public readonly string|int $id,
    ) {
        parent::__construct("Entity {$entityClass} with id '{$id}' not found.");
    }
}
