<?php
declare(strict_types=1);

namespace MonkeysLegion\Repository;

use MonkeysLegion\Query\QueryBuilder;

final class RepositoryFactory
{
    public function __construct(private QueryBuilder $qb) {}

    /**
     * @template T of EntityRepository
     * @param class-string<T> $repoClass
     * @return object
     */
    public function create(string $repoClass): object
    {
        if (! is_subclass_of($repoClass, EntityRepository::class, true)) {
            throw new \InvalidArgumentException(
                "{$repoClass} must extend " . EntityRepository::class
            );
        }
        return new $repoClass($this->qb);
    }
}