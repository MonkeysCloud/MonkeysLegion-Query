<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Immutable value object describing a single entity relationship.
 * Extracted from #[ManyToOne], #[OneToMany], #[OneToOne], #[ManyToMany]
 * attributes via cached reflection (one pass per class per process).
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final readonly class RelationMetadata
{
    /**
     * @param string      $propertyName  Entity property that holds the relation.
     * @param string      $relationType  'ManyToOne'|'OneToMany'|'OneToOne'|'ManyToMany'
     * @param class-string $targetEntity  Fully-qualified class name of the related entity.
     * @param string      $foreignKey    Computed foreign key column (e.g. 'user_id').
     * @param string|null $mappedBy      Inverse side property name (for OneToMany/ManyToMany).
     * @param string|null $inversedBy    Owning side property name.
     * @param string|null $joinTable     Pivot table name (ManyToMany only).
     * @param string|null $joinSourceKey Source FK column in pivot table.
     * @param string|null $joinTargetKey Target FK column in pivot table.
     * @param bool        $isCollection  Whether the relation is a collection (OneToMany/ManyToMany).
     */
    public function __construct(
        public string  $propertyName,
        public string  $relationType,
        public string  $targetEntity,
        public string  $foreignKey,
        public ?string $mappedBy = null,
        public ?string $inversedBy = null,
        public ?string $joinTable = null,
        public ?string $joinSourceKey = null,
        public ?string $joinTargetKey = null,
        public bool    $isCollection = false,
    ) {}
}
