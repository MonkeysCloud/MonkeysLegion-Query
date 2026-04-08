<?php
declare(strict_types=1);

namespace MonkeysLegion\Query\Repository;

use MonkeysLegion\Entity\Attributes\Field;
use MonkeysLegion\Entity\Attributes\ManyToOne;
use MonkeysLegion\Entity\Attributes\OneToOne;
use ReflectionClass;
use ReflectionProperty;

/**
 * MonkeysLegion Framework — Query Package
 *
 * Entity hydrator: converts database rows ↔ entity objects.
 *
 * Performance: caches reflection metadata per class. Each class
 * is analyzed exactly once per process lifecycle.
 *
 * @copyright 2026 MonkeysCloud Team
 * @license   MIT
 */
final class EntityHydrator
{
    // ── Reflection Cache ────────────────────────────────────────

    /** @var array<string, ReflectionClass<object>> */
    private static array $reflCache = [];

    /** @var array<string, list<array{prop: ReflectionProperty, column: string, type: string}>> */
    private static array $fieldMapCache = [];

    /**
     * Hydrate a database row into an entity object.
     *
     * @template T of object
     *
     * @param class-string<T>      $class Entity class.
     * @param array<string, mixed> $row   Database row (column => value).
     *
     * @return T
     */
    public function hydrate(string $class, array $row): object
    {
        $ref = self::reflect($class);
        $entity = $ref->newInstanceWithoutConstructor();
        $fieldMap = $this->getFieldMap($class);

        foreach ($fieldMap as $mapping) {
            $column = $mapping['column'];
            if (!array_key_exists($column, $row)) {
                continue;
            }

            $value = $this->castFromDatabase($row[$column], $mapping['type'], $mapping['prop']);
            $mapping['prop']->setValue($entity, $value);
        }

        return $entity;
    }

    /**
     * Dehydrate an entity into a database-ready associative array.
     *
     * @param object $entity
     *
     * @return array<string, mixed>
     */
    public function dehydrate(object $entity): array
    {
        $class = get_class($entity);
        $fieldMap = $this->getFieldMap($class);
        $data = [];

        foreach ($fieldMap as $mapping) {
            $prop = $mapping['prop'];
            if (!$prop->isInitialized($entity)) {
                continue;
            }

            $value = $prop->getValue($entity);
            $data[$mapping['column']] = $this->castToDatabase($value, $mapping['type']);
        }

        return $data;
    }

    /**
     * Set a single property value on an entity (bypasses visibility).
     */
    public function setPropertyValue(object $entity, string $property, mixed $value): void
    {
        $ref = self::reflect(get_class($entity));
        if ($ref->hasProperty($property)) {
            $prop = $ref->getProperty($property);
            $prop->setValue($entity, $value);
        }
    }

    /**
     * Get a single property value from an entity.
     */
    public function getPropertyValue(object $entity, string $property): mixed
    {
        $ref = self::reflect(get_class($entity));
        if (!$ref->hasProperty($property)) {
            return null;
        }

        $prop = $ref->getProperty($property);
        return $prop->isInitialized($entity) ? $prop->getValue($entity) : null;
    }

    /**
     * Get the entity ID value.
     */
    public function getEntityId(object $entity): string|int|null
    {
        $value = $this->getPropertyValue($entity, 'id');
        return $value !== null ? $value : null;
    }

    /**
     * Extract the column names for persistence.
     *
     * @return list<string>
     */
    public function getColumns(string $class): array
    {
        return array_map(fn(array $m) => $m['column'], $this->getFieldMap($class));
    }

    // ── Private ─────────────────────────────────────────────────

    /**
     * Get the field map for a class (cached).
     *
     * @return list<array{prop: ReflectionProperty, column: string, type: string}>
     */
    private function getFieldMap(string $class): array
    {
        if (isset(self::$fieldMapCache[$class])) {
            return self::$fieldMapCache[$class];
        }

        $ref = self::reflect($class);
        $map = [];

        foreach ($ref->getProperties() as $prop) {
            // Handle #[Field] attributes
            $fieldAttrs = $prop->getAttributes(Field::class);
            if ($fieldAttrs) {
                /** @var Field $field */
                $field = $fieldAttrs[0]->newInstance();
                $column = $prop->getName();
                $type = $field->type;

                // Override with BackedEnum detection from property type
                $inferredType = $this->inferTypeFromProperty($prop);
                if (str_starts_with($inferredType, 'enum:')) {
                    $type = $inferredType;
                }

                $map[] = [
                    'prop'   => $prop,
                    'column' => $column,
                    'type'   => $type,
                ];
                continue;
            }

            // Handle #[ManyToOne] — FK column
            $m2oAttrs = $prop->getAttributes(ManyToOne::class);
            if ($m2oAttrs) {
                $column = $this->toSnakeCase($prop->getName()) . '_id';
                $map[] = [
                    'prop'   => $prop,
                    'column' => $column,
                    'type'   => 'relation',
                ];
                continue;
            }

            // Handle #[OneToOne] owning side
            $o2oAttrs = $prop->getAttributes(OneToOne::class);
            if ($o2oAttrs) {
                /** @var OneToOne $o2o */
                $o2o = $o2oAttrs[0]->newInstance();
                if ($o2o->mappedBy === null) {
                    $column = $this->toSnakeCase($prop->getName()) . '_id';
                    $map[] = [
                        'prop'   => $prop,
                        'column' => $column,
                        'type'   => 'relation',
                    ];
                }
            }
        }

        self::$fieldMapCache[$class] = $map;
        return $map;
    }

    /**
     * Convert camelCase property name to snake_case column name.
     * Result is cached statically to avoid repeated regex per property per hydration.
     */
    private static function toSnakeCase(string $input): string
    {
        static $cache = [];
        return $cache[$input] ??= strtolower(preg_replace('/(?<!^)[A-Z]/', '_$0', $input));
    }

    /**
     * Infer the database type from a ReflectionProperty's type declaration.
     */
    private function inferTypeFromProperty(ReflectionProperty $prop): string
    {
        $type = $prop->getType();
        if ($type === null) {
            return 'string';
        }

        $name = $type instanceof \ReflectionNamedType ? $type->getName() : 'string';

        // Check for BackedEnum
        if ($type instanceof \ReflectionNamedType
            && !$type->isBuiltin()
            && is_subclass_of($name, \BackedEnum::class)
        ) {
            return 'enum:' . $name;
        }

        return match ($name) {
            'int'               => 'integer',
            'float'             => 'float',
            'bool'              => 'boolean',
            'array'             => 'json',
            'DateTimeImmutable', 'DateTime', 'DateTimeInterface' => 'datetime',
            default             => 'string',
        };
    }

    /**
     * Cast a database value to the appropriate PHP type.
     */
    private function castFromDatabase(mixed $value, string $type, ReflectionProperty $prop): mixed
    {
        if ($value === null) {
            return null;
        }

        // BackedEnum: type is 'enum:Full\Class\Name'
        if (str_starts_with($type, 'enum:')) {
            $enumClass = substr($type, 5);
            return $enumClass::from($value);
        }

        return match ($type) {
            'integer', 'int', 'unsignedBigInt', 'bigInt', 'smallInt', 'tinyInt' => (int) $value,
            'float', 'decimal'         => (float) $value,
            'boolean', 'bool'          => (bool) $value,
            'json'                     => is_string($value) ? json_decode($value, true) : $value,
            'datetime', 'timestamp', 'date', 'time' => new \DateTimeImmutable((string) $value),
            'relation'                 => $value,
            default                    => (string) $value,
        };
    }

    /**
     * Cast a PHP value to a database-storable format.
     */
    private function castToDatabase(mixed $value, string $type): mixed
    {
        if ($value === null) {
            return null;
        }

        // BackedEnum: extract the backed value
        if (str_starts_with($type, 'enum:') && $value instanceof \BackedEnum) {
            return $value->value;
        }

        return match ($type) {
            'json'                     => is_array($value) || is_object($value) ? json_encode($value) : $value,
            'datetime', 'timestamp'    => $value instanceof \DateTimeInterface ? $value->format('Y-m-d H:i:s') : $value,
            'date'                     => $value instanceof \DateTimeInterface ? $value->format('Y-m-d') : $value,
            'time'                     => $value instanceof \DateTimeInterface ? $value->format('H:i:s') : $value,
            'boolean', 'bool'          => (int) $value,
            'relation'                 => is_object($value) ? $this->getEntityId($value) : $value,
            default                    => $value,
        };
    }

    /**
     * Get (cached) ReflectionClass.
     *
     * @template T of object
     * @param class-string<T> $class
     * @return ReflectionClass<T>
     */
    private static function reflect(string $class): ReflectionClass
    {
        return self::$reflCache[$class] ??= new ReflectionClass($class);
    }

    /**
     * Clear all caches (call after schema changes or in tests).
     */
    public static function clearCache(): void
    {
        self::$reflCache = [];
        self::$fieldMapCache = [];
    }
}
