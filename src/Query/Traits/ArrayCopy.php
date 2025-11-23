<?php

declare(strict_types=1);

namespace MonkeysLegion\Query\Traits;

/**
 * Provides deep array copying functionality.
 * 
 * Used for creating independent copies of query builder state.
 */
trait ArrayCopy
{
    /**
     * Deep copy an array.
     */
    public function deepCopyArray(array $array): array
    {
        $copy = [];
        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopyArray($value);
            } elseif (is_object($value)) {
                $copy[$key] = clone $value;
            } else {
                $copy[$key] = $value;
            }
        }
        return $copy;
    }

    /**
     * Creates a shallow copy of an array (only top level).
     */
    public function shallowCopyArray(array $array): array
    {
        return $array;
    }

    /**
     * Creates a selective deep copy - only specified keys are deep copied.
     *
     * @param array $array Array to copy
     * @param array $deepKeys Keys that should be deep copied
     */
    public function selectiveDeepCopy(array $array, array $deepKeys): array
    {
        $copy = [];
        foreach ($array as $key => $value) {
            if (in_array($key, $deepKeys, true) && is_array($value)) {
                $copy[$key] = $this->deepCopyArray($value);
            } elseif (is_object($value)) {
                $copy[$key] = clone $value;
            } else {
                $copy[$key] = $value;
            }
        }
        return $copy;
    }

    /**
     * Deep copies an array with circular reference detection.
     *
     * @param array $array Array to copy
     * @param array $seen Track already-seen objects to detect circular refs
     */
    public function deepCopyArraySafe(array $array, array &$seen = []): array
    {
        $copy = [];

        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopyArraySafe($value, $seen);
            } elseif (is_object($value)) {
                $objHash = spl_object_hash($value);

                if (isset($seen[$objHash])) {
                    // Circular reference detected - use reference
                    $copy[$key] = $seen[$objHash];
                } else {
                    $cloned = clone $value;
                    $seen[$objHash] = $cloned;
                    $copy[$key] = $cloned;
                }
            } else {
                $copy[$key] = $value;
            }
        }

        return $copy;
    }

    /**
     * Checks if an array contains circular references.
     */
    public function hasCircularReferences(array $array, array &$seen = []): bool
    {
        foreach ($array as $value) {
            if (is_array($value)) {
                if ($this->hasCircularReferences($value, $seen)) {
                    return true;
                }
            } elseif (is_object($value)) {
                $objHash = spl_object_hash($value);
                if (isset($seen[$objHash])) {
                    return true;
                }
                $seen[$objHash] = true;
            }
        }

        return false;
    }

    /**
     * Deep copies an array and applies a transformation callback to each value.
     *
     * @param array $array Array to copy
     * @param callable $callback Function to transform each value: fn($value, $key) => $newValue
     */
    public function deepCopyWithTransform(array $array, callable $callback): array
    {
        $copy = [];

        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopyWithTransform($value, $callback);
            } else {
                $transformed = $callback($value, $key);

                if (is_object($transformed)) {
                    $copy[$key] = clone $transformed;
                } else {
                    $copy[$key] = $transformed;
                }
            }
        }

        return $copy;
    }

    /**
     * Deep copies an array and filters out values based on callback.
     *
     * @param array $array Array to copy
     * @param callable $callback Filter function: fn($value, $key) => bool
     */
    public function deepCopyWithFilter(array $array, callable $callback): array
    {
        $copy = [];

        foreach ($array as $key => $value) {
            if ($callback($value, $key)) {
                if (is_array($value)) {
                    $copy[$key] = $this->deepCopyWithFilter($value, $callback);
                } elseif (is_object($value)) {
                    $copy[$key] = clone $value;
                } else {
                    $copy[$key] = $value;
                }
            }
        }

        return $copy;
    }

    /**
     * Deep compares two arrays for equality.
     */
    public function deepArrayEquals(array $array1, array $array2): bool
    {
        if (count($array1) !== count($array2)) {
            return false;
        }

        foreach ($array1 as $key => $value) {
            if (!array_key_exists($key, $array2)) {
                return false;
            }

            if (is_array($value)) {
                if (!is_array($array2[$key]) || !$this->deepArrayEquals($value, $array2[$key])) {
                    return false;
                }
            } elseif (is_object($value)) {
                if (!is_object($array2[$key]) || $value != $array2[$key]) {
                    return false;
                }
            } else {
                if ($value !== $array2[$key]) {
                    return false;
                }
            }
        }

        return true;
    }

    /**
     * Computes the difference between two arrays (deep).
     * Returns elements in array1 that are not in array2.
     */
    public function deepArrayDiff(array $array1, array $array2): array
    {
        $diff = [];

        foreach ($array1 as $key => $value) {
            if (!array_key_exists($key, $array2)) {
                $diff[$key] = $value;
            } elseif (is_array($value) && is_array($array2[$key])) {
                $nested = $this->deepArrayDiff($value, $array2[$key]);
                if (!empty($nested)) {
                    $diff[$key] = $nested;
                }
            } elseif ($value !== $array2[$key]) {
                $diff[$key] = $value;
            }
        }

        return $diff;
    }

    /**
     * Deep copies and serializes an array (makes all objects serializable).
     */
    public function deepCopySerializable(array $array): array
    {
        return $this->deepCopyWithTransform($array, function($value, $key) {
            if (is_object($value)) {
                // Convert objects to arrays for serialization
                return (array) $value;
            }
            return $value;
        });
    }

    /**
     * Creates a JSON-safe copy of an array.
     * Converts objects to arrays and handles special types.
     */
    public function deepCopyJsonSafe(array $array): array
    {
        return $this->deepCopyWithTransform($array, function($value, $key) {
            if (is_object($value)) {
                if (method_exists($value, 'toArray')) {
                    return $value->toArray();
                }
                return (array) $value;
            }

            if (is_resource($value)) {
                return null;
            }

            return $value;
        });
    }

    /**
     * Deep copies an array up to a specified depth.
     *
     * @param array $array Array to copy
     * @param int $maxDepth Maximum depth to copy (0 = shallow copy)
     * @param int $currentDepth Current depth level
     */
    public function deepCopyToDepth(array $array, int $maxDepth, int $currentDepth = 0): array
    {
        if ($currentDepth >= $maxDepth) {
            return $array; // Return shallow copy at max depth
        }

        $copy = [];

        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopyToDepth($value, $maxDepth, $currentDepth + 1);
            } elseif (is_object($value)) {
                $copy[$key] = clone $value;
            } else {
                $copy[$key] = $value;
            }
        }

        return $copy;
    }

    /**
     * Gets the maximum depth of an array.
     */
    public function getArrayDepth(array $array): int
    {
        $maxDepth = 1;

        foreach ($array as $value) {
            if (is_array($value)) {
                $depth = $this->getArrayDepth($value) + 1;
                $maxDepth = max($maxDepth, $depth);
            }
        }

        return $maxDepth;
    }

    /**
     * Deep copies only arrays, leaving objects as references.
     */
    public function deepCopyArraysOnly(array $array): array
    {
        $copy = [];

        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopyArraysOnly($value);
            } else {
                $copy[$key] = $value; // Keep object references
            }
        }

        return $copy;
    }

    /**
     * Deep copies and clones only specific object types.
     *
     * @param array $array Array to copy
     * @param array $cloneableTypes Class names to clone
     */
    public function deepCopySelectiveClone(array $array, array $cloneableTypes): array
    {
        $copy = [];

        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopySelectiveClone($value, $cloneableTypes);
            } elseif (is_object($value)) {
                $shouldClone = false;
                foreach ($cloneableTypes as $type) {
                    if ($value instanceof $type) {
                        $shouldClone = true;
                        break;
                    }
                }

                $copy[$key] = $shouldClone ? clone $value : $value;
            } else {
                $copy[$key] = $value;
            }
        }

        return $copy;
    }

    /**
     * Fast shallow copy using array_merge (faster for simple arrays).
     */
    public function fastCopy(array $array): array
    {
        return array_merge([], $array);
    }

    /**
     * Copies an array using serialization (slower but handles complex structures).
     */
    public function copyViaSerialization(array $array): array
    {
        return unserialize(serialize($array));
    }

    /**
     * Copies using JSON encode/decode (fast but lossy for objects).
     */
    public function copyViaJson(array $array): array
    {
        return json_decode(json_encode($array), true);
    }

    /**
     * Deep copies an array and validates each value with a callback.
     *
     * @param array $array Array to copy
     * @param callable $validator Validation function: fn($value, $key) => bool
     * @throws \InvalidArgumentException if validation fails
     */
    public function deepCopyWithValidation(array $array, callable $validator): array
    {
        $copy = [];

        foreach ($array as $key => $value) {
            if (!$validator($value, $key)) {
                throw new \InvalidArgumentException("Validation failed for key: $key");
            }

            if (is_array($value)) {
                $copy[$key] = $this->deepCopyWithValidation($value, $validator);
            } elseif (is_object($value)) {
                $copy[$key] = clone $value;
            } else {
                $copy[$key] = $value;
            }
        }

        return $copy;
    }

    /**
     * Deep copies an array and ensures all values are of allowed types.
     *
     * @param array $array Array to copy
     * @param array $allowedTypes Allowed types: 'string', 'int', 'array', 'object', etc.
     */
    public function deepCopyTyped(array $array, array $allowedTypes): array
    {
        return $this->deepCopyWithValidation($array, function($value, $key) use ($allowedTypes) {
            $type = gettype($value);

            if (is_object($value)) {
                $type = get_class($value);
            }

            foreach ($allowedTypes as $allowed) {
                if ($type === $allowed || (is_object($value) && $value instanceof $allowed)) {
                    return true;
                }
            }

            return false;
        });
    }

    /**
     * Flattens a multi-dimensional array into a single level.
     * Keys are concatenated with a separator.
     *
     * @param array $array Array to flatten
     * @param string $separator Key separator (default: '.')
     * @param string $prefix Key prefix for recursion
     */
    public function flattenArray(array $array, string $separator = '.', string $prefix = ''): array
    {
        $result = [];

        foreach ($array as $key => $value) {
            $newKey = $prefix === '' ? $key : $prefix . $separator . $key;

            if (is_array($value)) {
                $result = array_merge($result, $this->flattenArray($value, $separator, $newKey));
            } else {
                $result[$newKey] = $value;
            }
        }

        return $result;
    }

    /**
     * Unflattens a flat array back to multi-dimensional.
     *
     * @param array $array Flattened array
     * @param string $separator Key separator
     */
    public function unflattenArray(array $array, string $separator = '.'): array
    {
        $result = [];

        foreach ($array as $key => $value) {
            $keys = explode($separator, $key);
            $temp = &$result;

            foreach ($keys as $k) {
                if (!isset($temp[$k])) {
                    $temp[$k] = [];
                }
                $temp = &$temp[$k];
            }

            $temp = $value;
        }

        return $result;
    }

    /**
     * Gets approximate memory usage of an array.
     */
    public function getArrayMemoryUsage(array $array): int
    {
        $memBefore = memory_get_usage();
        $copy = $this->deepCopyArray($array);
        $memAfter = memory_get_usage();
        unset($copy);

        return $memAfter - $memBefore;
    }

    /**
     * Copies an array and frees the original (for large arrays).
     */
    public function copyAndFree(array &$array): array
    {
        $copy = $this->deepCopyArray($array);
        unset($array);
        $array = [];
        return $copy;
    }

    /**
     * Checks if an array is associative.
     */
    public function isAssociativeArray(array $array): bool
    {
        if (empty($array)) {
            return false;
        }

        return array_keys($array) !== range(0, count($array) - 1);
    }

    /**
     * Deep copies only associative arrays (preserves numeric arrays as references).
     */
    public function deepCopyAssociativeOnly(array $array): array
    {
        if (!$this->isAssociativeArray($array)) {
            return $array;
        }

        $copy = [];

        foreach ($array as $key => $value) {
            if (is_array($value)) {
                $copy[$key] = $this->deepCopyAssociativeOnly($value);
            } elseif (is_object($value)) {
                $copy[$key] = clone $value;
            } else {
                $copy[$key] = $value;
            }
        }

        return $copy;
    }

    /**
     * Counts total elements in a nested array.
     */
    public function countDeep(array $array): int
    {
        $count = 0;

        foreach ($array as $value) {
            $count++;
            if (is_array($value)) {
                $count += $this->countDeep($value);
            }
        }

        return $count;
    }
}
