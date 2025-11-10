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
}
