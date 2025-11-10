<?php

declare(strict_types=1);

namespace MonkeysLegion\Query;

use MonkeysLegion\Database\Contracts\ConnectionInterface;
use MonkeysLegion\Query\Traits\AggregateOperations;
use MonkeysLegion\Query\Traits\ArrayCopy;
use MonkeysLegion\Query\Traits\DmlOperations;
use MonkeysLegion\Query\Traits\FetchOperations;
use MonkeysLegion\Query\Traits\JoinOperations;
use MonkeysLegion\Query\Traits\OrderGroupOperations;
use MonkeysLegion\Query\Traits\SelectOperations;
use MonkeysLegion\Query\Traits\TableOperations;
use MonkeysLegion\Query\Traits\TransactionOperations;
use MonkeysLegion\Query\Traits\WhereOperations;

/**
 * QueryBuilder — a fluent SQL builder supporting SELECT/INSERT/UPDATE/DELETE
 * with joins, conditions, grouping, ordering, pagination, and transactions.
 */
final class QueryBuilder extends AbstractQueryBuilder
{
    use ArrayCopy;
    use SelectOperations;
    use JoinOperations;
    use WhereOperations;
    use OrderGroupOperations;
    use DmlOperations;
    use FetchOperations;
    use AggregateOperations;
    use TransactionOperations;
    use TableOperations;

    /**
     * Constructor.
     *
     * @param ConnectionInterface $conn Database connection instance.
     */
    public function __construct(ConnectionInterface $conn)
    {
        parent::__construct($conn);
    }
}
