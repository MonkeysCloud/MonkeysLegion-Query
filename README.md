# MonkeysLegion Query — v2

[![PHP](https://img.shields.io/badge/PHP-8.4%2B-blue.svg)](https://php.net)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)
[![Tests](https://img.shields.io/badge/Tests-156%20pass-brightgreen.svg)]()

Performance-first Query Builder & Micro-ORM for the [MonkeysLegion](https://github.com/MonkeysCloud) framework.

## Architecture

```
QueryBuilder (Fluent API)
  → typed Clause VOs (zero string work)
    → QueryCompiler (stateless, cached)
      → GrammarInterface (MySQL/MariaDB, PostgreSQL, SQLite)
        → ConnectionManagerInterface (read/write routing)

EntityRepository (single class)
  → IdentityMap (same row → same object)
    → UnitOfWork (batched writes)
      → EntityHydrator (cached reflection)
```

## Key Features

| Feature | Description |
|---------|-------------|
| **Performance-first** | Structural SQL caching (xxh128), statement reuse, zero-copy bindings |
| **4 DB engines** | MySQL, MariaDB, PostgreSQL, SQLite — full grammar implementations |
| **Read/write routing** | Automatic: SELECTs → `read()`, DML → `write()` via `ConnectionManagerInterface` |
| **PHP 8.4** | Property hooks, asymmetric visibility, readonly classes, backed enums |
| **Identity map** | Same row always returns same object instance |
| **Unit of Work** | `persist()` / `remove()` / `flush()` — batched writes in one transaction |
| **Cursor pagination** | Constant-memory traversal of large datasets |
| **CTE builder** | Standard and recursive CTEs across all 4 engines |
| **Vector search** | pgvector, MySQL 9.x VEC_DISTANCE, with fallbacks |
| **#[Scope] attribute** | Global and per-query scopes on repository methods |

## Installation

```bash
composer require monkeyscloud/monkeyslegion-query:2.x-dev
```

## Quick Start

### Query Builder

```php
use MonkeysLegion\Query\Query\QueryBuilder;

// Inject ConnectionManagerInterface via DI
$qb = new QueryBuilder($connectionManager);

// SELECT
$users = $qb->from('users')
    ->select(['id', 'name', 'email'])
    ->where('status', '=', 'active')
    ->where('age', '>', 18)
    ->orderByDesc('created_at')
    ->limit(25)
    ->get();

// First row
$user = $qb->from('users')
    ->where('email', '=', 'alice@example.com')
    ->first();

// Aggregates
$count = $qb->from('orders')->where('status', '=', 'pending')->count();
$total = $qb->from('orders')->sum('amount');

// INSERT
$id = $qb->from('users')->insert([
    'name'  => 'Alice',
    'email' => 'alice@example.com',
]);

// UPDATE
$qb->from('users')
    ->where('id', '=', $id)
    ->update(['status' => 'verified']);

// DELETE
$qb->from('sessions')
    ->where('expired_at', '<', date('Y-m-d'))
    ->delete();
```

### Joins

```php
// Simple join
$qb->from('users', 'u')
    ->leftJoinOn('orders', 'u.id', '=', 'orders.user_id', 'o')
    ->select(['u.name', 'o.total'])
    ->get();

// Complex join with callback
$qb->from('users', 'u')
    ->join('orders', fn($j) => $j
        ->on('u.id', '=', 'o.user_id')
        ->where('o.status', '=', 'completed'),
        alias: 'o',
    )
    ->get();
```

### Where Clauses

```php
$qb->from('users')
    ->where('status', '=', 'active')       // standard
    ->orWhere('role', '=', 'admin')         // OR
    ->whereIn('id', [1, 2, 3])             // IN
    ->whereNotIn('status', ['banned'])     // NOT IN
    ->whereBetween('age', 18, 65)          // BETWEEN
    ->whereNull('deleted_at')              // IS NULL
    ->whereNotNull('email')                // IS NOT NULL
    ->get();
```

### Repository

```php
use MonkeysLegion\Query\Repository\EntityRepository;

class UserRepository extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = User::class;
}

// Find
$user = $repo->find(1);
$user = $repo->findOrFail(1);

// Batch loading (single WHERE IN query)
$users = $repo->findByIds([1, 2, 3]);

// Criteria-based
$admins = $repo->findBy(['role' => 'admin'], ['name' => 'ASC'], limit: 10);
$alice  = $repo->findOneBy(['email' => 'alice@example.com']);

// Unit of Work
$user = new User();
$user->name = 'Bob';
$repo->persist($user);
$repo->flush();  // INSERT in transaction

// Cursor pagination (constant memory)
$page = $repo->cursorPaginate(cursor: null, perPage: 25);
// $page = ['data' => [...], 'nextCursor' => 26, 'hasMore' => true]
```

### Common Table Expressions

```php
use MonkeysLegion\Query\Query\CteBuilder;

$cte = new CteBuilder();
$cte->add('active_users', 'SELECT * FROM users WHERE status = ?', ['active']);

// Recursive CTE (tree structures)
$cte->add(
    name: 'category_tree',
    sql: 'SELECT id, parent_id FROM categories WHERE id = ? '
       . 'UNION ALL '
       . 'SELECT c.id, c.parent_id FROM categories c '
       . 'JOIN category_tree ct ON c.parent_id = ct.id',
    bindings: [$rootId],
    recursive: true,
);
```

### Vector Search

```php
use MonkeysLegion\Query\Query\VectorSearch;

// Nearest neighbors using pgvector (PostgreSQL)
$expr = VectorSearch::distance('embedding', $queryVector, DatabaseDriver::PostgreSQL);
// Produces: embedding <-> '[1.0,2.0,3.0]'

// Cosine similarity
$expr = VectorSearch::distance('embedding', $vector, DatabaseDriver::PostgreSQL, 'cosine');
// Produces: embedding <=> '[1.0,2.0,3.0]'
```

## Performance

| Metric | v1 | v2 |
|--------|----|----|
| Test suite (156 tests) | ~120ms | **41ms** |
| Code size | 10,500 lines / 19 files | **3,600 lines / 24 files** |
| Inheritance depth | 3 classes | **1 class** |
| Schema queries at build | Per column | **Zero** |
| SQL compilation | Every execution | **Cached (xxh128)** |
| PDOStatement | New per query | **Cached per connection** |

## Database Compatibility

| Feature | MySQL 8+ | MariaDB 10.2+ | PostgreSQL | SQLite 3.35+ |
|---------|----------|---------------|------------|-------------|
| Query builder | ✅ | ✅ | ✅ | ✅ |
| Upsert | ON DUPLICATE KEY | ON DUPLICATE KEY | ON CONFLICT | ON CONFLICT |
| RETURNING | ❌ | ❌ | ✅ | ✅ |
| JSON path | ->> | ->> | ->> / #>> | json_extract |
| CTE | ✅ | ✅ | ✅ | ✅ |
| Recursive CTE | ✅ | ✅ | ✅ | ✅ |
| Vector search | VEC_DISTANCE* | ❌ | pgvector | ❌ |
| Identifier quoting | \`backtick\` | \`backtick\` | "double" | "double" |

## Requirements

- PHP 8.4+
- `monkeyscloud/monkeyslegion-database` v2
- `monkeyscloud/monkeyslegion-entity` v1+
- PDO extension

## License

MIT © [MonkeysCloud](https://github.com/MonkeysCloud)
