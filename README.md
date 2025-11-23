# MonkeysLegion Query Builder

A **powerful, fluent Query Builder & Micro-ORM** for PHP 8.4+, designed for the MonkeysLegion framework. Built on PDO with zero external dependencies, providing a clean, expressive API for database operations.

[![PHP Version](https://img.shields.io/badge/PHP-8.4%2B-blue.svg)](https://www.php.net/)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

## ✨ Features

- 🔗 **Fluent Query Builder** - Chainable, expressive API
- 🛡️ **SQL Injection Protection** - Automatic parameter binding
- 🔄 **Transaction Support** - Full ACID compliance with savepoints
- 🎯 **Multiple Database Support** - MySQL, PostgreSQL, SQLite
- 📊 **Advanced Queries** - Joins, subqueries, unions, CTEs
- 🏗️ **Repository Pattern** - Built-in entity repository support
- ⚡ **Performance Optimized** - Chunking, streaming, pagination
- 🎨 **Clean Code** - PSR-12 compliant, fully typed

---

## 📦 Installation

```bash
composer require monkeyscloud/monkeyslegion-query
```

Or add to your `composer.json`:

```json
{
    "require": {
        "monkeyscloud/monkeyslegion-query": "^1.0"
    },
    "autoload": {
        "psr-4": {
            "MonkeysLegion\\Query\\": "src/Query/",
            "MonkeysLegion\\Repository\\": "src/Repository/"
        }
    }
}
```

---

## 🚀 Quick Start

```php
use MonkeysLegion\Database\Connection;
use MonkeysLegion\Query\QueryBuilder;

// Initialize connection
$conn = new Connection([
    'driver' => 'mysql',
    'host' => 'localhost',
    'database' => 'mydb',
    'username' => 'root',
    'password' => 'secret',
    'charset' => 'utf8mb4'
]);

// Create query builder
$qb = new QueryBuilder($conn);

// Simple query
$users = $qb->from('users')
    ->where('status', '=', 'active')
    ->orderBy('created_at', 'DESC')
    ->limit(10)
    ->fetchAll();

// With joins
$posts = $qb->from('posts', 'p')
    ->leftJoin('users', 'u', 'u.id', '=', 'p.user_id')
    ->leftJoin('categories', 'c', 'c.id', '=', 'p.category_id')
    ->select(['p.*', 'u.name as author', 'c.name as category'])
    ->where('p.published', '=', true)
    ->fetchAll();
```

---

## 📚 Table of Contents

- [Select Operations](#select-operations)
- [Where Clauses](#where-clauses)
- [Joins](#joins)
- [Grouping & Ordering](#grouping--ordering)
- [Aggregate Functions](#aggregate-functions)
- [Insert, Update, Delete](#insert-update-delete)
- [Fetch Operations](#fetch-operations)
- [Transactions](#transactions)
- [Advanced Features](#advanced-features)
- [Repository Pattern](#repository-pattern)

---

## 🔍 Select Operations

### Basic SELECT

```php
// Select all columns
$users = $qb->from('users')->fetchAll();

// Select specific columns
$users = $qb->from('users')
    ->select(['id', 'name', 'email'])
    ->fetchAll();

// Select with alias
$users = $qb->from('users')
    ->selectAs('created_at', 'registered_date')
    ->fetchAll();

// Add columns to existing SELECT
$qb->select(['id', 'name'])
   ->addSelect(['email', 'phone']);
```

### SELECT with Expressions

```php
// Raw expressions
$qb->selectRaw('COUNT(*) as total, DATE(created_at) as date');

// Aggregate shortcuts
$qb->from('orders')
   ->selectSum('amount', 'total')
   ->selectAvg('quantity', 'avg_qty')
   ->selectMax('price', 'max_price');

// CASE statements
$qb->selectCase([
    'status = "active"' => '"Active"',
    'status = "pending"' => '"Pending"'
], '"Unknown"', 'status_label');

// CONCAT
$qb->selectConcat(['first_name', 'last_name'], 'full_name', ' ');

// JSON extraction (MySQL 5.7+)
$qb->selectJson('settings', '$.theme', 'user_theme');
```

### Subqueries in SELECT

```php
// Using callback
$qb->from('users', 'u')
   ->selectSubQuery(function($sub) {
       $sub->from('orders')
           ->selectRaw('COUNT(*)')
           ->whereRaw('orders.user_id = u.id');
   }, 'order_count');

// Raw subquery
$qb->selectSub('SELECT COUNT(*) FROM orders WHERE user_id = users.id', 'order_count');
```

### DISTINCT

```php
// Regular DISTINCT
$qb->from('users')->distinct()->select(['country']);

// DISTINCT ON (PostgreSQL)
$qb->from('events')->distinctOn(['user_id'])->orderBy('created_at', 'DESC');
```

---

## 🎯 Where Clauses

### Basic WHERE

```php
// Simple where
$qb->where('status', '=', 'active');
$qb->where('age', '>', 18);

// Multiple conditions (AND)
$qb->where('status', '=', 'active')
   ->where('verified', '=', true);

// OR conditions
$qb->where('role', '=', 'admin')
   ->orWhere('role', '=', 'moderator');

// AND/OR combined
$qb->where('status', '=', 'active')
   ->andWhere('age', '>=', 18)
   ->orWhere('role', '=', 'admin');
```

### Advanced WHERE

```php
// WHERE IN
$qb->whereIn('id', [1, 2, 3, 4, 5]);
$qb->whereNotIn('status', ['deleted', 'banned']);

// WHERE BETWEEN
$qb->whereBetween('age', 18, 65);
$qb->whereNotBetween('price', 100, 200);

// WHERE NULL
$qb->whereNull('deleted_at');
$qb->whereNotNull('verified_at');

// WHERE LIKE
$qb->whereLike('email', '%@gmail.com');
$qb->whereNotLike('name', '%test%');

// Column comparisons
$qb->whereColumn('updated_at', '>', 'created_at');

// WHERE EXISTS
$qb->whereExists('SELECT 1 FROM orders WHERE orders.user_id = users.id');
```

### Grouped WHERE

```php
// WHERE groups with AND
$qb->where('status', '=', 'active')
   ->whereGroup(function($q) {
       $q->where('role', '=', 'admin')
         ->orWhere('role', '=', 'moderator');
   });
// Produces: WHERE status = 'active' AND (role = 'admin' OR role = 'moderator')

// OR WHERE groups
$qb->where('age', '>=', 18)
   ->orWhereGroup(function($q) {
       $q->where('parent_consent', '=', true)
         ->where('guardian_id', '!=', null);
   });
```

### Date/Time WHERE

```php
// WHERE DATE
$qb->whereDate('created_at', '=', '2024-01-01');

// WHERE YEAR/MONTH/DAY
$qb->whereYear('created_at', '=', 2024);
$qb->whereMonth('created_at', '=', 1);
$qb->whereDay('created_at', '=', 15);
```

### JSON WHERE (MySQL 5.7+)

```php
// JSON contains
$qb->whereJsonContains('meta', '$.tags', 'php');

// JSON extract
$qb->whereJsonExtract('settings', '$.theme', '=', 'dark');

// JSON length
$qb->whereJsonLength('tags', '>', 3);
```

### Raw WHERE

```php
$qb->whereRaw('YEAR(created_at) = ?', [2024]);
$qb->orWhereRaw('status IN (?, ?)', ['active', 'verified']);
```

---

## 🔗 Joins

### Basic Joins

```php
// INNER JOIN
$qb->from('posts', 'p')
   ->innerJoin('users', 'u', 'u.id', '=', 'p.user_id');

// LEFT JOIN
$qb->from('users', 'u')
   ->leftJoin('profiles', 'p', 'p.user_id', '=', 'u.id');

// RIGHT JOIN
$qb->rightJoin('orders', 'o', 'o.user_id', '=', 'u.id');

// CROSS JOIN
$qb->crossJoin('settings', 's');
```

### Multiple Conditions

```php
// Using callback
$qb->from('orders', 'o')
   ->leftJoinOn('items', 'i', function($join) {
       $join->on('i.order_id', '=', 'o.id')
            ->andOn('i.deleted_at', 'IS', 'NULL')
            ->where('i.quantity', '>', 0, $this);
   });
```

### Subquery Joins

```php
// Join to subquery
$qb->from('users', 'u')
   ->leftJoinSubQuery(function($sub) {
       $sub->from('orders')
           ->select(['user_id', 'COUNT(*) as order_count'])
           ->groupBy('user_id');
   }, 'oc', 'oc.user_id', '=', 'u.id');
```

### USING Joins

```php
// When column names match
$qb->from('posts', 'p')
   ->leftJoinUsing('categories', 'c', 'category_id');
```

### Self Joins

```php
// Join table to itself
$qb->from('categories', 'c')
   ->leftSelfJoin('parent', 'parent.id', '=', 'c.parent_id');
```

### Lateral Joins (PostgreSQL)

```php
$qb->from('users', 'u')
   ->leftJoinLateral(
       'SELECT * FROM posts WHERE user_id = u.id ORDER BY created_at DESC LIMIT 3',
       'recent_posts'
   );
```

---

## 📊 Grouping & Ordering

### GROUP BY

```php
$qb->from('orders')
   ->select(['user_id', 'COUNT(*) as order_count'])
   ->groupBy('user_id');

// Multiple columns
$qb->groupBy('year', 'month', 'day');
```

### HAVING

```php
$qb->from('orders')
   ->select(['user_id', 'COUNT(*) as total'])
   ->groupBy('user_id')
   ->having('COUNT(*)', '>', 5);

// Raw HAVING
$qb->havingRaw('SUM(amount) > ?', [1000]);
```

### ORDER BY

```php
// Single column
$qb->orderBy('created_at', 'DESC');

// Multiple columns
$qb->orderBy('status', 'ASC')
   ->orderBy('priority', 'DESC')
   ->orderBy('created_at', 'DESC');

// Raw ORDER BY
$qb->orderByRaw('FIELD(status, "urgent", "high", "normal", "low")');
$qb->orderByRaw('RAND()'); // Random order
```

### LIMIT & OFFSET

```php
$qb->limit(10)->offset(20); // Skip 20, take 10
$qb->limit(5); // First 5 rows
```

---

## 📈 Aggregate Functions

### Basic Aggregates

```php
// COUNT
$total = $qb->from('users')->count();
$active = $qb->from('users')->where('status', '=', 'active')->count();

// SUM
$revenue = $qb->from('orders')->sum('amount');

// AVG
$avgPrice = $qb->from('products')->avg('price');

// MIN/MAX
$minPrice = $qb->from('products')->min('price');
$maxPrice = $qb->from('products')->max('price');
```

### Distinct Aggregates

```php
$uniqueCountries = $qb->from('users')->countDistinct('country');
$uniqueRevenue = $qb->from('orders')->sumDistinct('amount');
```

### Statistical Functions

```php
// Standard deviation
$stdDev = $qb->from('sales')->stdDev('amount');
$stdDevPop = $qb->from('sales')->stdDevPop('amount');

// Variance
$variance = $qb->from('sales')->variance('amount');
$varPop = $qb->from('sales')->varPop('amount');
```

### Conditional Aggregates

```php
// Count with condition
$activeCount = $qb->from('users')->countWhere('status', '=', 'active');

// Sum with condition
$activeRevenue = $qb->from('orders')->sumWhere('amount', 'status', '=', 'paid');
```

### Existence Checks

```php
$exists = $qb->from('users')->where('email', '=', 'admin@example.com')->exists();
$doesntExist = $qb->from('users')->where('id', '=', 999)->doesntExist();
```

### GROUP_CONCAT (MySQL)

```php
$tags = $qb->from('post_tags')
    ->where('post_id', '=', 1)
    ->groupConcat('tag_name', ', ', true); // Distinct, comma-separated
```

---

## ✏️ Insert, Update, Delete

### INSERT

```php
// Single insert
$userId = $qb->insert('users', [
    'name' => 'John Doe',
    'email' => 'john@example.com',
    'status' => 'active'
]);

// Batch insert
$count = $qb->insertBatch('users', [
    ['name' => 'Alice', 'email' => 'alice@example.com'],
    ['name' => 'Bob', 'email' => 'bob@example.com'],
    ['name' => 'Carol', 'email' => 'carol@example.com']
]);
```

### UPDATE

```php
// Update with WHERE
$affected = $qb->update('users', [
        'status' => 'inactive',
        'updated_at' => date('Y-m-d H:i:s')
    ])
    ->where('last_login', '<', date('Y-m-d', strtotime('-1 year')))
    ->execute();

// Update all
$affected = $qb->update('users', ['verified' => true])->execute();
```

### DELETE

```php
// Delete with WHERE
$affected = $qb->delete('users')
    ->where('status', '=', 'deleted')
    ->where('deleted_at', '<', date('Y-m-d', strtotime('-30 days')))
    ->execute();

// Delete all (dangerous!)
$affected = $qb->delete('users')->execute();
```

### Upsert / Insert or Update

```php
// Insert or update based on duplicate key (MySQL)
$qb->custom(
    "INSERT INTO users (id, name, email) VALUES (?, ?, ?) 
     ON DUPLICATE KEY UPDATE name = VALUES(name), email = VALUES(email)",
    [1, 'John', 'john@example.com']
)->execute();
```

---

## 📤 Fetch Operations

### Basic Fetching

```php
// Fetch all as arrays
$users = $qb->from('users')->fetchAll();

// Fetch all as objects
$users = $qb->from('users')->fetchAll(User::class);

// Fetch first row
$user = $qb->from('users')->where('id', '=', 1)->first();

// Fetch first or fail
$user = $qb->from('users')->where('id', '=', 1)->firstOrFail();

// Fetch single value
$name = $qb->from('users')->where('id', '=', 1)->value('name');

// Fetch column as array
$emails = $qb->from('users')->pluck('email');

// Fetch key-value pairs
$idNameMap = $qb->from('users')->pluck('name', 'id');
// Result: [1 => 'John', 2 => 'Jane', ...]
```

### Find Operations

```php
// Find by ID
$user = $qb->from('users')->find(1);

// Find or fail
$user = $qb->from('users')->findOrFail(1);

// Find many by IDs
$users = $qb->from('users')->findMany([1, 2, 3, 4, 5]);
```

### Advanced Fetching

```php
// Fetch as specific type
$users = $qb->from('users')->fetchAllAssoc();
$users = $qb->from('users')->fetchAllObjects();

// Fetch indexed by key
$usersById = $qb->from('users')->fetchIndexed('id');
// Result: [1 => [...], 2 => [...], ...]

// Fetch grouped by key
$usersByCountry = $qb->from('users')->fetchGrouped('country');
// Result: ['US' => [[...], [...]], 'UK' => [[...]], ...]
```

### Chunking & Streaming

```php
// Process in chunks (memory efficient)
$qb->from('users')->chunk(100, function($users, $page) {
    foreach ($users as $user) {
        // Process each user
    }
    // Return false to stop
});

// Stream with cursor (generator)
foreach ($qb->from('users')->cursor() as $user) {
    // Process one at a time
}

// Lazy loading (chunks via generator)
foreach ($qb->from('users')->lazy(1000) as $user) {
    // Memory efficient iteration
}

// Process each row
$qb->from('users')->each(function($user, $index) {
    echo "Processing user {$index}: {$user['name']}\n";
});
```

### Pagination

```php
// Full pagination (with total count)
$result = $qb->from('posts')
    ->where('published', '=', true)
    ->paginate(page: 2, perPage: 15);

// Result structure:
// [
//     'data' => [...],
//     'total' => 150,
//     'page' => 2,
//     'perPage' => 15,
//     'lastPage' => 10,
//     'from' => 16,
//     'to' => 30
// ]

// Simple pagination (no count, faster)
$result = $qb->from('posts')->simplePaginate(1, 20);
// Result: ['data' => [...], 'hasMore' => true, 'page' => 1, 'perPage' => 20]
```

### Transformations

```php
// Map results
$names = $qb->from('users')->map(fn($user) => strtoupper($user['name']));

// Filter results
$adults = $qb->from('users')->filter(fn($user) => $user['age'] >= 18);

// Reduce results
$totalAge = $qb->from('users')->reduce(fn($carry, $user) => $carry + $user['age'], 0);
```

---

## 💾 Transactions

### Basic Transactions

```php
// Manual control
$qb->beginTransaction();
try {
    $qb->insert('users', ['name' => 'Alice']);
    $qb->insert('profiles', ['user_id' => 1]);
    $qb->commit();
} catch (\Exception $e) {
    $qb->rollback();
    throw $e;
}

// Using callback
$result = $qb->transaction(function($qb) {
    $userId = $qb->insert('users', ['name' => 'Bob']);
    $qb->insert('profiles', ['user_id' => $userId]);
    return $userId;
});
```

### Nested Transactions (Savepoints)

```php
$qb->beginTransactionNested(); // Level 1
try {
    $qb->insert('users', ['name' => 'Alice']);
    
    $qb->beginTransactionNested(); // Level 2 (creates savepoint)
    try {
        $qb->insert('profiles', ['user_id' => 1]);
        $qb->commitNested(); // Releases savepoint
    } catch (\Exception $e) {
        $qb->rollbackNested(); // Rollback to savepoint
    }
    
    $qb->commitNested();
} catch (\Exception $e) {
    $qb->rollbackNested();
}
```

### Transaction with Retry

```php
// Automatically retry on deadlocks
$result = $qb->transactionWithRetry(function($qb) {
    $qb->update('accounts', ['balance' => 100])
       ->where('id', '=', 1)
       ->execute();
}, attempts: 3, sleep: 100);
```

### Isolation Levels

```php
// Set isolation level
$qb->setTransactionIsolation('SERIALIZABLE');
$qb->beginTransaction();

// Shortcuts
$qb->readUncommitted()->beginTransaction();
$qb->readCommitted()->beginTransaction();
$qb->repeatableRead()->beginTransaction();
$qb->serializable()->beginTransaction();
```

### Transaction Callbacks

```php
// After commit callback
$qb->transaction(function($qb) {
    $userId = $qb->insert('users', ['name' => 'Alice']);
    
    $qb->afterCommit(function() use ($userId) {
        // Send welcome email
        Mail::send('welcome', $userId);
    });
});

// After rollback callback
$qb->afterRollback(function() {
    Log::error('Transaction failed');
});
```

### Read-Only Transactions

```php
// Optimize read-only queries
$qb->beginReadOnlyTransaction();
$users = $qb->from('users')->fetchAll();
$qb->commit();
```

### Advisory Locks

```php
// Acquire lock
if ($qb->getLock('user_processing_123', timeout: 10)) {
    // Do work
    $qb->releaseLock('user_processing_123');
}

// Execute with lock
$qb->withLock('invoice_generation', function($qb) {
    // Generate invoice
}, timeout: 30);
```

---

## 🔧 Advanced Features

### Subqueries

```php
// FROM subquery
$qb->fromSubQuery(function($sub) {
    $sub->from('orders')
        ->select(['user_id', 'COUNT(*) as order_count'])
        ->groupBy('user_id');
}, 'user_orders')
->where('order_count', '>', 10);

// WHERE subquery
$qb->from('users')
   ->whereExists(
       'SELECT 1 FROM orders WHERE orders.user_id = users.id AND orders.status = ?',
       ['completed']
   );
```

### UNION

```php
$qb->from('customers')
   ->select(['id', 'name', '"customer" as type'])
   ->union(
       'SELECT id, name, "supplier" as type FROM suppliers',
       [],
       all: false
   );
```

### Raw Queries

```php
// Execute raw query
$results = $qb->raw('SELECT * FROM users WHERE created_at > ?', ['2024-01-01']);

// Raw query with single result
$user = $qb->rawOne('SELECT * FROM users WHERE id = ?', [1]);
```

### Custom SQL

```php
// Execute custom SQL with query builder features
$qb->custom('SELECT * FROM users')
   ->where('status', '=', 'active')
   ->orderBy('created_at', 'DESC')
   ->fetchAll();
```

### Query Introspection

```php
// Get generated SQL
$sql = $qb->from('users')->where('id', '=', 1)->toSql();

// Get bound parameters
$params = $qb->getParams();

// Debug query
$qb->from('users')->where('id', '=', 1)->dump(); // Prints debug info
$qb->from('users')->where('id', '=', 1)->dd();   // Dump and die
```

### Conditional Building

```php
// Conditional clauses
$qb->from('users')
   ->when($isAdmin, fn($q) => $q->select('*'))
   ->unless($isAdmin, fn($q) => $q->select(['id', 'name']))
   ->where('active', '=', true);

// Conditional joins
$qb->from('posts')
   ->leftJoinWhen($includeAuthor, 'users', 'u', 'u.id', '=', 'posts.user_id');
```

### Query Duplication

```php
// Clone query for reuse
$baseQuery = $qb->from('users')->where('status', '=', 'active');

$admins = $baseQuery->clone()->where('role', '=', 'admin')->fetchAll();
$users = $baseQuery->clone()->where('role', '=', 'user')->fetchAll();
```

### Macros (Custom Methods)

```php
// Register custom macro
QueryBuilder::macro('whereDateRange', function($column, $start, $end) {
    return $this->whereBetween($column, $start, $end);
});

// Use macro
$qb->from('orders')->whereDateRange('created_at', '2024-01-01', '2024-12-31');
```

---

## 🏗️ Repository Pattern

### Creating a Repository

```php
namespace App\Repository;

use MonkeysLegion\Repository\EntityRepository;
use App\Entity\User;

class UserRepository extends EntityRepository
{
    protected string $table = 'users';
    protected string $entityClass = User::class;
    
    // Custom methods
    public function findActive(): array
    {
        return $this->findBy(['status' => 'active']);
    }
    
    public function findByEmail(string $email): ?User
    {
        return $this->findOneBy(['email' => $email]);
    }
    
    public function getAdmins(): array
    {
        return $this->qb
            ->from($this->table)
            ->where('role', '=', 'admin')
            ->orderBy('name', 'ASC')
            ->fetchAll($this->entityClass);
    }
}
```

### Built-in Repository Methods

```php
$userRepo = new UserRepository($qb);

// Find all
$users = $userRepo->findAll();

// Find by ID
$user = $userRepo->find(1);

// Find by criteria
$users = $userRepo->findBy(
    ['status' => 'active', 'verified' => true],
    ['created_at' => 'DESC'],
    limit: 10,
    offset: 0
);

// Find one by criteria
$user = $userRepo->findOneBy(['email' => 'admin@example.com']);

// Count
$total = $userRepo->count();
$active = $userRepo->count(['status' => 'active']);

// Save (insert or update)
$userId = $userRepo->save($user);

// Delete
$affected = $userRepo->delete(1);
```

### Repository Factory

```php
namespace MonkeysLegion\Repository;

use MonkeysLegion\Query\QueryBuilder;

class RepositoryFactory
{
    public function __construct(private QueryBuilder $qb) {}
    
    /**
     * @template T of EntityRepository
     * @param class-string<T> $repoClass
     * @return T
     */
    public function create(string $repoClass): object
    {
        return new $repoClass($this->qb);
    }
}

// Usage
$factory = new RepositoryFactory($qb);
$userRepo = $factory->create(UserRepository::class);
```

### Dependency Injection Setup

```php
// In your DI container config
use MonkeysLegion\Database\Connection;
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Repository\RepositoryFactory;

return [
    Connection::class => fn() => new Connection(require __DIR__.'/database.php'),
    
    QueryBuilder::class => fn($c) => new QueryBuilder(
        $c->get(Connection::class)
    ),
    
    RepositoryFactory::class => fn($c) => new RepositoryFactory(
        $c->get(QueryBuilder::class)
    ),
    
    // Individual repositories
    UserRepository::class => fn($c) => new UserRepository(
        $c->get(QueryBuilder::class)
    ),
];
```

---

## 🎨 Best Practices

### 1. Always Use Parameter Binding

```php
// ❌ BAD - SQL Injection risk
$qb->whereRaw("email = '{$email}'");

// ✅ GOOD - Safe parameter binding
$qb->where('email', '=', $email);
$qb->whereRaw('email = ?', [$email]);
```

### 2. Use Transactions for Related Operations

```php
// ✅ GOOD - Atomic operations
$qb->transaction(function($qb) use ($orderData, $items) {
    $orderId = $qb->insert('orders', $orderData);
    
    foreach ($items as $item) {
        $item['order_id'] = $orderId;
        $qb->insert('order_items', $item);
    }
    
    return $orderId;
});
```

### 3. Use Repositories for Business Logic

```php
// ✅ GOOD - Encapsulated logic
class OrderRepository extends EntityRepository
{
    public function createOrder(array $orderData, array $items): int
    {
        return $this->qb->transaction(function($qb) use ($orderData, $items) {
            $orderId = $qb->insert('orders', $orderData);
            
            foreach ($items as $item) {
                $item['order_id'] = $orderId;
                $qb->insert('order_items', $item);
            }
            
            return $orderId;
        });
    }
}
```

### 4. Use Chunking for Large Datasets

```php
// ✅ GOOD - Memory efficient
$qb->from('users')->chunk(1000, function($users) {
    foreach ($users as $user) {
        // Process user
    }
});

// ❌ BAD - Loads all into memory
$users = $qb->from('users')->fetchAll();
```

### 5. Clone Queries for Reuse

```php
// ✅ GOOD - Reusable base query
$activeUsers = $qb->from('users')->where('status', '=', 'active');

$admins = $activeUsers->clone()->where('role', '=', 'admin')->fetchAll();
$regular = $activeUsers->clone()->where('role', '=', 'user')->fetchAll();
```

---

## 🔒 Security

### SQL Injection Protection

MonkeysLegion Query Builder automatically protects against SQL injection through:

1. **Automatic parameter binding** - All values are bound as PDO parameters
2. **Unique placeholder generation** - Prevents parameter collision
3. **Identifier quoting** - Table and column names are properly escaped

```php
// All of these are safe
$qb->where('email', '=', $userInput);
$qb->whereIn('id', $arrayFromUser);
$qb->whereLike('name', $searchTerm);
```

### Safe Raw Queries

When using raw SQL, always use parameter binding:

```php
// ✅ SAFE
$qb->whereRaw('YEAR(created_at) = ?', [2024]);
$qb->selectRaw('COUNT(CASE WHEN status = ? THEN 1 END) as count', ['active']);

// ❌ UNSAFE
$qb->whereRaw("YEAR(created_at) = {$year}"); // Don't do this!
```

---

## ⚡ Performance Tips

### 1. Use Indexes

```php
// Ensure WHERE, JOIN, and ORDER BY columns are indexed
$qb->from('users')
   ->where('email', '=', $email)  // email should be indexed
   ->orderBy('created_at', 'DESC'); // created_at should be indexed
```

### 2. Select Only Needed Columns

```php
// ✅ GOOD
$qb->select(['id', 'name', 'email']);

// ❌ BAD (if you don't need all columns)
$qb->select('*');
```

### 3. Use EXISTS Instead of COUNT

```php
// ✅ FASTER for existence checks
$exists = $qb->from('users')->where('email', '=', $email)->exists();

// ❌ SLOWER
$exists = $qb->from('users')->where('email', '=', $email)->count() > 0;
```

### 4. Eager Load Relationships

```php
// ✅ GOOD - Single query with joins
$posts = $qb->from('posts', 'p')
    ->leftJoin('users', 'u', 'u.id', '=', 'p.user_id')
    ->select(['p.*', 'u.name as author_name'])
    ->fetchAll();

// ❌ BAD - N+1 query problem
$posts = $qb->from('posts')->fetchAll();
foreach ($posts as $post) {
    $post->author = $qb->from('users')->find($post->user_id); // N queries!
}
```

### 5. Use Pagination for Large Results

```php
// ✅ GOOD
$result = $qb->from('posts')->paginate(1, 20);

// ❌ BAD - Loads all rows
$all = $qb->from('posts')->fetchAll();
```

---

## 🐛 Debugging

### Query Debugging

```php
// Print query and continue
$qb->from('users')->where('id', '=', 1)->dump();

// Print query and exit
$qb->from('users')->where('id', '=', 1)->dd();

// Log query
$qb->from('users')->where('id', '=', 1)->log('[UserQuery]');

// Get SQL and params
$sql = $qb->toSql();
$params = $qb->getParams();
```

### Enable PDO Error Mode

```php
$conn = new Connection([
    'driver' => 'mysql',
    'host' => 'localhost',
    'database' => 'mydb',
    'username' => 'root',
    'password' => 'secret',
    'options' => [
        PDO::ATTR_ERRMODE => PDO::ERRMODE_EXCEPTION,
        PDO::ATTR_DEFAULT_FETCH_MODE => PDO::FETCH_ASSOC
    ]
]);
```

---

## 🧪 Testing

### Example PHPUnit Test

```php
use PHPUnit\Framework\TestCase;
use MonkeysLegion\Query\QueryBuilder;

class UserRepositoryTest extends TestCase
{
    private QueryBuilder $qb;
    
    protected function setUp(): void
    {
        $this->qb = new QueryBuilder($this->createTestConnection());
        $this->qb->beginTransaction();
    }
    
    protected function tearDown(): void
    {
        $this->qb->rollback();
    }
    
    public function testFindUser(): void
    {
        $userId = $this->qb->insert('users', [
            'name' => 'Test User',
            'email' => 'test@example.com'
        ]);
        
        $user = $this->qb->from('users')->find($userId);
        
        $this->assertEquals('Test User', $user['name']);
        $this->assertEquals('test@example.com', $user['email']);
    }
}
```

---

## 📖 API Reference

### Complete Method List

#### Select Operations
- `select()`, `addSelect()`, `selectAs()`, `selectRaw()`
- `selectSum()`, `selectAvg()`, `selectMin()`, `selectMax()`, `selectCount()`
- `selectConcat()`, `selectCoalesce()`, `selectCase()`, `selectJson()`
- `distinct()`, `distinctOn()`

#### Where Clauses
- `where()`, `andWhere()`, `orWhere()`, `whereRaw()`
- `whereIn()`, `whereNotIn()`, `orWhereIn()`, `orWhereNotIn()`
- `whereBetween()`, `whereNotBetween()`, `orWhereBetween()`
- `whereNull()`, `whereNotNull()`, `orWhereNull()`, `orWhereNotNull()`
- `whereLike()`, `whereNotLike()`, `orWhereLike()`
- `whereExists()`, `whereNotExists()`, `orWhereExists()`
- `whereColumn()`, `orWhereColumn()`
- `whereDate()`, `whereYear()`, `whereMonth()`, `whereDay()`, `whereTime()`
- `whereJsonContains()`, `whereJsonExtract()`, `whereJsonLength()`
- `whereGroup()`, `orWhereGroup()`, `andWhereGroup()`

#### Joins
- `join()`, `innerJoin()`, `leftJoin()`, `rightJoin()`, `crossJoin()`
- `fullOuterJoin()`, `leftOuterJoin()`, `rightOuterJoin()`
- `joinOn()`, `innerJoinOn()`, `leftJoinOn()`, `rightJoinOn()`
- `joinSub()`, `leftJoinSub()`, `rightJoinSub()`, `joinSubQuery()`
- `joinUsing()`, `innerJoinUsing()`, `leftJoinUsing()`, `rightJoinUsing()`
- `naturalJoin()`, `naturalLeftJoin()`, `naturalRightJoin()`
- `joinLateral()`, `leftJoinLateral()`, `innerJoinLateral()`
- `selfJoin()`, `leftSelfJoin()`

#### Grouping & Ordering
- `groupBy()`, `having()`, `havingRaw()`
- `orderBy()`, `orderByRaw()`
- `limit()`, `offset()`

#### Aggregates
- `count()`, `countDistinct()`, `countWhere()`
- `sum()`, `sumDistinct()`, `sumWhere()`
- `avg()`, `avgDistinct()`
- `min()`, `max()`
- `stdDev()`, `stdDevPop()`, `stdDevSamp()`
- `variance()`, `varPop()`, `varSamp()`
- `groupConcat()`
- `exists()`, `doesntExist()`

#### DML Operations
- `insert()`, `insertBatch()`
- `update()`, `delete()`
- `execute()`, `executeRaw()`

#### Fetch Operations
- `fetchAll()`, `fetchAllAssoc()`, `fetchAllObjects()`
- `fetch()`, `first()`, `firstAs()`, `firstOrFail()`
- `find()`, `findOrFail()`, `findMany()`
- `value()`, `pluck()`, `fetchPairs()`, `fetchIndexed()`, `fetchGrouped()`
- `chunk()`, `cursor()`, `cursorAs()`, `each()`, `lazy()`
- `paginate()`, `simplePaginate()`
- `map()`, `filter()`, `reduce()`

#### Transactions
- `beginTransaction()`, `commit()`, `rollback()`
- `transaction()`, `safeTransaction()`, `transactionWithRetry()`
- `beginTransactionNested()`, `commitNested()`, `rollbackNested()`
- `savepoint()`, `rollbackToSavepoint()`, `releaseSavepoint()`
- `setTransactionIsolation()`, `readCommitted()`, `repeatableRead()`, `serializable()`
- `getLock()`, `releaseLock()`, `withLock()`

#### Utilities
- `from()`, `fromSub()`, `fromSubQuery()`
- `duplicate()`, `clone()`, `reset()`, `fresh()`
- `toSql()`, `getParams()`, `dump()`, `dd()`, `log()`
- `when()`, `unless()`, `tap()`

---

## 📝 License

MIT License - see [LICENSE](LICENSE) file for details

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

---

## 📮 Support

- **Documentation**: https://monkeyslegion.com/docs/starter
- **Issues**: https://github.com/MonkeysCloud/MonkeysLegion-Skeleton
- **Slack**: https://join.slack.com/t/monkeyslegion/shared_invite/zt-36jut3kqo-WCwOabVrVrhHBln4xhMATA

---

## 🙏 Credits

Created and maintained by [MonkeysCloud](https://github.com/monkeyscloud)

---

**Built with ❤️ by the MonkeysLegion team**

## Contributors
<table>
  <tr>
    <td>
      <a href="https://github.com/yorchperaza">
        <img src="https://github.com/yorchperaza.png" width="100px;" alt="Jorge Peraza"/><br />
        <sub><b>Jorge Peraza</b></sub>
      </a>
    </td>
    <td>
      <a href="https://github.com/Amanar-Marouane">
        <img src="https://github.com/Amanar-Marouane.png" width="100px;" alt="Amanar Marouane"/><br />
        <sub><b>Amanar Marouane</b></sub>
      </a>
    </td>
  </tr>
</table>