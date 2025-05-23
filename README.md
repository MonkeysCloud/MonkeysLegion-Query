# MonkeysLegion Query Component

A **lightweight Query Builder & Micro-ORM** for MonkeysLegion, built on top of your existing `Connection` (PDO). Provides:

* **Fluent Query Builder**: select, insert, update, delete, joins, where, grouping, ordering, limit, offset
* **Safe parameter binding**: automatic placeholder naming to prevent SQL injection
* **Chainable API**: build complex queries in a single expression
* **EntityRepository**: base class with common CRUD and query methods
* **Zero external dependencies** beyond PDO

---

## 📦 Installation

Add the package to your project (or include locally):

```bash
composer require monkeyscloud/monkeyslegion-query:^1.0@dev
composer dump-autoload
```

Ensure your `composer.json` includes the PSR-4 autoload for:

```jsonc
"autoload": {
  "psr-4": {
    "MonkeysLegion\\Query\\": "src/Query/",
    "MonkeysLegion\\Repository\\": "src/Repository/"
  }
}
```

---

## 📂 Directory Structure

```
src/
├── Query/
│   └── QueryBuilder.php
└── Repository/
    └── EntityRepository.php
```

---

## 🔧 QueryBuilder Usage

```php
use MonkeysLegion\Database\Connection;
use MonkeysLegion\Query\QueryBuilder;

// get Connection from DI container
/** @var Connection $conn */
$conn = /* ... */;
\$qb   = new QueryBuilder(\$conn);

// SELECT with WHERE, JOIN, ORDER, LIMIT
\$users = \$qb
    ->select(['u.id', 'u.name', 'p.title'])
    ->from('users', 'u')
    ->join('posts', 'p', 'p.user_id', '=', 'u.id')
    ->where('u.status', '=', 'active')
    ->orderBy('u.created_at', 'DESC')
    ->limit(10)
    ->offset(20)
    ->fetchAll(App\Entity\User::class);

// INSERT
\$newId = \$qb->insert('users', [
    'name'   => 'Alice',
    'email'  => 'alice@example.com',
    'status' => 'active'
]);

// UPDATE
\$qb->update('users', ['name' => 'Alice Smith'])
   ->where('id', '=', \$newId)
   ->execute();

// DELETE
\$qb->delete('users')
   ->where('id', '=', \$newId)
   ->execute();
```

---

## 📚 EntityRepository Usage

Extend the base repository for each entity:

```php
namespace App\Repository;

use MonkeysLegion\Repository\EntityRepository;

class UserRepository extends EntityRepository
{
    protected string \$table = 'users';
    protected string \$entityClass = App\Entity\User::class;
}
```

Register in your DI container:

```php
use MonkeysLegion\Query\QueryBuilder;
use App\Repository\UserRepository;

QueryBuilder::class      => fn($c) => new QueryBuilder(\$c->get(Connection::class)),
UserRepository::class   => fn($c) => new UserRepository(\$c->get(QueryBuilder::class)),
```

Common methods:

```php
\$repo = \$container->get(UserRepository::class);

// find all active users
\$active = \$repo->findAll(['status' => 'active']);

// find by primary key
\$user = \$repo->find(42);

// find by criteria with sorting and pagination
\$recent = \$repo->findBy(
    ['status'=>'active'],
    ['created_at'=>'DESC'],
    limit: 5,
    offset: 0
);

// count users
\$count = \$repo->count(['status'=>'active']);

// save new user
\$u = new App\Entity\User();
\$u->name = 'Bob';\$u->email = 'bob@example.com';
\$id = \$repo->save(\$u);

// update existing
\$u->id = \$id;
\$u->name = 'Bobby';
\$repo->save(\$u);

// delete
\$repo->delete(\$id);
```

---

## ⚙️ Extending

* **Custom queries**: use `custom(\$sql, \$params)` to run raw SQL
* **Transactions**: use `$conn->pdo()->beginTransaction()` etc.
* **Relations**: add methods in your repository to eager load related data

---

## 📄 License

MIT © 2025 MonkeysCloud
