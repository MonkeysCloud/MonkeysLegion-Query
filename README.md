# MonkeysLegion Query Component

A **lightweight Query Builder & Micro-ORM** for MonkeysLegion, built on top of your existing `Connection` (PDO). Provides:

* **Fluent Query Builder**: select, insert, update, delete, joins, where, grouping, ordering, limit, offset
* **Safe parameter binding**: automatic placeholder naming to prevent SQL injection
* **Chainable API**: build complex queries in a single expression
* **EntityRepository**: base class with common CRUD and query methods
* **RepositoryFactory**: dynamic factory to instantiate any repository without manual DI wiring
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
    ├── EntityRepository.php
    └── RepositoryFactory.php
```

---

## 🔧 QueryBuilder Usage

```php
use MonkeysLegion\Database\Connection;
use MonkeysLegion\Query\QueryBuilder;

// get Connection from DI container
/** @var Connection $conn */
\$conn = /* ... */;
\$qb   = new QueryBuilder(\$conn);

// SELECT with WHERE, JOIN, ORDER, LIMIT, OFFSET
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

The `EntityRepository` provides:

* `findAll(array \$criteria = []): object[]`
* `find(int \$id): ?object`
* `findBy(array \$criteria, array \$orderBy = [], int|null \$limit = null, int|null \$offset = null): object[]`
* `findOneBy(array \$criteria): ?object`
* `count(array \$criteria = []): int`
* `save(object \$entity): int` (insert or update)
* `delete(int \$id): int`

---

## ⚙️ Dynamic RepositoryFactory

Instead of manual DI entries for each repository, use the `RepositoryFactory`:

```php
namespace MonkeysLegion\Repository;

use MonkeysLegion\Query\QueryBuilder;

final class RepositoryFactory
{
    public function __construct(private QueryBuilder \$qb) {}

    /**
     * @template T of EntityRepository
     * @param class-string<T> \$repoClass
     * @return T
     */
    public function create(string \$repoClass): object
    {
        if (!is_subclass_of(\$repoClass, EntityRepository::class, true)) {
            throw new \InvalidArgumentException("{\$repoClass} must extend EntityRepository");
        }
        return new \$repoClass(\$this->qb);
    }
}
```

Register it in your DI config:

```php
use MonkeysLegion\Query\QueryBuilder;
use MonkeysLegion\Repository\RepositoryFactory;

Connection::class       => fn()   => new Connection(require __DIR__.'/database.php'),
QueryBuilder::class    => fn(\$c) => new QueryBuilder(\$c->get(Connection::class)),
RepositoryFactory::class => fn(\$c) => new RepositoryFactory(\$c->get(QueryBuilder::class)),
```

---

## 🚀 Using RepositoryFactory

In your controller or service:

```php
use MonkeysLegion\Repository\RepositoryFactory;
use App\Repository\UserRepository;

final class UserController
{
    public function __construct(private RepositoryFactory \$repos) {}

    public function list(): array
    {
        /** @var UserRepository \$userRepo */
        \$userRepo = \$this->repos->create(UserRepository::class);
        return \$userRepo->findAll(['status' => 'active']);
    }
}
```

Or, if you prefer a helper on `QueryBuilder`:

```php
public function repository(string \$repoClass): EntityRepository
{
    return new \$repoClass(\$this);
}
```

Then:

```php
\$userRepo = \$qb->repository(UserRepository::class);
```

---

## ⚙️ Extending

* **Custom queries**: use `custom(\$sql, \$params)` to run raw SQL
* **Transactions**: use `\$conn->pdo()->beginTransaction()` etc.
* **Relations**: add methods in your repository to eager load related data

---

## 📄 License

MIT © 2025 MonkeysCloud
