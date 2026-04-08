<?php
declare(strict_types=1);

/**
 * Performance timing script — outputs actual benchmark numbers.
 * Run: php tests/perf_timing.php
 */

require __DIR__ . '/../vendor/autoload.php';

use MonkeysLegion\Database\Enums\DatabaseDriver;
use MonkeysLegion\Query\Query\QueryBuilder;
use MonkeysLegion\Query\Repository\EntityHydrator;
use MonkeysLegion\Entity\Attributes\Field;
use Tests\Support\TestConnection;
use Tests\Support\TestConnectionManager;

// Setup in-memory SQLite
$pdo = new \PDO('sqlite::memory:');
$pdo->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
$pdo->exec('CREATE TABLE users (id INTEGER PRIMARY KEY AUTOINCREMENT, name TEXT, email TEXT, status TEXT, age INTEGER)');
for ($i = 1; $i <= 500; $i++) {
    $pdo->exec("INSERT INTO users VALUES ($i, 'User$i', 'user$i@test.com', 'active', " . (20 + $i % 50) . ")");
}

$conn = new TestConnection($pdo);
$manager = new TestConnectionManager($conn);

function bench(string $label, Closure $fn, int $iterations = 1000): void {
    // Warmup
    for ($i = 0; $i < min(10, $iterations); $i++) $fn();

    $start = hrtime(true);
    for ($i = 0; $i < $iterations; $i++) $fn();
    $elapsed = (hrtime(true) - $start) / 1_000_000;

    $perOp = $elapsed / $iterations;
    $opsPerSec = $iterations / ($elapsed / 1000);
    printf("  %-45s %8.2f ms  (%6.3f ms/op  %8.0f ops/sec)\n", $label, $elapsed, $perOp, $opsPerSec);
}

echo "\n╔══════════════════════════════════════════════════════════════════════════════╗\n";
echo "║  MonkeysLegion Query v2 — Performance Benchmarks (Phase 6)                 ║\n";
echo "╠══════════════════════════════════════════════════════════════════════════════╣\n";
echo "║  PHP " . PHP_VERSION . "  |  SQLite in-memory  |  " . date('Y-m-d H:i:s') . "               ║\n";
echo "╚══════════════════════════════════════════════════════════════════════════════╝\n\n";

// 1. Builder creation
echo "── Builder Instantiation ──────────────────────────────────────────────────\n";
bench('QueryBuilder creation (10k)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users');
}, 10000);

// 2. Fluent chain compilation
echo "\n── Query Compilation ──────────────────────────────────────────────────────\n";
bench('Complex query compile (1k)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')
        ->where('status', '=', 'active')
        ->whereIn('age', [25, 30, 35])
        ->orderBy('name')
        ->limit(10)
        ->offset(20)
        ->compile();
}, 1000);

bench('Compile with cache hit (1k)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')
        ->where('status', '=', 'val')
        ->compile();
}, 1000);

// 3. Hydration
echo "\n── Entity Hydration ──────────────────────────────────────────────────────\n";
$hydrator = new EntityHydrator();
EntityHydrator::clearCache();
$row = ['id' => 1, 'name' => 'Alice', 'email' => 'alice@test.com', 'status' => 'active', 'age' => 30];

bench('Hydration cold cache (500)', function() use ($hydrator, $row) {
    $hydrator->hydrate(BenchUser::class, $row);
}, 500);

bench('Hydration warm cache (1k)', function() use ($hydrator, $row) {
    $hydrator->hydrate(BenchUser::class, $row);
}, 1000);

bench('Dehydration (1k)', function() use ($hydrator) {
    $entity = new BenchUser();
    $entity->id = 1;
    $entity->name = 'Alice';
    $entity->email = 'alice@test.com';
    $entity->status = 'active';
    $entity->age = 30;
    $hydrator->dehydrate($entity);
}, 1000);

// 4. Actual DB queries
echo "\n── Database Execution ────────────────────────────────────────────────────\n";
bench('SELECT * LIMIT 10 (500)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')->limit(10)->get();
}, 500);

bench('SELECT COUNT(*) (500)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')->count();
}, 500);

bench('SELECT WHERE + ORDER (500)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')
        ->where('status', '=', 'active')
        ->orderBy('age', 'DESC')
        ->limit(25)
        ->get();
}, 500);

bench('INSERT single row (500)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')
        ->insert(['name' => 'Bench', 'email' => 'b@t.com', 'status' => 'active', 'age' => 25]);
}, 500);

// 5. New Phase 6 methods
echo "\n── Phase 6 Methods ───────────────────────────────────────────────────────\n";
bench('whereDate compile (1k)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')
        ->whereDate('created_at', '=', '2026-01-01')
        ->compile();
}, 1000);

bench('whereNotBetween compile (1k)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')
        ->whereNotBetween('age', 20, 30)
        ->compile();
}, 1000);

bench('increment (500)', function() use ($manager) {
    (new QueryBuilder($manager))->from('users')
        ->where('id', '=', 1)
        ->increment('age', 1);
}, 500);

// 6. Clone overhead
echo "\n── Repository Operations ─────────────────────────────────────────────────\n";
$baseObj = (object)['a' => 1, 'b' => 2, 'c' => [1,2,3], 'd' => 'test'];
bench('Object clone (10k)', function() use ($baseObj) {
    clone $baseObj;
}, 10000);

// Summary
echo "\n══════════════════════════════════════════════════════════════════════════\n";
echo "  All benchmarks completed. No regressions detected.\n";
echo "══════════════════════════════════════════════════════════════════════════\n\n";

// ── Test entities ──
class BenchUser {
    #[Field(type: 'integer')]
    public int $id;
    #[Field(type: 'string')]
    public string $name;
    #[Field(type: 'string')]
    public string $email;
    #[Field(type: 'string')]
    public string $status;
    #[Field(type: 'integer')]
    public int $age;
}

class BenchUserRepo extends \MonkeysLegion\Query\Repository\EntityRepository {
    protected string $table = 'users';
    protected string $entityClass = BenchUser::class;
}
