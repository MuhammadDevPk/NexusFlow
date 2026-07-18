<?php

declare(strict_types=1);

require_once __DIR__ . '/vendor/autoload.php';

$config = require __DIR__ . '/config.php';

use NexusFlow\Queue\QueueManager;

$qm = new QueueManager($config);
$redis = $qm->getRedis();

$queue = 'pdf-generation';
$stream = "stream:$queue";
$group = 'nexusflow-group';
$consumer = 'test-worker-random-' . rand(1, 100);

echo "Checking pending for new consumer: $consumer\n";
try {
    $res = $redis->xReadGroup($group, $consumer, [$stream => '0'], 1, 0);
    echo "Pending result:\n";
    var_dump($res);
} catch (\Throwable $e) {
    echo "Pending error: " . $e->getMessage() . "\n";
}

echo "\nChecking new messages for: $consumer\n";
try {
    $res = $redis->xReadGroup($group, $consumer, [$stream => '>'], 1, 500);
    echo "New result:\n";
    var_dump($res);
} catch (\Throwable $e) {
    echo "New error: " . $e->getMessage() . "\n";
}
