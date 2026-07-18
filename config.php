<?php

declare(strict_types=1);

return [
    'redis' => [
        'host' => '127.0.0.1',
        'port' => 6379,
        'database' => 0,
        'timeout' => 2.5,
    ],
    'queues' => [
        'pdf-generation' => [
            'min_workers' => 1,
            'max_workers' => 5,
            'backlog_threshold' => 10,
        ],
        'api-sync' => [
            'min_workers' => 1,
            'max_workers' => 8,
            'backlog_threshold' => 5,
        ],
        'data-transform' => [
            'min_workers' => 1,
            'max_workers' => 3,
            'backlog_threshold' => 15,
        ],
    ],
    'scaling' => [
        'check_interval' => 2,
        'scale_down_delay' => 10,
    ],
    'retry' => [
        'max_retries' => 3,
        'base_delay' => 5,
        'multiplier' => 2,
    ],
    'locks' => [
        'idempotency_ttl' => 3600,
    ],
    'logging' => [
        'path' => __DIR__ . '/logs/nexusflow.log',
    ],
];
