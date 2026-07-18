<?php

declare(strict_types=1);

namespace NexusFlow\Handlers;

use NexusFlow\Job\Job;
use NexusFlow\Job\JobHandlerInterface;
use RuntimeException;

class ApiSyncHandler implements JobHandlerInterface
{
    public function handle(Job $job): void
    {
        $payload = $job->payload;
        $endpoint = $payload['endpoint'] ?? 'unknown';

        // Simulate network API call latency: 300ms - 1 second
        $workTimeMs = random_int(300, 1000);
        usleep($workTimeMs * 1000);

        // 20% chance of rate limiting or timeout
        $roll = random_int(1, 100);
        if ($roll <= 10) {
            throw new RuntimeException("External API returned HTTP 429: Too Many Requests on endpoint: /$endpoint.");
        } elseif ($roll <= 20) {
            throw new RuntimeException("Gateway Timeout: External service failed to respond in time.");
        }

        // Successfully completed
    }
}
