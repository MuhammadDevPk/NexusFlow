<?php

declare(strict_types=1);

namespace NexusFlow\Handlers;

use NexusFlow\Job\Job;
use NexusFlow\Job\JobHandlerInterface;
use RuntimeException;

class DataTransformationHandler implements JobHandlerInterface
{
    public function handle(Job $job): void
    {
        $payload = $job->payload;
        $recordsCount = $payload['records_count'] ?? 0;
        
        // Simulate data transformation: 100ms - 400ms delay
        $workTimeMs = random_int(100, 400);
        usleep($workTimeMs * 1000);

        // 5% chance of deadlock or processing error
        if (random_int(1, 100) <= 5) {
            throw new RuntimeException("Database deadlock occurred during serialization of $recordsCount records.");
        }

        // Successfully completed
    }
}
