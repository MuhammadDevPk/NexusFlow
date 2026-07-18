<?php

declare(strict_types=1);

namespace NexusFlow\Handlers;

use NexusFlow\Job\Job;
use NexusFlow\Job\JobHandlerInterface;
use RuntimeException;

class PdfGenerationHandler implements JobHandlerInterface
{
    public function handle(Job $job): void
    {
        $payload = $job->payload;
        $orderId = $payload['order_id'] ?? 'unknown';
        $template = $payload['template'] ?? 'default';

        // Simulate heavy work: 500ms - 1.5 seconds delay
        $workTimeMs = random_int(500, 1500);
        usleep($workTimeMs * 1000);

        // 15% chance of transient failure
        if (random_int(1, 100) <= 15) {
            throw new RuntimeException("PDF rendering engine timed out. Disk write failure on order #$orderId.");
        }

        // Successfully completed
    }
}
