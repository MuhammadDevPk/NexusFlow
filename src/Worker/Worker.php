<?php

declare(strict_types=1);

namespace NexusFlow\Worker;

use NexusFlow\Job\Job;
use NexusFlow\Job\JobRegistry;
use NexusFlow\Logging\Logger;
use NexusFlow\Queue\QueueManager;

class Worker
{
    private bool $running = true;
    private string $id;

    public function __construct(
        private readonly QueueManager $queueManager,
        private readonly JobRegistry $jobRegistry,
        private readonly Logger $logger,
        private readonly string $queue,
        private readonly string $group = 'nexusflow-group',
        private readonly array $retryConfig = []
    ) {
        $this->id = "worker:" . $this->queue . ":" . bin2hex(random_bytes(4));
    }

    /**
     * Start the worker execution loop.
     */
    public function start(): void
    {
        $this->logger->info("Worker {$this->id} starting...");
        
        // Register signal handlers for clean shut-down
        if (extension_loaded('pcntl')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGTERM, [$this, 'handleSignal']);
            pcntl_signal(SIGINT, [$this, 'handleSignal']);
        }

        // Initialize consumer group
        $this->queueManager->initConsumerGroup($this->queue, $this->group);

        while ($this->running) {
            if (!$this->running) {
                break;
            }

            try {
                // Heartbeat: Idle
                $this->queueManager->registerWorkerHeartbeat($this->id, $this->queue, 'idle');

                // Reserve a job (blocking read up to 500ms)
                $job = $this->queueManager->reserve($this->queue, $this->group, $this->id, 500);

                if ($job !== null) {
                    // Heartbeat: Busy
                    $this->queueManager->registerWorkerHeartbeat($this->id, $this->queue, 'busy');
                    $this->processJob($job);
                }

            } catch (\Throwable $e) {
                $this->logger->error("Error in worker loop: " . $e->getMessage(), [
                    'worker_id' => $this->id,
                    'exception' => get_class($e),
                    'trace' => $e->getTraceAsString()
                ]);
                // Prevent tight loop on Redis connection issues
                sleep(1);
            }

            // Sleep briefly to prevent high CPU utilization
            usleep(20000); // 20ms
        }

        // Cleanup heartbeat
        $this->queueManager->unregisterWorkerHeartbeat($this->id);
        $this->logger->info("Worker {$this->id} shut down gracefully.");
    }

    /**
     * Process a single reserved job.
     */
    private function processJob(Job $job): void
    {
        $this->logger->info("Processing job {$job->id} [{$job->name}] (Attempt: {$job->attempts})");
        
        $ttl = $this->retryConfig['idempotency_ttl'] ?? 3600;

        // 1. Try to acquire idempotency lock
        if (!$this->queueManager->acquireIdempotencyLock($job->id, 60)) {
            $this->logger->warning("Lock already held for job {$job->id}. Skipping processing.");
            return;
        }

        try {
            // 2. Check if job is already marked as success
            $status = $this->queueManager->checkIdempotencyStatus($job->id);
            if ($status === 'success') {
                $this->logger->warning("Job {$job->id} already processed successfully. Acknowledging and skipping.");
                $this->queueManager->acknowledge($this->queue, $this->group, $job->streamId);
                return;
            }

            // 3. Resolve the job handler and run
            $handler = $this->jobRegistry->get($job->name);
            
            $startTime = microtime(true);
            $handler->handle($job);
            $duration = round((microtime(true) - $startTime) * 1000, 2);

            // 4. Handle Success
            $this->queueManager->markIdempotencySuccess($job->id, $ttl);
            $this->queueManager->acknowledge($this->queue, $this->group, $job->streamId);
            
            $this->logger->success("Job {$job->id} [{$job->name}] completed successfully in {$duration}ms");

        } catch (\Throwable $e) {
            $this->logger->error("Job {$job->id} failed: " . $e->getMessage());
            $this->handleJobFailure($job, $e->getMessage());
        } finally {
            // 5. Always release the lock
            $this->queueManager->releaseIdempotencyLock($job->id);
        }
    }

    /**
     * Handle job failure - retry with exponential backoff or dispatch to DLQ.
     */
    private function handleJobFailure(Job $job, string $errorMessage): void
    {
        $maxRetries = $this->retryConfig['max_retries'] ?? 3;
        $baseDelay = $this->retryConfig['base_delay'] ?? 5;
        $multiplier = $this->retryConfig['multiplier'] ?? 2;

        if ($job->attempts < $maxRetries) {
            // Calculate delay using exponential backoff: base_delay * (multiplier ^ attempts)
            // e.g. Attempt 0: 5s. Attempt 1: 10s. Attempt 2: 20s.
            $delay = $baseDelay * pow($multiplier, $job->attempts);
            
            $this->logger->warning("Scheduling retry for job {$job->id} [{$job->name}] in {$delay}s (Attempt {$job->attempts} of {$maxRetries})");
            
            $this->queueManager->scheduleRetry($job, $delay, $errorMessage);
        } else {
            $this->logger->error("Job {$job->id} [{$job->name}] exceeded max retries. Moving to Dead Letter Queue (DLQ).");
            
            $this->queueManager->moveToDLQ($job, $errorMessage);
        }

        // Acknowledge the message in the current stream so it is removed from the active loop
        if ($job->streamId !== null) {
            $this->queueManager->acknowledge($this->queue, $this->group, $job->streamId);
        }
    }

    /**
     * POSIX signal handler.
     */
    public function handleSignal(int $signo): void
    {
        $this->logger->info("Signal $signo received. Requesting worker shutdown...");
        $this->running = false;
    }
}
