<?php

declare(strict_types=1);

namespace NexusFlow\Worker;

use NexusFlow\Job\JobRegistry;
use NexusFlow\Logging\Logger;
use NexusFlow\Queue\QueueManager;

class WorkerManager
{
    private bool $running = true;
    
    /**
     * Map of PID => ['queue' => string, 'started_at' => float]
     */
    private array $activeWorkers = [];

    /**
     * Track last time queue had backlog for cool-down calculation: queue_name => timestamp
     */
    private array $lastActiveTime = [];

    private float $lastReclaimTime = 0.0;

    public function __construct(
        private readonly array $config,
        private readonly QueueManager $queueManager,
        private readonly JobRegistry $jobRegistry,
        private readonly Logger $logger
    ) {}

    /**
     * Start the Worker Manager master loop.
     */
    public function start(): void
    {
        $this->logger->info("Master Worker Manager starting [PID: " . getmypid() . "]");
        
        $this->registerSignals();
        $this->initializeMinWorkers();

        $checkInterval = $this->config['scaling']['check_interval'] ?? 2;

        while ($this->running) {
            // Signals are dispatched automatically when pcntl_async_signals(true) is set

            if (!$this->running) {
                break;
            }

            // 1. Reap child processes that have exited (prevents zombie processes)
            $this->reapChildren();

            // 2. Poll delayed queue and distribute due tasks to active streams
            try {
                $movedCount = $this->queueManager->moveDelayedToActive(50);
                if ($movedCount > 0) {
                    $this->logger->info("Re-enqueued $movedCount delayed tasks to active streams.");
                }
            } catch (\Throwable $e) {
                $this->logger->error("Error moving delayed tasks: " . $e->getMessage());
            }

            // 2b. Reclaim orphaned/crashed tasks (every 10 seconds)
            if (microtime(true) - $this->lastReclaimTime >= 10.0) {
                $this->lastReclaimTime = microtime(true);
                foreach ($this->config['queues'] as $queue => $cfg) {
                    try {
                        // Reclaim jobs that have been idle/pending for > 60 seconds
                        $reclaimed = $this->queueManager->reclaimOrphanedTasks($queue, 'nexusflow-group', 60);
                        if ($reclaimed > 0) {
                            $this->logger->warning("Reclaimed $reclaimed orphaned/crashed tasks from queue '$queue'.");
                        }
                    } catch (\Throwable $e) {
                        $this->logger->error("Error reclaiming orphaned tasks on queue '$queue': " . $e->getMessage());
                    }
                }
            }

            // 3. Monitor queue sizes and dynamically scale workers
            try {
                $this->evaluateScaling();
            } catch (\Throwable $e) {
                $this->logger->error("Error in scaling evaluation: " . $e->getMessage());
            }

            // 4. Pause for the configured check interval
            sleep($checkInterval);
        }

        $this->shutdown();
    }

    /**
     * Register POSIX signals.
     */
    private function registerSignals(): void
    {
        if (extension_loaded('pcntl')) {
            pcntl_async_signals(true);
            pcntl_signal(SIGTERM, [$this, 'handleSignal']);
            pcntl_signal(SIGINT, [$this, 'handleSignal']);
            // Ignore SIGCHLD here as we poll with pcntl_waitpid inside the loop (cleaner)
            pcntl_signal(SIGCHLD, SIG_IGN);
        }
    }

    /**
     * Handle master process signals.
     */
    public function handleSignal(int $signo): void
    {
        $this->logger->info("Master process received signal $signo. Starting shutdown sequence.");
        $this->running = false;
    }

    /**
     * Scale up all queues to their configured minimum worker counts.
     */
    private function initializeMinWorkers(): void
    {
        foreach ($this->config['queues'] as $queue => $cfg) {
            $minWorkers = $cfg['min_workers'] ?? 1;
            $this->logger->info("Initializing queue '$queue' with $minWorkers minimum workers.");
            for ($i = 0; $i < $minWorkers; $i++) {
                $this->spawnWorker($queue);
            }
        }
    }

    /**
     * Fork a new worker child process for a given queue.
     */
    private function spawnWorker(string $queue): void
    {
        $pid = pcntl_fork();

        if ($pid === -1) {
            $this->logger->error("Failed to fork worker process for queue: $queue");
        } elseif ($pid === 0) {
            // --- CHILD PROCESS ---
            // Re-establish connection to Redis to prevent connection sharing between master and children
            try {
                $childQueueManager = new QueueManager($this->config);
            } catch (\Throwable $e) {
                $this->logger->error("Child failed to connect to Redis: " . $e->getMessage());
                exit(1);
            }

            // Run child worker loop
            $worker = new Worker(
                queueManager: $childQueueManager,
                jobRegistry: $this->jobRegistry,
                logger: $this->logger,
                queue: $queue,
                retryConfig: array_merge($this->config['retry'], ['idempotency_ttl' => $this->config['locks']['idempotency_ttl']])
            );
            
            $worker->start();
            exit(0);
        } else {
            // --- MASTER PROCESS ---
            $this->activeWorkers[$pid] = [
                'queue' => $queue,
                'started_at' => microtime(true)
            ];
            $this->logger->info("Spawned worker process [PID: $pid] for queue '$queue'. Active workers count: " . count($this->activeWorkers));
        }
    }

    /**
     * Reap completed or crashed child processes non-blockingly.
     */
    private function reapChildren(): void
    {
        while (true) {
            // WNOHANG makes the call non-blocking
            $pid = pcntl_waitpid(-1, $status, WNOHANG);

            if ($pid > 0) {
                if (isset($this->activeWorkers[$pid])) {
                    $queue = $this->activeWorkers[$pid]['queue'];
                    unset($this->activeWorkers[$pid]);
                    
                    $exitCode = pcntl_wexitstatus($status);
                    $this->logger->warning("Worker process [PID: $pid] for queue '$queue' exited with code $exitCode.");
                }
            } else {
                break; // No more child state changes
            }
        }
    }

    /**
     * Evaluates backlogs and adjusts worker counts dynamically.
     */
    private function evaluateScaling(): void
    {
        $metrics = $this->queueManager->getSystemMetrics($this->config['queues']);
        $scaleDownDelay = $this->config['scaling']['scale_down_delay'] ?? 10;

        foreach ($this->config['queues'] as $queue => $cfg) {
            $min = $cfg['min_workers'] ?? 1;
            $max = $cfg['max_workers'] ?? 5;
            $threshold = $cfg['backlog_threshold'] ?? 10;

            // Current worker count for this queue
            $currentCount = 0;
            foreach ($this->activeWorkers as $pid => $info) {
                if ($info['queue'] === $queue) {
                    $currentCount++;
                }
            }

            // Load size = backlog + pending un-ACKed jobs
            $backlog = $metrics['queues'][$queue]['backlog'] ?? 0;
            $pending = $metrics['queues'][$queue]['pending'] ?? 0;
            $totalLoad = $backlog + $pending;

            // Target workers formula
            $target = (int)ceil($totalLoad / $threshold);
            $target = max($min, min($max, $target));

            if ($totalLoad > 0) {
                $this->lastActiveTime[$queue] = microtime(true);
            }

            if ($target > $currentCount) {
                // Scale UP
                $toSpawn = $target - $currentCount;
                $this->logger->info("Queue '$queue' load: $totalLoad. Scaling UP: spawning $toSpawn workers. ($currentCount -> $target)");
                for ($i = 0; $i < $toSpawn; $i++) {
                    $this->spawnWorker($queue);
                }
            } elseif ($target < $currentCount) {
                // Scale DOWN check
                // Implement scaling down cool-down to prevent rapid spawn/kill cycles
                $lastActive = $this->lastActiveTime[$queue] ?? 0.0;
                $idleDuration = microtime(true) - $lastActive;

                if ($idleDuration >= $scaleDownDelay) {
                    $toKill = $currentCount - $target;
                    $this->logger->info("Queue '$queue' has been low load for {$scaleDownDelay}s. Scaling DOWN: terminating $toKill workers. ($currentCount -> $target)");
                    
                    // Terminate excess workers
                    $killed = 0;
                    foreach ($this->activeWorkers as $pid => $info) {
                        if ($info['queue'] === $queue) {
                            $this->logger->info("Sending SIGTERM to worker [PID: $pid] (Queue: $queue)");
                            posix_kill($pid, SIGTERM);
                            $killed++;
                            if ($killed >= $toKill) {
                                break;
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * Shut down all workers gracefully.
     */
    private function shutdown(): void
    {
        $this->logger->info("Initiating graceful shutdown. Terminating child workers...");

        // Send SIGTERM to all child worker processes
        foreach (array_keys($this->activeWorkers) as $pid) {
            $this->logger->info("Terminating worker [PID: $pid]...");
            posix_kill($pid, SIGTERM);
        }

        // Wait for all child processes to exit (timeout up to 5 seconds)
        $start = microtime(true);
        while (!empty($this->activeWorkers) && (microtime(true) - $start) < 5.0) {
            $pid = pcntl_waitpid(-1, $status, WNOHANG);
            if ($pid > 0) {
                unset($this->activeWorkers[$pid]);
            }
            usleep(50000); // 50ms
        }

        // Force kill any remaining workers
        if (!empty($this->activeWorkers)) {
            $this->logger->warning("Some workers did not exit in time. Force killing remaining workers: " . implode(', ', array_keys($this->activeWorkers)));
            foreach (array_keys($this->activeWorkers) as $pid) {
                posix_kill($pid, SIGKILL);
            }
        }

        $this->logger->info("Master Worker Manager shut down completed.");
    }
}
