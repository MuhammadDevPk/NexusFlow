<?php

declare(strict_types=1);

namespace NexusFlow\Queue;

use NexusFlow\Job\Job;
use Redis;
use RedisException;

class QueueManager
{
    private Redis $redis;

    public function __construct(array $config)
    {
        $this->redis = new Redis();
        
        $host = $config['redis']['host'] ?? '127.0.0.1';
        $port = (int) ($config['redis']['port'] ?? 6379);
        $timeout = (float) ($config['redis']['timeout'] ?? 2.0);
        $database = (int) ($config['redis']['database'] ?? 0);

        try {
            $this->redis->connect($host, $port, $timeout);
            $this->redis->select($database);
        } catch (RedisException $e) {
            throw new \RuntimeException("Failed to connect to Redis: " . $e->getMessage(), 0, $e);
        }
    }

    public function getRedis(): Redis
    {
        return $this->redis;
    }

    /**
     * Enqueue a job into its designated Redis Stream.
     */
    public function enqueue(Job $job): string
    {
        $stream = "stream:{$job->queue}";
        $data = $job->toArray();
        
        // Add to Stream, capping it at ~10,000 to prevent infinite growth
        $streamId = $this->redis->xAdd($stream, '*', $data, 10000, true);
        if (!$streamId) {
            throw new \RuntimeException("Failed to enqueue job {$job->id} to stream $stream");
        }

        // Log throughput activity
        $this->incrementMetric('throughput:pushed');
        
        return $streamId;
    }

    /**
     * Initialize a consumer group if it doesn't already exist.
     */
    public function initConsumerGroup(string $queue, string $group): void
    {
        $stream = "stream:$queue";
        try {
            // MKSTREAM option (last param = true) will create the stream if it doesn't exist.
            // Under php-redis, xGroup('CREATE', stream, group, '0', true)
            @$this->redis->xGroup('CREATE', $stream, $group, '0', true);
        } catch (RedisException $e) {
            // Ignore if the group already exists
            if (str_contains($e->getMessage(), 'BUSYGROUP')) {
                return;
            }
            throw $e;
        }
    }

    /**
     * Reserve a job from the stream.
     * Checks pending jobs first, then checks for new jobs.
     */
    public function reserve(string $queue, string $group, string $consumer, int $blockMs = 500): ?Job
    {
        $stream = "stream:$queue";
        
        // 1. First check for pending messages of this specific consumer (recovery from crash)
        // Read pending messages with ID = '0'
        $pending = $this->redis->xReadGroup($group, $consumer, [$stream => '0'], 1, 0);
        if (!empty($pending) && !empty($pending[$stream])) {
            return $this->parseReadResult($pending, $stream);
        }

        // 2. No pending messages for this worker, wait for new messages using ID = '>'
        // blockMs limits how long we wait if there is no load
        $new = $this->redis->xReadGroup($group, $consumer, [$stream => '>'], 1, $blockMs);
        if (!empty($new) && !empty($new[$stream])) {
            return $this->parseReadResult($new, $stream);
        }

        return null;
    }

    /**
     * Parse the raw nested array from xReadGroup into a Job DTO.
     */
    private function parseReadResult(array $result, string $stream): ?Job
    {
        foreach ($result[$stream] as $streamId => $fields) {
            // Ensure fields are strings
            $stringFields = [];
            foreach ($fields as $key => $value) {
                $stringFields[(string)$key] = (string)$value;
            }
            
            // Add the internal stream message ID
            $stringFields['stream_id'] = (string)$streamId;
            
            return Job::fromArray($stringFields);
        }
        return null;
    }

    /**
     * Acknowledge and delete a job from the stream to keep memory low.
     */
    public function acknowledge(string $queue, string $group, string $streamId): void
    {
        $stream = "stream:$queue";
        $this->redis->xAck($stream, $group, [$streamId]);
        $this->redis->xDel($stream, [$streamId]);
        
        $this->incrementMetric('throughput:acked');
    }

    /**
     * Tries to acquire an idempotency lock for an event ID.
     */
    public function acquireIdempotencyLock(string $eventId, int $ttl): bool
    {
        // SET key val NX EX ttl
        $key = "lock:idempotency:$eventId";
        $result = $this->redis->set($key, '1', ['NX', 'EX' => $ttl]);
        return $result === true;
    }

    /**
     * Release the idempotency lock.
     */
    public function releaseIdempotencyLock(string $eventId): void
    {
        $key = "lock:idempotency:$eventId";
        $this->redis->del($key);
    }

    /**
     * Marks an event ID as successfully processed.
     */
    public function markIdempotencySuccess(string $eventId, int $ttl): void
    {
        $key = "status:idempotency:$eventId";
        $this->redis->set($key, 'success', ['EX' => $ttl]);
    }

    /**
     * Checks if a job has already completed successfully.
     */
    public function checkIdempotencyStatus(string $eventId): ?string
    {
        $key = "status:idempotency:$eventId";
        $val = $this->redis->get($key);
        return $val ? (string)$val : null;
    }

    /**
     * Add a job to the delayed queue.
     */
    public function scheduleRetry(Job $job, int $delaySeconds, string $error): void
    {
        $updatedJob = new Job(
            id: $job->id,
            name: $job->name,
            payload: $job->payload,
            queue: $job->queue,
            attempts: $job->attempts + 1,
            createdAt: $job->createdAt,
            lastError: $error
        );

        $delayTimestamp = microtime(true) + $delaySeconds;
        $payload = json_encode($updatedJob->toArray(), JSON_THROW_ON_ERROR);

        $this->redis->zAdd('queue:delayed', [], $delayTimestamp, $payload);
        $this->incrementMetric('throughput:delayed');
    }

    /**
     * Move due delayed jobs back to their active streams.
     * Returns the count of jobs moved.
     */
    public function moveDelayedToActive(int $limit = 50): int
    {
        $now = microtime(true);
        // Find jobs where score <= now
        $jobs = $this->redis->zRangeByScore('queue:delayed', '-inf', (string)$now, ['limit' => [0, $limit]]);
        
        if (empty($jobs)) {
            return 0;
        }

        $moved = 0;
        foreach ($jobs as $jobJson) {
            // Atomic check: Try to remove it from ZSET first.
            // If zRem returns 1, we won the race and are responsible for enqueuing it.
            if ($this->redis->zRem('queue:delayed', $jobJson) === 1) {
                try {
                    $data = json_decode($jobJson, true, 512, JSON_THROW_ON_ERROR);
                    $job = Job::fromArray($data);
                    $this->enqueue($job);
                    $moved++;
                } catch (\Throwable $e) {
                    // If enqueuing fails, push to DLQ directly to avoid dropping the job
                    $this->redis->lPush('queue:dlq', $jobJson);
                }
            }
        }

        return $moved;
    }

    /**
     * Inspects the Pending Entry List (PEL) for a queue, claims orphaned tasks
     * (whose idle time exceeds the threshold), and either moves them to the DLQ (if delivery count is too high)
     * or re-enqueues them with incremented attempts.
     */
    public function reclaimOrphanedTasks(string $queue, string $group, int $timeoutSeconds = 300): int
    {
        $stream = "stream:$queue";
        $timeoutMs = $timeoutSeconds * 1000;
        
        try {
            // Fetch pending messages: xPending(stream, group, start, end, count)
            // We read the first 50 pending messages.
            $pendingList = $this->redis->xPending($stream, $group, '-', '+', 50);
        } catch (RedisException) {
            return 0; // Group might not exist yet
        }

        if (empty($pendingList) || !is_array($pendingList)) {
            return 0;
        }

        $reclaimed = 0;

        foreach ($pendingList as $item) {
            // In php-redis, xPending with range returns arrays containing:
            // [0] messageId, [1] consumer, [2] idleTimeMs, [3] deliveryCount
            if (!isset($item[0], $item[1], $item[2], $item[3])) {
                continue;
            }

            $messageId = (string)$item[0];
            $consumer = (string)$item[1];
            $idleMs = (int)$item[2];
            $deliveries = (int)$item[3];

            // If the message has been idle (un-ACKed) for longer than the timeout
            if ($idleMs > $timeoutMs) {
                // Try to claim it atomically. This shifts the owner to 'reclaimer-daemon' and resets the idle timer.
                $claimed = $this->redis->xClaim($stream, $group, 'reclaimer-daemon', $timeoutMs, [$messageId]);
                
                if (!empty($claimed) && is_array($claimed)) {
                    foreach ($claimed as $msgId => $fields) {
                        try {
                            $stringFields = [];
                            foreach ($fields as $k => $v) {
                                $stringFields[(string)$k] = (string)$v;
                            }
                            $stringFields['stream_id'] = (string)$msgId;
                            $job = Job::fromArray($stringFields);

                            // If it has been delivered too many times, it's a poison pill
                            if ($deliveries >= 3) {
                                $this->moveToDLQ($job, "Orphaned task exceeded max deliveries ($deliveries). Force moved to DLQ.");
                                $this->acknowledge($queue, $group, (string)$msgId);
                            } else {
                                // Increment attempt and re-enqueue
                                $updatedJob = new Job(
                                    id: $job->id,
                                    name: $job->name,
                                    payload: $job->payload,
                                    queue: $job->queue,
                                    attempts: $job->attempts + 1,
                                    createdAt: $job->createdAt,
                                    lastError: "Orphaned task reclaimed from consumer '$consumer' (Idle: " . round($idleMs / 1000, 1) . "s)"
                                );
                                $this->enqueue($updatedJob);
                                $this->acknowledge($queue, $group, (string)$msgId);
                            }
                            $reclaimed++;
                        } catch (\Throwable) {
                            // If parsing fails, push to DLQ directly
                            $this->redis->lPush('queue:dlq', json_encode([
                                'id' => $messageId,
                                'queue' => $queue,
                                'name' => 'unknown',
                                'payload' => [],
                                'attempts' => $deliveries,
                                'created_at' => (string)microtime(true),
                                'last_error' => 'Failed to parse claimed task payload'
                            ]));
                            $this->acknowledge($queue, $group, (string)$msgId);
                            $reclaimed++;
                        }
                    }
                }
            }
        }

        return $reclaimed;
    }

    /**
     * Push a failed job to the Dead Letter Queue.
     */
    public function moveToDLQ(Job $job, string $error): void
    {
        $failedJob = new Job(
            id: $job->id,
            name: $job->name,
            payload: $job->payload,
            queue: $job->queue,
            attempts: $job->attempts,
            createdAt: $job->createdAt,
            lastError: $error
        );

        $this->redis->lPush('queue:dlq', json_encode($failedJob->toArray(), JSON_THROW_ON_ERROR));
        $this->incrementMetric('throughput:dlq');
    }

    /**
     * Get DLQ statistics and jobs.
     */
    public function getDLQJobs(int $offset = 0, int $limit = 50): array
    {
        $raw = $this->redis->lRange('queue:dlq', $offset, $offset + $limit - 1);
        $jobs = [];
        foreach ($raw as $json) {
            try {
                $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
                $jobs[] = Job::fromArray($data);
            } catch (\Throwable) {
                // Ignore corrupted json
            }
        }
        return $jobs;
    }

    /**
     * Retry a job from the DLQ.
     */
    public function retryDLQJob(string $jobId): bool
    {
        $rawJobs = $this->redis->lRange('queue:dlq', 0, -1);
        foreach ($rawJobs as $json) {
            try {
                $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
                if (($data['id'] ?? '') === $jobId) {
                    // Try to remove atomically by value
                    if ($this->redis->lRem('queue:dlq', $json, 1) > 0) {
                        $job = Job::fromArray($data);
                        // Reset attempt count for a fresh start
                        $resetJob = new Job(
                            id: $job->id,
                            name: $job->name,
                            payload: $job->payload,
                            queue: $job->queue,
                            attempts: 0,
                            createdAt: microtime(true),
                            lastError: null
                        );
                        $this->enqueue($resetJob);
                        return true;
                    }
                }
            } catch (\Throwable) {
                continue;
            }
        }
        return false;
    }

    /**
     * Purge a specific job from DLQ.
     */
    public function purgeDLQJob(string $jobId): bool
    {
        $rawJobs = $this->redis->lRange('queue:dlq', 0, -1);
        foreach ($rawJobs as $json) {
            try {
                $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
                if (($data['id'] ?? '') === $jobId) {
                    if ($this->redis->lRem('queue:dlq', $json, 1) > 0) {
                        return true;
                    }
                }
            } catch (\Throwable) {
                continue;
            }
        }
        return false;
    }

    /**
     * Clear all DLQ jobs.
     */
    public function purgeDLQAll(): void
    {
        $this->redis->del('queue:dlq');
    }

    /**
     * Retry all DLQ jobs.
     */
    public function retryDLQAll(): int
    {
        $rawJobs = $this->redis->lRange('queue:dlq', 0, -1);
        $retried = 0;
        foreach ($rawJobs as $json) {
            try {
                $data = json_decode($json, true, 512, JSON_THROW_ON_ERROR);
                if ($this->redis->lRem('queue:dlq', $json, 1) > 0) {
                    $job = Job::fromArray($data);
                    $resetJob = new Job(
                        id: $job->id,
                        name: $job->name,
                        payload: $job->payload,
                        queue: $job->queue,
                        attempts: 0,
                        createdAt: microtime(true),
                        lastError: null
                    );
                    $this->enqueue($resetJob);
                    $retried++;
                }
            } catch (\Throwable) {
                continue;
            }
        }
        return $retried;
    }

    /**
     * Increment a performance/throughput metric counter.
     */
    public function incrementMetric(string $key, int $by = 1): void
    {
        $this->redis->incrBy($key, $by);
    }

    /**
     * Register worker heartbeat.
     */
    public function registerWorkerHeartbeat(string $workerId, string $queue, string $status): void
    {
        $key = "worker:heartbeat:$workerId";
        $data = [
            'id' => $workerId,
            'queue' => $queue,
            'status' => $status,
            'pid' => (string)getmypid(),
            'updated_at' => (string)microtime(true)
        ];
        $this->redis->hMSet($key, $data);
        $this->redis->expire($key, 10); // Expire if no heartbeat in 10s
    }

    /**
     * Unregister worker heartbeat.
     */
    public function unregisterWorkerHeartbeat(string $workerId): void
    {
        $key = "worker:heartbeat:$workerId";
        $this->redis->del($key);
    }

    /**
     * Fetch active workers registered.
     */
    public function getActiveWorkers(): array
    {
        // Scan keys matching worker:heartbeat:*
        $iterator = null;
        $workers = [];
        do {
            $keys = $this->redis->scan($iterator, 'worker:heartbeat:*', 100);
            if ($keys !== false) {
                foreach ($keys as $key) {
                    $data = $this->redis->hGetAll($key);
                    if (!empty($data)) {
                        $workers[] = $data;
                    }
                }
            }
        } while ($iterator > 0);

        return $workers;
    }

    /**
     * Pull system metrics for the dashboard.
     */
    public function getSystemMetrics(array $queueConfigs): array
    {
        $metrics = [
            'queues' => [],
            'dlq_count' => $this->redis->lLen('queue:dlq'),
            'delayed_count' => $this->redis->zCard('queue:delayed'),
            'throughput' => [
                'pushed' => (int) ($this->redis->get('throughput:pushed') ?: 0),
                'acked' => (int) ($this->redis->get('throughput:acked') ?: 0),
                'delayed' => (int) ($this->redis->get('throughput:delayed') ?: 0),
                'dlq' => (int) ($this->redis->get('throughput:dlq') ?: 0),
            ],
            'workers' => $this->getActiveWorkers()
        ];

        foreach ($queueConfigs as $name => $cfg) {
            $stream = "stream:$name";
            $len = $this->redis->xLen($stream);
            
            // Get size of pending entries
            $pendingCount = 0;
            try {
                // xPending returns [count, minId, maxId, consumers] in php-redis
                $pendingInfo = $this->redis->xPending($stream, "nexusflow-group");
                if (isset($pendingInfo[0])) {
                    $pendingCount = (int)$pendingInfo[0];
                }
            } catch (RedisException) {
                // If group doesn't exist yet, it has 0 pending
            }

            $metrics['queues'][$name] = [
                'backlog' => $len,
                'pending' => $pendingCount,
                'min_workers' => $cfg['min_workers'],
                'max_workers' => $cfg['max_workers'],
            ];
        }

        return $metrics;
    }

    /**
     * Resets throughput counters.
     */
    public function resetMetrics(): void
    {
        $this->redis->set('throughput:pushed', '0');
        $this->redis->set('throughput:acked', '0');
        $this->redis->set('throughput:delayed', '0');
        $this->redis->set('throughput:dlq', '0');
    }
}
