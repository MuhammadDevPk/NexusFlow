<?php

declare(strict_types=1);

namespace NexusFlow\Tests;

use PHPUnit\Framework\TestCase;
use NexusFlow\Queue\QueueManager;
use NexusFlow\Job\Job;
use RedisException;

class QueueManagerTest extends TestCase
{
    private QueueManager $queueManager;
    private array $testConfig;

    protected function setUp(): void
    {
        // Integration test configuration using DB 15 (isolated from dev)
        $this->testConfig = [
            'redis' => [
                'host' => '127.0.0.1',
                'port' => 6379,
                'database' => 15,
                'timeout' => 1.0,
            ],
            'retry' => [
                'max_retries' => 2,
                'base_delay' => 1,
                'multiplier' => 2,
            ],
        ];

        try {
            $this->queueManager = new QueueManager($this->testConfig);
            // Flush database 15 before test starts
            $this->queueManager->getRedis()->flushDb();
        } catch (\Throwable $e) {
            $this->markTestSkipped("Redis is not available for integration tests: " . $e->getMessage());
        }
    }

    protected function tearDown(): void
    {
        if (isset($this->queueManager)) {
            try {
                $this->queueManager->getRedis()->flushDb();
            } catch (RedisException) {
                // Ignore disconnect issues during tearDown
            }
        }
    }

    public function testEnqueueAndReserveMessage(): void
    {
        $queue = 'test-queue';
        $group = 'test-group';
        $consumer = 'test-consumer';

        $this->queueManager->initConsumerGroup($queue, $group);

        $job = new Job(
            id: 'job-1',
            name: 'test.run',
            payload: ['data' => 'hello'],
            queue: $queue,
            createdAt: microtime(true)
        );

        $streamId = $this->queueManager->enqueue($job);
        $this->assertNotEmpty($streamId);

        // Reserve
        $reservedJob = $this->queueManager->reserve($queue, $group, $consumer, 100);
        $this->assertNotNull($reservedJob);
        $this->assertEquals($job->id, $reservedJob->id);
        $this->assertEquals($streamId, $reservedJob->streamId);

        // Acknowledge
        $this->queueManager->acknowledge($queue, $group, $reservedJob->streamId);

        // Verify it is removed from active backlog
        $reservedAgain = $this->queueManager->reserve($queue, $group, $consumer, 100);
        $this->assertNull($reservedAgain);
    }

    public function testIdempotencyFlow(): void
    {
        $eventId = 'unique-event-id-999';

        // 1. Lock should succeed first time
        $this->assertTrue($this->queueManager->acquireIdempotencyLock($eventId, 10));

        // 2. Lock should fail while locked
        $this->assertFalse($this->queueManager->acquireIdempotencyLock($eventId, 10));

        // 3. Release lock
        $this->queueManager->releaseIdempotencyLock($eventId);
        $this->assertTrue($this->queueManager->acquireIdempotencyLock($eventId, 10));

        // 4. Mark success
        $this->assertNull($this->queueManager->checkIdempotencyStatus($eventId));
        $this->queueManager->markIdempotencySuccess($eventId, 10);
        $this->assertEquals('success', $this->queueManager->checkIdempotencyStatus($eventId));
    }

    public function testDelayedJobSchedulerAndDistribution(): void
    {
        $job = new Job(
            id: 'job-delayed-1',
            name: 'test.run',
            payload: [],
            queue: 'test-queue'
        );

        // Schedule to run immediately (0s delay)
        $this->queueManager->scheduleRetry($job, 0, 'First failure');

        // Verify delayed count
        $metrics = $this->queueManager->getSystemMetrics(['test-queue' => ['min_workers'=>1, 'max_workers'=>2, 'backlog_threshold'=>5]]);
        $this->assertEquals(1, $metrics['delayed_count']);

        // Move delayed back to active stream
        $moved = $this->queueManager->moveDelayedToActive(10);
        $this->assertEquals(1, $moved);

        // Verify stream size has the job
        $this->queueManager->initConsumerGroup('test-queue', 'test-group');
        $reserved = $this->queueManager->reserve('test-queue', 'test-group', 'test-consumer', 100);
        
        $this->assertNotNull($reserved);
        $this->assertEquals('job-delayed-1', $reserved->id);
        $this->assertEquals(1, $reserved->attempts); // Attempt incremented
        $this->assertEquals('First failure', $reserved->lastError);
    }

    public function testReclaimOrphanedTasks(): void
    {
        $queue = 'reclaim-queue';
        $group = 'reclaim-group';
        $consumer = 'dead-worker';

        $this->queueManager->initConsumerGroup($queue, $group);

        $job = new Job(id: 'orphan-1', name: 'test.reclaim', payload: [], queue: $queue);
        $this->queueManager->enqueue($job);

        // Reserve it. It is now pending for 'dead-worker'
        $reserved = $this->queueManager->reserve($queue, $group, $consumer, 100);
        $this->assertNotNull($reserved);

        // Run reclaimer with 0 seconds idle limit (reclaim anything idle > 0 seconds)
        // This will simulate the task being idle for too long.
        $reclaimed = $this->queueManager->reclaimOrphanedTasks($queue, $group, -1);
        $this->assertEquals(1, $reclaimed);

        // Since delivery count was 1, it should be re-enqueued as attempt 1.
        // Let's verify we can reserve it as a new consumer.
        $newReserved = $this->queueManager->reserve($queue, $group, 'new-worker', 100);
        $this->assertNotNull($newReserved);
        $this->assertEquals('orphan-1', $newReserved->id);
        $this->assertEquals(1, $newReserved->attempts);
        $this->assertStringContainsString("dead-worker", $newReserved->lastError);
    }
}
