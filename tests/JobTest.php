<?php

declare(strict_types=1);

namespace NexusFlow\Tests;

use PHPUnit\Framework\TestCase;
use NexusFlow\Job\Job;

class JobTest extends TestCase
{
    public function testSerializationAndDeserialization(): void
    {
        $payload = [
            'order_id' => 12345,
            'template' => 'invoice'
        ];
        
        $job = new Job(
            id: 'job-uuid-123',
            name: 'pdf.generate',
            payload: $payload,
            queue: 'pdf-generation',
            attempts: 1,
            createdAt: 1784376125.089,
            lastError: 'Disk write failed',
            streamId: '167812345-0'
        );

        $serialized = $job->toArray();

        $this->assertEquals('job-uuid-123', $serialized['id']);
        $this->assertEquals('pdf.generate', $serialized['name']);
        $this->assertEquals(json_encode($payload), $serialized['payload']);
        $this->assertEquals('pdf-generation', $serialized['queue']);
        $this->assertEquals('1', $serialized['attempts']);
        $this->assertEquals('1784376125.089', $serialized['created_at']);
        $this->assertEquals('Disk write failed', $serialized['last_error']);
        $this->assertEquals('167812345-0', $serialized['stream_id']);

        // Reconstitute
        $deserialized = Job::fromArray($serialized);

        $this->assertEquals($job->id, $deserialized->id);
        $this->assertEquals($job->name, $deserialized->name);
        $this->assertEquals($job->payload, $deserialized->payload);
        $this->assertEquals($job->queue, $deserialized->queue);
        $this->assertEquals($job->attempts, $deserialized->attempts);
        $this->assertEquals($job->createdAt, $deserialized->createdAt);
        $this->assertEquals($job->lastError, $deserialized->lastError);
        $this->assertEquals($job->streamId, $deserialized->streamId);
    }
}
