<?php

declare(strict_types=1);

namespace NexusFlow\Tests;

use PHPUnit\Framework\TestCase;
use NexusFlow\Job\JobRegistry;
use NexusFlow\Job\JobHandlerInterface;
use NexusFlow\Job\Job;

class JobRegistryTest extends TestCase
{
    public function testRegisterAndRetrieveHandler(): void
    {
        $registry = new JobRegistry();
        
        // Mock a handler
        $mockHandler = $this->createMock(JobHandlerInterface::class);
        
        $registry->register('test.job', $mockHandler);

        $this->assertTrue($registry->has('test.job'));
        $this->assertFalse($registry->has('nonexistent.job'));
        $this->assertSame($mockHandler, $registry->get('test.job'));
    }

    public function testGetMissingHandlerThrowsException(): void
    {
        $registry = new JobRegistry();

        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage("No handler registered for job: nonexistent.job");
        
        $registry->get('nonexistent.job');
    }
}
