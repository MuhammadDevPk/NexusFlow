<?php

declare(strict_types=1);

namespace NexusFlow\Job;

class JobRegistry
{
    /**
     * @var array<string, JobHandlerInterface>
     */
    private array $handlers = [];

    /**
     * Register a job handler.
     */
    public function register(string $jobName, JobHandlerInterface $handler): void
    {
        $this->handlers[$jobName] = $handler;
    }

    /**
     * Check if a job handler is registered.
     */
    public function has(string $jobName): bool
    {
        return isset($this->handlers[$jobName]);
    }

    /**
     * Get a job handler by name.
     */
    public function get(string $jobName): JobHandlerInterface
    {
        if (!$this->has($jobName)) {
            throw new \InvalidArgumentException("No handler registered for job: $jobName");
        }
        return $this->handlers[$jobName];
    }
}
