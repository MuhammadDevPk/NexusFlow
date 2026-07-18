<?php

declare(strict_types=1);

namespace NexusFlow\Job;

interface JobHandlerInterface
{
    /**
     * Executes the task logic.
     * Throws an exception on failure, which will automatically trigger the retry policy.
     *
     * @param Job $job
     * @throws \Throwable
     */
    public function handle(Job $job): void;
}
