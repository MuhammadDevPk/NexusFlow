<?php

declare(strict_types=1);

namespace NexusFlow\Job;

readonly class Job
{
    /**
     * @param array<string, mixed> $payload
     */
    public function __construct(
        public string $id,
        public string $name,
        public array $payload,
        public string $queue,
        public int $attempts = 0,
        public float $createdAt = 0.0,
        public ?string $lastError = null,
        public ?string $streamId = null
    ) {}

    /**
     * Convert the Job DTO into a array suitable for Redis serialization.
     *
     * @return array{id: string, name: string, payload: string, queue: string, attempts: string, created_at: string, last_error: string, stream_id: string}
     */
    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'name' => $this->name,
            'payload' => json_encode($this->payload, JSON_THROW_ON_ERROR),
            'queue' => $this->queue,
            'attempts' => (string) $this->attempts,
            'created_at' => (string) $this->createdAt,
            'last_error' => $this->lastError ?? '',
            'stream_id' => $this->streamId ?? '',
        ];
    }

    /**
     * Reconstitute a Job DTO from serialized Redis data.
     *
     * @param array<string, string> $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            id: $data['id'] ?? '',
            name: $data['name'] ?? '',
            payload: json_decode($data['payload'] ?? '{}', true, 512, JSON_THROW_ON_ERROR),
            queue: $data['queue'] ?? '',
            attempts: (int) ($data['attempts'] ?? 0),
            createdAt: (float) ($data['created_at'] ?? microtime(true)),
            lastError: !empty($data['last_error']) ? $data['last_error'] : null,
            streamId: !empty($data['stream_id']) ? $data['stream_id'] : null
        );
    }
}
