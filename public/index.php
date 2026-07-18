<?php

declare(strict_types=1);

require_once __DIR__ . '/../vendor/autoload.php';

$config = require __DIR__ . '/../config.php';

use NexusFlow\Queue\QueueManager;
use NexusFlow\Job\Job;

// Initialize Queue Manager
try {
    $queueManager = new QueueManager($config);
} catch (\Throwable $e) {
    header('HTTP/1.1 500 Internal Server Error');
    header('Content-Type: application/json');
    echo json_encode(['error' => 'Could not connect to database/broker: ' . $e->getMessage()]);
    exit;
}

$uri = parse_url($_SERVER['REQUEST_URI'], PHP_URL_PATH);
$method = $_SERVER['REQUEST_METHOD'];

// Helper to return JSON responses
function jsonResponse(array $data, int $statusCode = 200): void {
    header('Content-Type: application/json');
    http_response_code($statusCode);
    echo json_encode($data);
    exit;
}

// -------------------------------------------------------------
// Route: Webhook Ingestion API
// -------------------------------------------------------------
if ($uri === '/api/events' && $method === 'POST') {
    $rawInput = file_get_contents('php://input');
    $input = json_decode($rawInput, true);

    if (!$input) {
        jsonResponse(['error' => 'Invalid JSON payload'], 400);
    }

    $queue = $input['queue'] ?? null;
    $jobName = $input['name'] ?? null;
    $payload = $input['payload'] ?? null;
    $idempotencyKey = $input['idempotency_key'] ?? null;

    // Validation
    if (!$queue || !isset($config['queues'][$queue])) {
        jsonResponse(['error' => 'Invalid or missing queue name. Available: ' . implode(', ', array_keys($config['queues']))], 400);
    }

    if (!$jobName) {
        jsonResponse(['error' => 'Missing job name'], 400);
    }

    if (!is_array($payload)) {
        jsonResponse(['error' => 'Payload must be an array'], 400);
    }

    if (!$idempotencyKey) {
        jsonResponse(['error' => 'Idempotency key is required for distributed processing resiliency'], 400);
    }

    // Check Idempotency Lock/Status
    // First check status
    $status = $queueManager->checkIdempotencyStatus($idempotencyKey);
    if ($status === 'success') {
        jsonResponse([
            'status' => 'completed',
            'duplicate' => true,
            'message' => 'Event already processed successfully'
        ], 200);
    }

    // Try to acquire lock to see if it is in progress
    if (!$queueManager->acquireIdempotencyLock($idempotencyKey, 60)) {
        jsonResponse(['error' => 'Event is already being processed by a worker'], 409);
    }

    // Release lock since we will enqueue it and worker will lock it during execution
    $queueManager->releaseIdempotencyLock($idempotencyKey);

    // Create Job DTO
    $job = new Job(
        id: $idempotencyKey,
        name: $jobName,
        payload: $payload,
        queue: $queue,
        attempts: 0,
        createdAt: microtime(true)
    );

    try {
        $streamId = $queueManager->enqueue($job);
        jsonResponse([
            'status' => 'queued',
            'job_id' => $job->id,
            'stream_id' => $streamId
        ], 202);
    } catch (\Throwable $e) {
        jsonResponse(['error' => 'Failed to enqueue job: ' . $e->getMessage()], 500);
    }
}

// -------------------------------------------------------------
// Route: System Metrics (Dashboard Data)
// -------------------------------------------------------------
if ($uri === '/api/metrics' && $method === 'GET') {
    $metrics = $queueManager->getSystemMetrics($config['queues']);
    jsonResponse($metrics);
}

// -------------------------------------------------------------
// Route: Reset Dashboard Metrics
// -------------------------------------------------------------
if ($uri === '/api/metrics/reset' && $method === 'POST') {
    $queueManager->resetMetrics();
    jsonResponse(['status' => 'success', 'message' => 'Throughput metrics reset.']);
}

// -------------------------------------------------------------
// Route: DLQ - Fetch Jobs
// -------------------------------------------------------------
if ($uri === '/api/dlq' && $method === 'GET') {
    $jobs = $queueManager->getDLQJobs();
    jsonResponse($jobs);
}

// -------------------------------------------------------------
// Route: DLQ - Retry Jobs
// -------------------------------------------------------------
if ($uri === '/api/dlq/retry' && $method === 'POST') {
    $input = json_decode(file_get_contents('php://input'), true);
    $jobId = $input['job_id'] ?? null;

    if ($jobId) {
        $success = $queueManager->retryDLQJob($jobId);
        if ($success) {
            jsonResponse(['status' => 'success', 'message' => "Job $jobId re-enqueued successfully."]);
        } else {
            jsonResponse(['error' => "Job $jobId not found in DLQ or could not be retried."], 404);
        }
    } else {
        $count = $queueManager->retryDLQAll();
        jsonResponse(['status' => 'success', 'message' => "Successfully re-enqueued $count jobs from DLQ."]);
    }
}

// -------------------------------------------------------------
// Route: DLQ - Purge Jobs
// -------------------------------------------------------------
if ($uri === '/api/dlq/purge' && $method === 'POST') {
    $input = json_decode(file_get_contents('php://input'), true);
    $jobId = $input['job_id'] ?? null;

    if ($jobId) {
        $success = $queueManager->purgeDLQJob($jobId);
        if ($success) {
            jsonResponse(['status' => 'success', 'message' => "Job $jobId purged from DLQ."]);
        } else {
            jsonResponse(['error' => "Job $jobId not found in DLQ."], 404);
        }
    } else {
        $queueManager->purgeDLQAll();
        jsonResponse(['status' => 'success', 'message' => 'DLQ purged successfully.']);
    }
}

// -------------------------------------------------------------
// Route: Bulk Load-Test (Task Spawning Generator)
// -------------------------------------------------------------
if ($uri === '/api/load-test' && $method === 'POST') {
    $input = json_decode(file_get_contents('php://input'), true);
    $count = (int) ($input['count'] ?? 30);
    
    $queues = ['pdf-generation', 'api-sync', 'data-transform'];
    $jobsCreated = 0;

    for ($i = 0; $i < $count; $i++) {
        $queue = $queues[array_rand($queues)];
        $id = 'test-job-' . bin2hex(random_bytes(6));
        
        switch ($queue) {
            case 'pdf-generation':
                $job = new Job(
                    id: $id,
                    name: 'pdf.generate',
                    payload: [
                        'template' => 'invoice-template-' . random_int(1, 5),
                        'order_id' => random_int(10000, 99999),
                        'user_id' => random_int(100, 999)
                    ],
                    queue: $queue,
                    createdAt: microtime(true)
                );
                break;
            case 'api-sync':
                $endpoints = ['users/sync', 'crm/contacts', 'billing/invoices'];
                $job = new Job(
                    id: $id,
                    name: 'api.sync',
                    payload: [
                        'endpoint' => $endpoints[array_rand($endpoints)],
                        'payload' => [
                            'synced_at' => date('Y-m-d H:i:s'),
                            'records' => random_int(10, 50)
                        ]
                    ],
                    queue: $queue,
                    createdAt: microtime(true)
                );
                break;
            case 'data-transform':
                $operations = ['aggregation', 'anonymization', 'scrubbing'];
                $job = new Job(
                    id: $id,
                    name: 'data.transform',
                    payload: [
                        'records_count' => random_int(100, 1000),
                        'operation' => $operations[array_rand($operations)]
                    ],
                    queue: $queue,
                    createdAt: microtime(true)
                );
                break;
        }

        try {
            $queueManager->enqueue($job);
            $jobsCreated++;
        } catch (\Throwable) {
            // Ignore individual insertion issues during load test
        }
    }

    jsonResponse(['status' => 'success', 'message' => "Successfully queued $jobsCreated load-test jobs across all queues."]);
}

// -------------------------------------------------------------
// Route: Read Logs
// -------------------------------------------------------------
if ($uri === '/api/logs' && $method === 'GET') {
    $logPath = __DIR__ . '/../logs/nexusflow.log';
    if (file_exists($logPath)) {
        $lines = file($logPath);
        $lastLines = array_slice($lines, -50);
        jsonResponse(['logs' => array_map('trim', $lastLines)]);
    } else {
        jsonResponse(['logs' => ['No logs available yet. Start the worker-manager daemon to see activity.']]);
    }
}

// -------------------------------------------------------------
// Serve Static Frontend Dashboard Assets
// -------------------------------------------------------------
if ($uri === '/' || $uri === '/index.html') {
    header('Content-Type: text/html');
    readfile(__DIR__ . '/index.html');
    exit;
}

if ($uri === '/assets/css/styles.css') {
    header('Content-Type: text/css');
    readfile(__DIR__ . '/assets/css/styles.css');
    exit;
}

if ($uri === '/assets/js/dashboard.js') {
    header('Content-Type: application/javascript');
    readfile(__DIR__ . '/assets/js/dashboard.js');
    exit;
}

// Route not found
header('HTTP/1.1 404 Not Found');
header('Content-Type: application/json');
echo json_encode(['error' => 'Endpoint not found']);
exit;
