<?php

declare(strict_types=1);

namespace NexusFlow\Logging;

class Logger
{
    private string $logFile;

    public function __construct(string $logFile)
    {
        $this->logFile = $logFile;
        $dir = dirname($logFile);
        if (!is_dir($dir)) {
            mkdir($dir, 0755, true);
        }
    }

    public function info(string $message, array $context = []): void
    {
        $this->log('INFO', $message, $context, '36m'); // Cyan
    }

    public function success(string $message, array $context = []): void
    {
        $this->log('SUCCESS', $message, $context, '32m'); // Green
    }

    public function warning(string $message, array $context = []): void
    {
        $this->log('WARNING', $message, $context, '33m'); // Yellow
    }

    public function error(string $message, array $context = []): void
    {
        $this->log('ERROR', $message, $context, '31m'); // Red
    }

    private function log(string $level, string $message, array $context, string $colorCode): void
    {
        $pid = getmypid();
        $timestamp = date('Y-m-d H:i:s') . '.' . sprintf('%03d', (int)(microtime(true) * 1000) % 1000);
        
        $contextStr = !empty($context) ? ' ' . json_encode($context, JSON_UNESCAPED_SLASHES) : '';
        
        // Console Output (Colored)
        $consoleMsg = sprintf(
            "\033[%s[%s] [PID:%d] [%s] %s%s\033[0m\n",
            $colorCode,
            $timestamp,
            $pid,
            $level,
            $message,
            $contextStr
        );
        echo $consoleMsg;

        // File Output (No colors)
        $fileMsg = sprintf(
            "[%s] [PID:%d] [%s] %s%s\n",
            $timestamp,
            $pid,
            $level,
            $message,
            $contextStr
        );
        file_put_contents($this->logFile, $fileMsg, FILE_APPEND | LOCK_EX);
    }
}
