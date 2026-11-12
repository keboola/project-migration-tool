<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use Psr\Log\AbstractLogger;
use Stringable;

/**
 * Minimal in-test logger replacing the unavailable \Psr\Log\Test\TestLogger.
 * Records all log messages and exposes a substring helper for assertions.
 */
class TestLogger extends AbstractLogger
{
    /** @var array<int, array{level: mixed, message: string}> */
    public array $records = [];

    /**
     * @param array<mixed> $context
     */
    public function log(mixed $level, string|Stringable $message, array $context = []): void
    {
        $this->records[] = [
            'level' => $level,
            'message' => (string) $message,
        ];
    }

    public function hasInfoThatContains(string $needle): bool
    {
        foreach ($this->records as $record) {
            if ($record['level'] === 'info' && str_contains($record['message'], $needle)) {
                return true;
            }
        }
        return false;
    }
}
