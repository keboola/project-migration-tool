<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\Retry;

class RetryTest extends TestCase
{
    private const ERROR = 'does not exist or is not authorized';

    public function testRunsOnceWhenOperationSucceeds(): void
    {
        $calls = 0;
        $sleeps = [];
        $logger = new TestLogger();

        Retry::whileErrorContains(
            self::ERROR,
            function () use (&$calls): void {
                $calls++;
            },
            sleeper: function (int $seconds) use (&$sleeps): void {
                $sleeps[] = $seconds;
            },
            logger: $logger,
            retryMessage: 'Group is not visible yet.',
        );

        self::assertSame(1, $calls);
        self::assertSame([], $sleeps);
        self::assertSame([], $logger->records);
    }

    public function testRetriesMatchingErrorUntilOperationSucceeds(): void
    {
        $calls = 0;
        $sleeps = [];
        $logger = new TestLogger();

        Retry::whileErrorContains(
            self::ERROR,
            function () use (&$calls): void {
                $calls++;
                if ($calls < 3) {
                    throw new RuntimeException("Replication group 'X' " . self::ERROR . '.');
                }
            },
            sleeper: function (int $seconds) use (&$sleeps): void {
                $sleeps[] = $seconds;
            },
            logger: $logger,
            retryMessage: 'Group is not visible yet.',
        );

        self::assertSame(3, $calls);
        self::assertSame([15, 15], $sleeps);
        self::assertSame(
            [
                'Group is not visible yet. Retrying in 15 seconds (attempt 2 of 10).',
                'Group is not visible yet. Retrying in 15 seconds (attempt 3 of 10).',
            ],
            array_column($logger->records, 'message'),
        );
    }

    public function testRethrowsOtherErrorWithoutRetry(): void
    {
        $calls = 0;
        $error = new RuntimeException('Insufficient privileges to operate on account');

        try {
            Retry::whileErrorContains(
                self::ERROR,
                function () use (&$calls, $error): void {
                    $calls++;
                    throw $error;
                },
                sleeper: function (int $seconds): void {
                    self::fail('Must not sleep.');
                },
                logger: new TestLogger(),
                retryMessage: 'Group is not visible yet.',
            );
            self::fail('Exception expected.');
        } catch (RuntimeException $e) {
            self::assertSame($error, $e);
        }
        self::assertSame(1, $calls);
    }

    public function testRethrowsLastErrorWhenAttemptsAreExhausted(): void
    {
        $calls = 0;
        $sleeps = 0;

        try {
            Retry::whileErrorContains(
                self::ERROR,
                function () use (&$calls): void {
                    $calls++;
                    throw new RuntimeException('attempt ' . $calls . ' ' . self::ERROR);
                },
                sleeper: function (int $seconds) use (&$sleeps): void {
                    $sleeps++;
                },
                logger: new TestLogger(),
                retryMessage: 'Group is not visible yet.',
                maxAttempts: 3,
            );
            self::fail('Exception expected.');
        } catch (RuntimeException $e) {
            self::assertSame('attempt 3 ' . self::ERROR, $e->getMessage());
        }
        self::assertSame(3, $calls);
        self::assertSame(2, $sleeps);
    }

    public function testAddsHintWhenAttemptsAreExhausted(): void
    {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Group X ' . self::ERROR . '. Check the replication setup.');

        Retry::whileErrorContains(
            self::ERROR,
            function (): void {
                throw new RuntimeException('Group X ' . self::ERROR . '.');
            },
            sleeper: fn(int $seconds): null => null,
            logger: new TestLogger(),
            retryMessage: 'Group is not visible yet.',
            exhaustedHint: 'Check the replication setup.',
            maxAttempts: 2,
        );
    }
}
