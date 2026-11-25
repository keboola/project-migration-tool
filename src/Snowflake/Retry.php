<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\Component\UserException;
use Psr\Log\LoggerInterface;
use Throwable;

class Retry
{
    public const DEFAULT_MAX_ATTEMPTS = 10;

    public const DEFAULT_DELAY_SECONDS = 15;

    /**
     * Runs the operation again while it fails with an error that contains the given text.
     * This covers changes that are not yet visible on another Snowflake account.
     * Other errors are thrown at once.
     *
     * @param callable():void $operation
     * @param callable(int):mixed $sleeper
     */
    public static function whileErrorContains(
        string $errorText,
        callable $operation,
        callable $sleeper,
        LoggerInterface $logger,
        string $retryMessage,
        ?string $exhaustedHint = null,
        int $maxAttempts = self::DEFAULT_MAX_ATTEMPTS,
        int $delaySeconds = self::DEFAULT_DELAY_SECONDS,
    ): void {
        for ($attempt = 1;; $attempt++) {
            try {
                $operation();
                return;
            } catch (Throwable $e) {
                if (!str_contains($e->getMessage(), $errorText)) {
                    throw $e;
                }
                if ($attempt >= $maxAttempts) {
                    if ($exhaustedHint === null) {
                        throw $e;
                    }
                    throw new UserException($e->getMessage() . ' ' . $exhaustedHint, 0, $e);
                }
            }

            $logger->info(sprintf(
                '%s Retrying in %d seconds (attempt %d of %d).',
                $retryMessage,
                $delaySeconds,
                $attempt + 1,
                $maxAttempts,
            ));
            $sleeper($delaySeconds);
        }
    }
}
