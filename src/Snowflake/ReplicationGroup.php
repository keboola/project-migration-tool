<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Psr\Log\LoggerInterface;

class ReplicationGroup
{
    private const TERMINAL_FAILURE_PHASES = ['FAILED', 'CANCELED'];

    private const NAME_PREFIX = 'MIGRATION_RG_';

    /**
     * Builds a deterministic replication group name from the migrated databases.
     *
     * The name is stable across repeated runs of the same migration (same database set
     * => same name, so the group can be reused idempotently) yet differs between migrations
     * with different database sets, so parallel migrations never collide on a single shared
     * group name. The database list is upper-cased and sorted before hashing so neither the
     * order nor the casing in the configuration affects the result.
     *
     * The whole name is upper-cased so it matches Snowflake's default identifier casing. This
     * matters for REPLICATION_GROUP_REFRESH_PROGRESS, which folds its string argument to upper
     * case: a quoted lower-case group name would be stored case-sensitively and never match.
     *
     * @param string[] $databases
     */
    public static function buildName(array $databases): string
    {
        $normalized = array_map(
            fn(string $database): string => strtoupper($database),
            $databases,
        );
        sort($normalized);

        return self::NAME_PREFIX . strtoupper(substr(hash('sha256', implode(',', $normalized)), 0, 32));
    }

    /**
     * Replication groups authorize accounts by their organization account identifier
     * (`<organization_name>.<account_name>`), not by the legacy `<region>.<account_locator>`
     * form, so the migrate account is passed as organization name + account name.
     *
     * @param string[] $databases
     */
    public static function createOnSourceSql(
        string $groupName,
        array $databases,
        string $migrateOrganizationName,
        string $migrateAccountName,
    ): string {
        return sprintf(
            'CREATE REPLICATION GROUP IF NOT EXISTS %s '
            . 'OBJECT_TYPES = DATABASES '
            . 'ALLOWED_DATABASES = %s '
            . 'ALLOWED_ACCOUNTS = %s.%s;',
            Helper::quoteIdentifier($groupName),
            self::quoteDatabaseList($databases),
            $migrateOrganizationName,
            $migrateAccountName,
        );
    }

    /**
     * @param string[] $databases
     */
    public static function setAllowedDatabasesSql(string $groupName, array $databases): string
    {
        return sprintf(
            'ALTER REPLICATION GROUP %s SET ALLOWED_DATABASES = %s;',
            Helper::quoteIdentifier($groupName),
            self::quoteDatabaseList($databases),
        );
    }

    /**
     * References the primary group by the source account's organization account identifier
     * (`<organization_name>.<account_name>.<group_name>`).
     */
    public static function createReplicaSql(
        string $groupName,
        string $sourceOrganizationName,
        string $sourceAccountName,
    ): string {
        return sprintf(
            'CREATE REPLICATION GROUP IF NOT EXISTS %s AS REPLICA OF %s.%s.%s;',
            Helper::quoteIdentifier($groupName),
            $sourceOrganizationName,
            $sourceAccountName,
            Helper::quoteIdentifier($groupName),
        );
    }

    public static function refreshSql(string $groupName): string
    {
        return sprintf('ALTER REPLICATION GROUP %s REFRESH;', Helper::quoteIdentifier($groupName));
    }

    public static function refreshProgressSql(string $groupName): string
    {
        return sprintf(
            'SELECT * FROM TABLE(SNOWFLAKE.INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS(%s));',
            QueryBuilder::quote($groupName),
        );
    }

    public static function dropSql(string $groupName): string
    {
        return sprintf('DROP REPLICATION GROUP IF EXISTS %s;', Helper::quoteIdentifier($groupName));
    }

    /**
     * Poll REPLICATION_GROUP_REFRESH_PROGRESS until the refresh completes.
     *
     * @param callable():array<int,array<string,mixed>> $fetchProgress
     * @param callable(int):mixed $sleeper
     * @param callable():int $clock epoch seconds
     */
    public function waitForRefresh(
        string $groupName,
        callable $fetchProgress,
        callable $sleeper,
        callable $clock,
        LoggerInterface $logger,
        int $timeoutSeconds,
        int $pollIntervalSeconds,
    ): void {
        $startedAt = $clock();
        $first = true;

        while (true) {
            if (!$first) {
                $sleeper($pollIntervalSeconds);
            }
            $first = false;

            $rows = $fetchProgress();
            $this->logProgress($groupName, $rows, $logger);

            // Terminal failure: stop immediately, do not wait for the timeout.
            $failedPhase = self::failedPhase($rows);
            if ($failedPhase !== null) {
                throw new UserException(sprintf(
                    'Replication group "%s" refresh failed (phase %s).',
                    $groupName,
                    $failedPhase,
                ));
            }

            if (self::isRefreshComplete($rows)) {
                $logger->info(sprintf('Replication group "%s" refresh completed.', $groupName));
                return;
            }

            $elapsed = $clock() - $startedAt;
            if ($elapsed >= $timeoutSeconds) {
                throw new UserException(sprintf(
                    'Replication group "%s" refresh did not complete within %d seconds.',
                    $groupName,
                    $timeoutSeconds,
                ));
            }
        }
    }

    /**
     * @param array<int,array<string,mixed>> $rows
     */
    public static function isRefreshComplete(array $rows): bool
    {
        foreach ($rows as $row) {
            if (self::phaseName($row) === 'COMPLETED') {
                return true;
            }
        }
        return false;
    }

    /**
     * Returns true if any row reports a terminal failure phase (FAILED|CANCELED).
     *
     * @param array<int,array<string,mixed>> $rows
     */
    public static function isRefreshFailed(array $rows): bool
    {
        return self::failedPhase($rows) !== null;
    }

    /**
     * @param array<int,array<string,mixed>> $rows
     */
    private static function failedPhase(array $rows): ?string
    {
        foreach ($rows as $row) {
            $phase = self::phaseName($row);
            if (in_array($phase, self::TERMINAL_FAILURE_PHASES, true)) {
                return $phase;
            }
        }
        return null;
    }

    /**
     * Case-insensitive read of the PHASE_NAME column (Snowflake returns upper-cased keys).
     *
     * @param array<string,mixed> $row
     */
    private static function phaseName(array $row): string
    {
        $value = $row['PHASE_NAME'] ?? $row['phase_name'] ?? '';
        return strtoupper(is_scalar($value) ? (string) $value : '');
    }

    /**
     * @param array<int,array<string,mixed>> $rows
     */
    private function logProgress(string $groupName, array $rows, LoggerInterface $logger): void
    {
        foreach ($rows as $row) {
            $progressValue = $row['PROGRESS'] ?? $row['progress'] ?? '?';
            $progress = is_scalar($progressValue) ? (string) $progressValue : '?';
            $logger->info(sprintf(
                'Replication group "%s" refresh phase "%s": %s%%',
                $groupName,
                self::phaseName($row) !== '' ? self::phaseName($row) : 'UNKNOWN',
                $progress,
            ));
        }
    }

    /**
     * @param string[] $databases
     */
    private static function quoteDatabaseList(array $databases): string
    {
        return implode(', ', array_map(
            fn(string $db): string => Helper::quoteIdentifier($db),
            $databases,
        ));
    }
}
