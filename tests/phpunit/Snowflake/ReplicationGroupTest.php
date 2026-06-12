<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use Keboola\Component\UserException;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\ReplicationGroup;

class ReplicationGroupTest extends TestCase
{
    public function testCreateSqlOnSource(): void
    {
        $sql = ReplicationGroup::createOnSourceSql(
            'MIGRATION_REPLICATION_GROUP',
            ['DB1', 'DB2'],
            'AWS_US_WEST_2',
            'XY12345',
        );

        self::assertSame(
            'CREATE REPLICATION GROUP IF NOT EXISTS "MIGRATION_REPLICATION_GROUP" '
            . 'OBJECT_TYPES = DATABASES '
            . 'ALLOWED_DATABASES "DB1", "DB2" '
            . 'ALLOWED_ACCOUNTS = AWS_US_WEST_2.XY12345;',
            $sql,
        );
    }

    public function testSetAllowedDatabasesSql(): void
    {
        $sql = ReplicationGroup::setAllowedDatabasesSql(
            'MIGRATION_REPLICATION_GROUP',
            ['DB1', 'DB2'],
        );

        self::assertSame(
            'ALTER REPLICATION GROUP "MIGRATION_REPLICATION_GROUP" '
            . 'SET ALLOWED_DATABASES "DB1", "DB2";',
            $sql,
        );
    }

    public function testCreateReplicaSql(): void
    {
        $sql = ReplicationGroup::createReplicaSql(
            'MIGRATION_REPLICATION_GROUP',
            'AWS_US_EAST_1',
            'SRCACCT',
        );

        self::assertSame(
            'CREATE REPLICATION GROUP IF NOT EXISTS "MIGRATION_REPLICATION_GROUP" '
            . 'AS REPLICA OF AWS_US_EAST_1.SRCACCT."MIGRATION_REPLICATION_GROUP";',
            $sql,
        );
    }

    public function testRefreshSql(): void
    {
        self::assertSame(
            'ALTER REPLICATION GROUP "MIGRATION_REPLICATION_GROUP" REFRESH;',
            ReplicationGroup::refreshSql('MIGRATION_REPLICATION_GROUP'),
        );
    }

    public function testRefreshProgressSql(): void
    {
        self::assertSame(
            'SELECT * FROM TABLE(INFORMATION_SCHEMA.REPLICATION_GROUP_REFRESH_PROGRESS('
            . '\'MIGRATION_REPLICATION_GROUP\'));',
            ReplicationGroup::refreshProgressSql('MIGRATION_REPLICATION_GROUP'),
        );
    }

    public function testDropSql(): void
    {
        self::assertSame(
            'DROP REPLICATION GROUP IF EXISTS "MIGRATION_REPLICATION_GROUP";',
            ReplicationGroup::dropSql('MIGRATION_REPLICATION_GROUP'),
        );
    }

    public function testBuildNameIsDeterministicAndOrderIndependent(): void
    {
        $name1 = ReplicationGroup::buildName(['DB2', 'DB1']);
        $name2 = ReplicationGroup::buildName(['DB1', 'DB2']);

        self::assertSame($name1, $name2);
        self::assertStringStartsWith('MIGRATION_RG_', $name1);
    }

    public function testBuildNameIsCaseInsensitive(): void
    {
        self::assertSame(
            ReplicationGroup::buildName(['db1', 'DB2']),
            ReplicationGroup::buildName(['DB1', 'db2']),
        );
    }

    public function testBuildNameDiffersForDifferentDatabaseSets(): void
    {
        self::assertNotSame(
            ReplicationGroup::buildName(['DB1', 'DB2']),
            ReplicationGroup::buildName(['DB1', 'DB3']),
        );
    }

    public function testWaitForRefreshCompletesAfterInProgressTransition(): void
    {
        // In-progress phases first, then COMPLETED — verifies the in-progress→completed transition.
        $progressResponses = [
            [['PHASE_NAME' => 'PRIMARY_UPLOADING', 'PROGRESS' => '40']],
            [['PHASE_NAME' => 'SECONDARY_DOWNLOADING', 'PROGRESS' => '80']],
            [['PHASE_NAME' => 'COMPLETED', 'PROGRESS' => '100']],
        ];
        $logger = new TestLogger();
        $sleptTimes = 0;

        $group = new ReplicationGroup();
        $group->waitForRefresh(
            'MIGRATION_REPLICATION_GROUP',
            fetchProgress: function () use (&$progressResponses) {
                return array_shift($progressResponses);
            },
            sleeper: function (int $seconds) use (&$sleptTimes): void {
                $sleptTimes++;
            },
            clock: fn(): int => 1000,
            logger: $logger,
            timeoutSeconds: 3600,
            pollIntervalSeconds: 30,
        );

        // polled 3 times, slept twice (between polls)
        self::assertSame(2, $sleptTimes);
        self::assertTrue($logger->hasInfoThatContains('PRIMARY_UPLOADING'));
        self::assertTrue($logger->hasInfoThatContains('COMPLETED'));
    }

    public function testWaitForRefreshCompletesImmediatelyOnCompletedPhase(): void
    {
        $logger = new TestLogger();
        $sleptTimes = 0;

        $group = new ReplicationGroup();
        $group->waitForRefresh(
            'MIGRATION_REPLICATION_GROUP',
            fetchProgress: fn(): array => [['PHASE_NAME' => 'COMPLETED', 'PROGRESS' => '100']],
            sleeper: function (int $seconds) use (&$sleptTimes): void {
                $sleptTimes++;
            },
            clock: fn(): int => 1000,
            logger: $logger,
            timeoutSeconds: 3600,
            pollIntervalSeconds: 30,
        );

        // First poll already COMPLETED — no sleeping.
        self::assertSame(0, $sleptTimes);
        self::assertTrue($logger->hasInfoThatContains('COMPLETED'));
    }

    public function testWaitForRefreshThrowsOnFailedPhase(): void
    {
        $logger = new TestLogger();
        $group = new ReplicationGroup();

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Replication group "MIGRATION_REPLICATION_GROUP" refresh failed (phase FAILED)');

        $group->waitForRefresh(
            'MIGRATION_REPLICATION_GROUP',
            fetchProgress: fn(): array => [['PHASE_NAME' => 'FAILED', 'PROGRESS' => '0']],
            sleeper: fn(int $seconds): null => null,
            clock: fn(): int => 1000,
            logger: $logger,
            timeoutSeconds: 3600,
            pollIntervalSeconds: 30,
        );
    }

    public function testWaitForRefreshThrowsOnCanceledPhase(): void
    {
        $logger = new TestLogger();
        $group = new ReplicationGroup();

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Replication group "MIGRATION_REPLICATION_GROUP" refresh failed (phase CANCELED)',
        );

        $group->waitForRefresh(
            'MIGRATION_REPLICATION_GROUP',
            fetchProgress: fn(): array => [['PHASE_NAME' => 'CANCELED', 'PROGRESS' => '0']],
            sleeper: fn(int $seconds): null => null,
            clock: fn(): int => 1000,
            logger: $logger,
            timeoutSeconds: 3600,
            pollIntervalSeconds: 30,
        );
    }

    public function testWaitForRefreshThrowsOnTimeout(): void
    {
        $logger = new TestLogger();
        $now = 1000;

        $group = new ReplicationGroup();

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Replication group "MIGRATION_REPLICATION_GROUP" refresh did not complete within 60 seconds',
        );

        $group->waitForRefresh(
            'MIGRATION_REPLICATION_GROUP',
            fetchProgress: fn(): array => [['PHASE_NAME' => 'PRIMARY_UPLOADING', 'PROGRESS' => '40']],
            sleeper: function (int $seconds) use (&$now): void {
                $now += $seconds;
            },
            clock: function () use (&$now): int {
                return $now;
            },
            logger: $logger,
            timeoutSeconds: 60,
            pollIntervalSeconds: 30,
        );
    }
}
