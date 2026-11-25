<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Cleanup;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\ReplicationGroup;
use ProjectMigrationTool\Tests\Snowflake\TestLogger;

class CleanupTest extends TestCase
{
    private const DATABASE = 'KEBOOLA_1';
    private const TARGET_ROLE = 'MIGRATE_TARGET';
    private const TARGET_USER = 'MIGRATE_TARGET_USER';
    private const STILL_PRIMARY_ERROR = 'SQL error: Replication group \'MIGRATION_RG_X\' is currently a primary '
        . 'replication group in a replication relationship and cannot be dropped. Promote a secondary first.';

    private TestLogger $logger;

    /** @var string[] */
    private array $sourceQueries = [];

    /** @var int[] */
    private array $sleeps = [];

    protected function setUp(): void
    {
        $this->logger = new TestLogger();
    }

    public function testTeardownReplicationRetriesPrimaryDropUntilReplicaDropIsVisible(): void
    {
        $failures = 2;
        $this->createCrossRegionCleanup(function (string $sql) use (&$failures): void {
            if ($failures > 0) {
                $failures--;
                throw new RuntimeException(self::STILL_PRIMARY_ERROR);
            }
        })->teardownReplication($this->sleeper());

        $dropSql = ReplicationGroup::dropSql($this->groupName());
        self::assertSame([$dropSql, $dropSql, $dropSql], $this->sourceQueries);
        self::assertSame([15, 15], $this->sleeps);
        self::assertTrue($this->logger->hasInfoThatContains(sprintf(
            'Replication group "%s" is still a primary of a replica on the source account. '
            . 'Retrying in 15 seconds (attempt 2 of 10).',
            $this->groupName(),
        )));
    }

    public function testTeardownReplicationRethrowsWhenReplicaDropIsNeverVisible(): void
    {
        $cleanup = $this->createCrossRegionCleanup(function (string $sql): void {
            throw new RuntimeException(self::STILL_PRIMARY_ERROR);
        });

        try {
            $cleanup->teardownReplication($this->sleeper());
            self::fail('Exception expected.');
        } catch (RuntimeException $e) {
            self::assertSame(self::STILL_PRIMARY_ERROR, $e->getMessage());
        }
        self::assertCount(10, $this->sourceQueries);
        self::assertCount(9, $this->sleeps);
    }

    public function testTeardownReplicationDoesNotRetryOtherErrors(): void
    {
        $error = new RuntimeException('SQL error: Insufficient privileges to operate on replication group');
        $cleanup = $this->createCrossRegionCleanup(function (string $sql) use ($error): void {
            throw $error;
        });

        try {
            $cleanup->teardownReplication($this->sleeper());
            self::fail('Exception expected.');
        } catch (RuntimeException $e) {
            self::assertSame($error, $e);
        }
        self::assertSame([], $this->sleeps);
    }

    /**
     * @param callable(string):void $sourceQuery
     */
    private function createCrossRegionCleanup(callable $sourceQuery): Cleanup
    {
        $source = $this->createConnection('AWS_EU_CENTRAL_1');
        $source->expects(self::atLeastOnce())
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($sourceQuery): void {
                $this->sourceQueries[] = $sql;
                $sourceQuery($sql);
            });

        $destination = $this->createConnection('AWS_US_EAST_1');
        $destination->expects(self::never())->method('query');

        $migrate = $this->createConnection(null);
        $migrate->expects(self::exactly(2))
            ->method('query')
            ->willReturnCallback(function (string $sql): void {
                self::assertContains($sql, [
                    ReplicationGroup::dropSql($this->groupName()),
                    sprintf('DROP DATABASE IF EXISTS "%s";', self::DATABASE),
                ]);
            });

        return new Cleanup($this->createConfig(), $source, $destination, $this->logger, $migrate);
    }

    private function createConfig(): Config
    {
        return new Config(['parameters' => [
            'action' => Config::ACTION_CLEANUP,
            'migrateDatabases' => [self::DATABASE],
            'credentials' => [
                'source' => [
                    'host' => 'source.snowflakecomputing.com',
                    'username' => 'MIGRATE_SOURCE_USER',
                    'warehouse' => 'WH',
                    'role' => 'MIGRATE_SOURCE',
                ],
                'target' => [
                    'host' => 'target.snowflakecomputing.com',
                    'username' => self::TARGET_USER,
                    'warehouse' => 'WH',
                    'role' => self::TARGET_ROLE,
                ],
            ],
        ]], new ConfigDefinition());
    }

    private function createConnection(?string $region): Connection&MockObject
    {
        $connection = $this->getMockBuilder(Connection::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['query', 'fetchAll'])
            ->getMock();
        $connection->expects($region === null ? self::never() : self::atLeastOnce())
            ->method('fetchAll')
            ->willReturnCallback(fn(string $sql): array => match ($sql) {
                'SELECT CURRENT_REGION() AS "region";' => [['region' => $region]],
                default => self::fail('Unexpected fetch: ' . $sql),
            });
        return $connection;
    }

    /**
     * @return callable(int):void
     */
    private function sleeper(): callable
    {
        return function (int $seconds): void {
            $this->sleeps[] = $seconds;
        };
    }

    private function groupName(): string
    {
        return ReplicationGroup::buildName([self::DATABASE]);
    }
}
