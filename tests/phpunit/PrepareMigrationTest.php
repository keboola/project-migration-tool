<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\PrepareMigration;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\ReplicationGroup;
use ProjectMigrationTool\Tests\Snowflake\TestLogger;

class PrepareMigrationTest extends TestCase
{
    private const DATABASE = 'KEBOOLA_1';
    private const NOT_VISIBLE_ERROR = 'SQL error: Replication group \'ORG.SOURCE.MIGRATION_RG_X\' '
        . 'does not exist or is not authorized., SQL state 55000';

    private TestLogger $logger;

    /** @var string[] */
    private array $migrateQueries = [];

    /** @var int[] */
    private array $sleeps = [];

    protected function setUp(): void
    {
        $this->logger = new TestLogger();
    }

    public function testCreateReplicationRetriesReplicaUntilPrimaryIsVisible(): void
    {
        $failures = 2;
        $this->createPrepareMigration(function (string $sql) use (&$failures): void {
            if (str_contains($sql, 'AS REPLICA OF') && $failures > 0) {
                $failures--;
                throw new RuntimeException(self::NOT_VISIBLE_ERROR);
            }
        })->createReplication($this->sleeper());

        $replicaSql = ReplicationGroup::createReplicaSql($this->groupName(), 'ORG', 'SOURCE');
        self::assertSame(
            [$replicaSql, $replicaSql, $replicaSql],
            array_values(array_filter($this->migrateQueries, fn(string $sql) => $sql === $replicaSql)),
        );
        self::assertSame([15, 15], $this->sleeps);
        self::assertTrue($this->logger->hasInfoThatContains(sprintf(
            'Primary replication group "%s" is not visible on the migrate account yet. '
            . 'Retrying in 15 seconds (attempt 2 of 10).',
            $this->groupName(),
        )));
        self::assertContains(ReplicationGroup::refreshSql($this->groupName()), $this->migrateQueries);
    }

    public function testCreateReplicationAddsHintWhenPrimaryNeverBecomesVisible(): void
    {
        $prepareMigration = $this->createPrepareMigration(function (string $sql): void {
            if (str_contains($sql, 'AS REPLICA OF')) {
                throw new RuntimeException(self::NOT_VISIBLE_ERROR);
            }
        });

        try {
            $prepareMigration->createReplication($this->sleeper());
            self::fail('Exception expected.');
        } catch (UserException $e) {
            self::assertStringStartsWith(self::NOT_VISIBLE_ERROR, $e->getMessage());
            self::assertStringContainsString(
                'The migrate account may not be enabled for replication with the source account.',
                $e->getMessage(),
            );
        }
        self::assertCount(9, $this->sleeps);
        self::assertNotContains(ReplicationGroup::refreshSql($this->groupName()), $this->migrateQueries);
    }

    public function testCreateReplicationDoesNotRetryOtherErrors(): void
    {
        $error = new RuntimeException('SQL error: Insufficient privileges to operate on account');
        $prepareMigration = $this->createPrepareMigration(function (string $sql) use ($error): void {
            if (str_contains($sql, 'AS REPLICA OF')) {
                throw $error;
            }
        });

        try {
            $prepareMigration->createReplication($this->sleeper());
            self::fail('Exception expected.');
        } catch (RuntimeException $e) {
            self::assertSame($error, $e);
        }
        self::assertSame([], $this->sleeps);
    }

    /**
     * @param callable(string):void $migrateQuery
     */
    private function createPrepareMigration(callable $migrateQuery): PrepareMigration
    {
        $config = new Config(['parameters' => [
            'action' => Config::ACTION_MIGRATE_STRUCTURE,
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
                    'username' => 'MIGRATE_TARGET_USER',
                    'warehouse' => 'WH',
                    'role' => 'MIGRATE_TARGET',
                ],
            ],
        ]], new ConfigDefinition());

        $source = $this->createConnection('AWS_EU_CENTRAL_1', 'SOURCE');
        $source->expects(self::once())->method('query');

        $destination = $this->createConnection('AWS_US_EAST_1', 'TARGET');
        $destination->expects(self::never())->method('query');

        $migrate = $this->createConnection('AWS_US_EAST_1', 'MIGRATE', [
            ReplicationGroup::refreshProgressSql($this->groupName()) => [
                ['PHASE_NAME' => 'COMPLETED', 'PROGRESS' => '100'],
            ],
        ]);
        $migrate->expects(self::atLeastOnce())
            ->method('query')
            ->willReturnCallback(function (string $sql) use ($migrateQuery): void {
                $this->migrateQueries[] = $sql;
                $migrateQuery($sql);
            });

        return new PrepareMigration(
            $config,
            [self::DATABASE],
            $source,
            $destination,
            $this->logger,
            $migrate,
        );
    }

    /**
     * @param array<string, array<int, array<string, string>>> $otherRows
     */
    private function createConnection(string $region, string $accountName, array $otherRows = []): Connection&MockObject
    {
        $connection = $this->getMockBuilder(Connection::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['query', 'fetchAll'])
            ->getMock();
        $connection->expects(self::atLeastOnce())
            ->method('fetchAll')
            ->willReturnCallback(fn(string $sql): array => match ($sql) {
                'SELECT CURRENT_REGION() AS "region";' => [['region' => $region]],
                'SELECT CURRENT_ORGANIZATION_NAME() AS "organizationName";' => [['organizationName' => 'ORG']],
                'SELECT CURRENT_ACCOUNT_NAME() AS "accountName";' => [['accountName' => $accountName]],
                default => $otherRows[$sql] ?? self::fail('Unexpected fetch: ' . $sql),
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
