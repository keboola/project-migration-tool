<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Generator;
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
            . 'Retrying in 15 seconds (attempt 2 of 20).',
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
        self::assertCount(20, $this->sourceQueries);
        self::assertCount(19, $this->sleeps);
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
     * @param array<string, string> $databaseOwners database => owner role, as SHOW DATABASES on the target
     * @param array<string, string> $userRoles role => granted_by, in SHOW GRANTS TO USER order
     * @param string[] $expectedQueries
     * @param string[] $expectedInfo
     * @dataProvider postMigrationProvider
     */
    public function testPostMigrationRevokesOnlyRolesOfMigratedDatabases(
        array $databaseOwners,
        array $userRoles,
        array $expectedQueries,
        array $expectedInfo,
    ): void {
        $queries = [];
        $destination = $this->createConnectionMock();
        $destination->expects(self::atLeastOnce())
            ->method('fetchAll')
            ->willReturnCallback(fn(string $sql): array => match ($sql) {
                'SHOW DATABASES;' => array_map(
                    fn(string $name, string $owner) => ['name' => $name, 'owner' => $owner],
                    array_keys($databaseOwners),
                    $databaseOwners,
                ),
                'SHOW GRANTS TO USER "MIGRATE_TARGET_USER"' => array_map(
                    fn(int|string $role, string $grantedBy) => [
                        'role' => (string) $role,
                        'granted_to' => 'USER',
                        'grantee_name' => self::TARGET_USER,
                        'granted_by' => $grantedBy,
                    ],
                    array_keys($userRoles),
                    $userRoles,
                ),
                default => self::fail('Unexpected fetch: ' . $sql),
            });
        $destination->expects(self::any())
            ->method('query')
            ->willReturnCallback(function (string $sql) use (&$queries): void {
                $queries[] = $sql;
            });

        $source = $this->createConnectionMock();
        $source->expects(self::never())->method('query');
        $source->expects(self::never())->method('fetchAll');

        $cleanup = new Cleanup(
            $this->createConfig(['KEBOOLA_1', 'KEBOOLA_2']),
            $source,
            $destination,
            $this->logger,
        );
        $cleanup->postMigration();

        self::assertSame($expectedQueries, $queries);
        foreach ($expectedInfo as $message) {
            self::assertTrue($this->logger->hasInfoThatContains($message), 'Missing info: ' . $message);
        }
        self::assertFalse($this->logger->hasInfoThatContains('Warning'));
    }

    public static function postMigrationProvider(): Generator
    {
        $migratedProjects = [
            'MIGRATE_TARGET' => '',
            'KEBOOLA_STORAGE' => 'MIGRATE_TARGET',
            'KEBOOLA_1' => 'KEBOOLA_STORAGE',
            'KEBOOLA_1_RO' => 'KEBOOLA_1',
            'KEBOOLA_WORKSPACE_1' => 'KEBOOLA_1',
            'SAPI_2' => 'KEBOOLA_STORAGE',
            'SAPI_2_RO' => 'SAPI_2',
        ];
        $revokeMigratedProjects = [
            'USE ROLE "SAPI_2";',
            'REVOKE ROLE "SAPI_2_RO" FROM USER "MIGRATE_TARGET_USER"',
            'USE ROLE "KEBOOLA_STORAGE";',
            'REVOKE ROLE "SAPI_2" FROM USER "MIGRATE_TARGET_USER"',
            'USE ROLE "KEBOOLA_1";',
            'REVOKE ROLE "KEBOOLA_WORKSPACE_1" FROM USER "MIGRATE_TARGET_USER"',
            'REVOKE ROLE "KEBOOLA_1_RO" FROM USER "MIGRATE_TARGET_USER"',
            'USE ROLE "KEBOOLA_STORAGE";',
            'REVOKE ROLE "KEBOOLA_1" FROM USER "MIGRATE_TARGET_USER"',
        ];

        yield 'no other project in progress' => [
            'databaseOwners' => ['KEBOOLA_1' => 'KEBOOLA_1', 'KEBOOLA_2' => 'SAPI_2'],
            'userRoles' => $migratedProjects,
            'expectedQueries' => [
                ...$revokeMigratedProjects,
                'USE ROLE "MIGRATE_TARGET";',
                'REVOKE ROLE "KEBOOLA_STORAGE" FROM USER "MIGRATE_TARGET_USER"',
            ],
            'expectedInfo' => [],
        ];

        yield 'other project in progress keeps its roles and the main role' => [
            'databaseOwners' => ['KEBOOLA_1' => 'KEBOOLA_1', 'KEBOOLA_2' => 'SAPI_2', 'KEBOOLA_9' => 'KEBOOLA_9'],
            'userRoles' => $migratedProjects + [
                'KEBOOLA_9' => 'KEBOOLA_STORAGE',
                'KEBOOLA_9_RO' => 'KEBOOLA_9',
            ],
            'expectedQueries' => $revokeMigratedProjects,
            'expectedInfo' => [
                'Keeping roles of other projects on user "MIGRATE_TARGET_USER": KEBOOLA_9, KEBOOLA_9_RO.',
                'Keeping main role "KEBOOLA_STORAGE" on user "MIGRATE_TARGET_USER", '
                . 'because roles of other projects depend on it: KEBOOLA_9, KEBOOLA_9_RO.',
            ],
        ];

        yield 'role of unknown origin is kept' => [
            'databaseOwners' => ['KEBOOLA_1' => 'KEBOOLA_1', 'KEBOOLA_2' => 'SAPI_2'],
            'userRoles' => $migratedProjects + ['SOME_ADMIN_ROLE' => ''],
            'expectedQueries' => [
                ...$revokeMigratedProjects,
                'USE ROLE "MIGRATE_TARGET";',
                'REVOKE ROLE "KEBOOLA_STORAGE" FROM USER "MIGRATE_TARGET_USER"',
            ],
            'expectedInfo' => [
                'Keeping roles of other projects on user "MIGRATE_TARGET_USER": SOME_ADMIN_ROLE.',
            ],
        ];

        yield 'database without known owner keeps its roles and the main role' => [
            'databaseOwners' => ['KEBOOLA_1' => 'KEBOOLA_1'],
            'userRoles' => $migratedProjects,
            'expectedQueries' => [
                'USE ROLE "KEBOOLA_1";',
                'REVOKE ROLE "KEBOOLA_WORKSPACE_1" FROM USER "MIGRATE_TARGET_USER"',
                'REVOKE ROLE "KEBOOLA_1_RO" FROM USER "MIGRATE_TARGET_USER"',
                'USE ROLE "KEBOOLA_STORAGE";',
                'REVOKE ROLE "KEBOOLA_1" FROM USER "MIGRATE_TARGET_USER"',
            ],
            'expectedInfo' => [
                'The owner role of database "KEBOOLA_2" was not found on the target account, '
                . 'its roles are kept on user "MIGRATE_TARGET_USER".',
                'Keeping roles of other projects on user "MIGRATE_TARGET_USER": SAPI_2, SAPI_2_RO.',
                'Keeping main role "KEBOOLA_STORAGE" on user "MIGRATE_TARGET_USER", '
                . 'because roles of other projects depend on it: SAPI_2, SAPI_2_RO.',
            ],
        ];

        yield 'database still owned by main role' => [
            'databaseOwners' => ['KEBOOLA_1' => 'KEBOOLA_STORAGE', 'KEBOOLA_2' => 'SAPI_2', 'KEBOOLA_9' => 'KEBOOLA_9'],
            'userRoles' => $migratedProjects + [
                'KEBOOLA_9' => 'KEBOOLA_STORAGE',
                'KEBOOLA_9_RO' => 'KEBOOLA_9',
            ],
            'expectedQueries' => [
                'USE ROLE "SAPI_2";',
                'REVOKE ROLE "SAPI_2_RO" FROM USER "MIGRATE_TARGET_USER"',
                'USE ROLE "KEBOOLA_STORAGE";',
                'REVOKE ROLE "SAPI_2" FROM USER "MIGRATE_TARGET_USER"',
            ],
            'expectedInfo' => [
                'Database "KEBOOLA_1" is still owned by the main role "KEBOOLA_STORAGE", '
                . 'so its roles are kept on user "MIGRATE_TARGET_USER". Please check them manually.',
                'Keeping roles of other projects on user "MIGRATE_TARGET_USER": '
                . 'KEBOOLA_1, KEBOOLA_1_RO, KEBOOLA_WORKSPACE_1, KEBOOLA_9, KEBOOLA_9_RO.',
                'Keeping main role "KEBOOLA_STORAGE" on user "MIGRATE_TARGET_USER", because roles of other '
                . 'projects depend on it: KEBOOLA_1, KEBOOLA_1_RO, KEBOOLA_WORKSPACE_1, KEBOOLA_9, KEBOOLA_9_RO.',
            ],
        ];

        yield 'all-digit role names' => [
            'databaseOwners' => ['KEBOOLA_1' => 'KEBOOLA_1', 'KEBOOLA_2' => '123'],
            'userRoles' => [
                'MIGRATE_TARGET' => '',
                'KEBOOLA_STORAGE' => 'MIGRATE_TARGET',
                'KEBOOLA_1' => 'KEBOOLA_STORAGE',
                '123' => 'KEBOOLA_STORAGE',
                '456' => '123',
            ],
            'expectedQueries' => [
                'USE ROLE "123";',
                'REVOKE ROLE "456" FROM USER "MIGRATE_TARGET_USER"',
                'USE ROLE "KEBOOLA_STORAGE";',
                'REVOKE ROLE "123" FROM USER "MIGRATE_TARGET_USER"',
                'REVOKE ROLE "KEBOOLA_1" FROM USER "MIGRATE_TARGET_USER"',
                'USE ROLE "MIGRATE_TARGET";',
                'REVOKE ROLE "KEBOOLA_STORAGE" FROM USER "MIGRATE_TARGET_USER"',
            ],
            'expectedInfo' => [],
        ];
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

    private function createConnectionMock(): Connection&MockObject
    {
        return $this->getMockBuilder(Connection::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['query', 'fetchAll'])
            ->getMock();
    }

    /**
     * @param string[] $databases
     */
    private function createConfig(array $databases = [self::DATABASE]): Config
    {
        return new Config(['parameters' => [
            'action' => Config::ACTION_CLEANUP,
            'migrateDatabases' => $databases,
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
        $connection = $this->createConnectionMock();
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
