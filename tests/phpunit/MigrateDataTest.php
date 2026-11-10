<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Generator;
use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\MigrateData;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Tests\Snowflake\TestLogger;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\ProjectRoles;
use ProjectMigrationTool\ValueObject\Role;
use ProjectMigrationTool\ValueObject\RoleGrants;
use Throwable;

class MigrateDataTest extends TestCase
{
    private const OWNER_ROLE = 'DB_OWNER';

    private const SHOW_SCHEMAS_SQL = 'SHOW SCHEMAS IN DATABASE "DB_SHARE";';
    private const SHOW_SHARE_TABLES_SQL = 'SHOW TABLES IN SCHEMA "DB_SHARE"."S";';
    private const SHOW_TARGET_TABLES_SQL = 'SHOW TABLES IN SCHEMA "DB"."S";';
    private const SHOW_OLD_TABLES_SQL = 'SHOW TABLES IN SCHEMA "DB_OLD"."S";';
    private const SHOW_OLD_DATABASE_SQL = 'SHOW DATABASES LIKE \'DB_OLD\'';

    private const TRUNCATE_SQL = 'TRUNCATE TABLE "DB"."S"."T";';
    private const INSERT_SQL = 'INSERT INTO "DB"."S"."T" SELECT * FROM "DB_SHARE"."S"."T";';
    private const CLONE_SQLS = [
        'grant usage on database "DB_OLD" to role "DB_OWNER";',
        'grant usage on schema "DB_OLD"."S" to role "DB_OWNER";',
        'grant select on table "DB_OLD"."S"."T" to role "DB_OWNER";',
        'DROP TABLE "DB"."S"."T"',
        'CREATE TABLE "DB"."S"."T" CLONE "DB_OLD"."S"."T";',
    ];

    /** @var list<string> */
    private array $queries = [];

    private TestLogger $logger;

    protected function setUp(): void
    {
        parent::setUp();
        $this->queries = [];
        $this->logger = new TestLogger();
    }

    public function testSynchronizeSkipsTableWithSameTimestampAndRowCount(): void
    {
        $this->runMigration(
            self::synchronizeResults(shareRows: '5', targetRows: '5', oldRows: null),
            ['synchronize' => true],
        );

        self::assertSame([], $this->queries);
        self::assertTrue($this->logger->hasInfoThatContains('Table "DB.S.T": skip'));
    }

    public function testSynchronizeCopiesTableWithSameTimestampButFewerRows(): void
    {
        $this->runMigration(
            self::synchronizeResults(shareRows: '5', targetRows: '3', oldRows: null),
            ['synchronize' => true],
        );

        self::assertSame([self::TRUNCATE_SQL, self::INSERT_SQL], $this->queries);
        self::assertTrue($this->logger->hasInfoThatContains('Table "DB.S.T": copy'));
    }

    public function testSynchronizeCopiesTableWithDifferentTimestamp(): void
    {
        $this->runMigration(
            self::synchronizeResults(shareRows: '5', targetRows: '5', oldRows: null, targetTimestamp: '2020-01-01'),
            ['synchronize' => true],
        );

        self::assertSame([self::TRUNCATE_SQL, self::INSERT_SQL], $this->queries);
    }

    public function testSynchronizeClonesTableWithSameTimestampAndRowCountAsOld(): void
    {
        $this->runMigration(
            self::synchronizeResults(shareRows: '5', targetRows: '3', oldRows: '5'),
            ['synchronize' => true],
        );

        self::assertSame(self::CLONE_SQLS, $this->queries);
        self::assertTrue($this->logger->hasInfoThatContains('Table "DB.S.T": clone'));
    }

    public function testSynchronizeDoesNotCloneTableWithDifferentRowCountThanOld(): void
    {
        $this->runMigration(
            self::synchronizeResults(shareRows: '5', targetRows: '3', oldRows: '4'),
            ['synchronize' => true],
        );

        self::assertSame([self::TRUNCATE_SQL, self::INSERT_SQL], $this->queries);
    }

    public function testSynchronizeUsesTimestampOnlyWhenTargetRowCountIsMissing(): void
    {
        $this->runMigration(
            self::synchronizeResults(shareRows: '5', targetRows: '', oldRows: null),
            ['synchronize' => true],
        );

        self::assertSame([], $this->queries);
        self::assertTrue($this->logger->hasInfoThatContains('row count is unknown'));
    }

    public function testSynchronizeUsesTimestampOnlyWhenOldRowCountIsMissing(): void
    {
        $results = self::synchronizeResults(shareRows: '5', targetRows: '3', oldRows: '5');
        $results[self::SHOW_OLD_TABLES_SQL] = [['name' => 'T', 'rows' => null]];

        $this->runMigration($results, ['synchronize' => true]);

        self::assertSame(self::CLONE_SQLS, $this->queries);
        self::assertTrue($this->logger->hasInfoThatContains('row count is unknown'));
    }

    public function testSynchronizeDoesNotCloneWhenTableIsNotInOld(): void
    {
        $results = self::synchronizeResults(shareRows: '5', targetRows: '3', oldRows: '5');
        $results[self::SHOW_OLD_TABLES_SQL] = [['name' => 'OTHER', 'rows' => '5']];
        unset($results[self::timestampSql('DB_OLD')]);

        $this->runMigration($results, ['synchronize' => true]);

        self::assertSame([self::TRUNCATE_SQL, self::INSERT_SQL], $this->queries);
    }

    public function testSynchronizeTreatsFailedTimestampQueryAsChanged(): void
    {
        $results = self::synchronizeResults(shareRows: '5', targetRows: '5', oldRows: null);
        $results[self::timestampSql('DB')] = new RuntimeException('invalid identifier "_timestamp"');

        $this->runMigration($results, ['synchronize' => true]);

        self::assertSame([self::TRUNCATE_SQL, self::INSERT_SQL], $this->queries);
    }

    public function testSynchronizeWithMissingOldDatabaseDoesNotFail(): void
    {
        $results = self::synchronizeResults(shareRows: '5', targetRows: '5', oldRows: null);
        self::assertInstanceOf(Throwable::class, $results[self::SHOW_OLD_TABLES_SQL]);
        self::assertArrayNotHasKey(self::timestampSql('DB_OLD'), $results);

        $this->runMigration($results, ['synchronize' => true]);

        self::assertSame([], $this->queries);
    }

    /**
     * @param list<string> $expectedQueries
     * @dataProvider notSynchronizeProvider
     */
    public function testNotSynchronizeRun(?string $targetRows, ?string $shareRows, array $expectedQueries): void
    {
        $this->runMigration([
            self::SHOW_SCHEMAS_SQL => [['name' => 'S']],
            self::SHOW_SHARE_TABLES_SQL => [['name' => 'T', 'rows' => $shareRows]],
            self::SHOW_TARGET_TABLES_SQL => [['name' => 'T', 'rows' => $targetRows]],
        ]);

        self::assertSame($expectedQueries, $this->queries);
    }

    public static function notSynchronizeProvider(): Generator
    {
        yield 'empty target, non-empty share' => [
            'targetRows' => '0',
            'shareRows' => '5',
            'expectedQueries' => [self::INSERT_SQL],
        ];
        yield 'empty target, empty share' => [
            'targetRows' => '0',
            'shareRows' => '0',
            'expectedQueries' => [],
        ];
        yield 'empty target, unknown share row count' => [
            'targetRows' => '0',
            'shareRows' => null,
            'expectedQueries' => [self::INSERT_SQL],
        ];
        yield 'non-empty target' => [
            'targetRows' => '3',
            'shareRows' => '5',
            'expectedQueries' => [],
        ];
        yield 'unknown target row count' => [
            'targetRows' => null,
            'shareRows' => '5',
            'expectedQueries' => [],
        ];
    }

    /**
     * @param list<GrantToRole> $tableGrants
     * @dataProvider invalidTableProvider
     */
    public function testInvalidTableFailsWithoutSkipCheck(
        array $tableGrants,
        bool $withWarehouse,
        string $expectedMessage,
    ): void {
        $this->expectException(UserException::class);
        $this->expectExceptionMessage($expectedMessage);

        $this->runMigration(
            [
                self::SHOW_SCHEMAS_SQL => [['name' => 'S']],
                self::SHOW_SHARE_TABLES_SQL => [['name' => 'T', 'rows' => '5']],
                self::SHOW_TARGET_TABLES_SQL => [['name' => 'T', 'rows' => '0']],
            ],
            [],
            self::createProjectRoles($tableGrants, $withWarehouse),
        );
    }

    public static function invalidTableProvider(): Generator
    {
        yield 'no ownership grant' => [
            'tableGrants' => [self::createTableGrant('DB.S.T', 'SELECT')],
            'withWarehouse' => true,
            'expectedMessage' => 'Table "DB.S.T" has no OWNERSHIP grant.',
        ];
        yield 'more than one ownership grant' => [
            'tableGrants' => [
                self::createTableGrant('DB.S.T', 'OWNERSHIP'),
                self::createTableGrant('"DB"."S"."T"', 'OWNERSHIP'),
            ],
            'withWarehouse' => true,
            'expectedMessage' => 'Table "DB.S.T" has 2 OWNERSHIP grants, expected one.',
        ];
        yield 'no warehouse grant' => [
            'tableGrants' => [self::createTableGrant('DB.S.T', 'OWNERSHIP')],
            'withWarehouse' => false,
            'expectedMessage' => 'Owner role "DB_OWNER" of table "DB.S.T" has no warehouse grant.',
        ];
    }

    public function testInvalidTableIsSkippedWithSkipCheck(): void
    {
        $this->runMigration(
            [
                self::SHOW_SCHEMAS_SQL => [['name' => 'S']],
                self::SHOW_SHARE_TABLES_SQL => [
                    ['name' => 'BROKEN', 'rows' => '5'],
                    ['name' => 'T', 'rows' => '5'],
                ],
                self::SHOW_TARGET_TABLES_SQL => [
                    ['name' => 'BROKEN', 'rows' => '0'],
                    ['name' => 'T', 'rows' => '0'],
                ],
            ],
            ['skipCheck' => true],
            self::createProjectRoles([self::createTableGrant('DB.S.T', 'OWNERSHIP')]),
        );

        self::assertSame([self::INSERT_SQL], $this->queries);
        self::assertSame(
            [
                'Skipping table "DB.S.BROKEN": Table "DB.S.BROKEN" has no OWNERSHIP grant.',
                'Skipped 1 table(s) because of failed checks: DB.S.BROKEN',
            ],
            $this->getWarnings(),
        );
    }

    public function testSkippedTablesSummaryIsCapped(): void
    {
        $shareTables = [];
        $targetTables = [];
        for ($i = 1; $i <= 52; $i++) {
            $shareTables[] = ['name' => 'T' . $i, 'rows' => '5'];
            $targetTables[] = ['name' => 'T' . $i, 'rows' => '0'];
        }

        $this->runMigration(
            [
                self::SHOW_SCHEMAS_SQL => [['name' => 'S']],
                self::SHOW_SHARE_TABLES_SQL => $shareTables,
                self::SHOW_TARGET_TABLES_SQL => $targetTables,
            ],
            ['skipCheck' => true],
            self::createProjectRoles([]),
        );

        $warnings = $this->getWarnings();
        self::assertCount(53, $warnings);
        $summary = (string) end($warnings);
        self::assertStringStartsWith('Skipped 52 table(s) because of failed checks: DB.S.T1, DB.S.T2,', $summary);
        self::assertStringEndsWith('DB.S.T50 and 2 more', $summary);
    }

    /**
     * @dataProvider synchronizeDecisionProvider
     */
    public function testDecideSynchronizeAction(
        bool $sameTimestampAsOld,
        ?int $oldRows,
        bool $sameTimestampAsTarget,
        ?int $targetRows,
        ?int $shareRows,
        string $expectedAction,
    ): void {
        $decision = MigrateData::decideSynchronizeAction(
            $sameTimestampAsOld,
            $oldRows,
            $sameTimestampAsTarget,
            $targetRows,
            $shareRows,
        );

        self::assertSame($expectedAction, $decision['action']);
        self::assertNotSame('', $decision['reason']);
    }

    public static function synchronizeDecisionProvider(): Generator
    {
        yield 'same as old' => [
            'sameTimestampAsOld' => true,
            'oldRows' => 5,
            'sameTimestampAsTarget' => true,
            'targetRows' => 5,
            'shareRows' => 5,
            'expectedAction' => MigrateData::SYNC_CLONE,
        ];
        yield 'same timestamp as old, different rows, same as target' => [
            'sameTimestampAsOld' => true,
            'oldRows' => 4,
            'sameTimestampAsTarget' => true,
            'targetRows' => 5,
            'shareRows' => 5,
            'expectedAction' => MigrateData::SYNC_SKIP,
        ];
        yield 'same timestamp as old, unknown old rows' => [
            'sameTimestampAsOld' => true,
            'oldRows' => null,
            'sameTimestampAsTarget' => false,
            'targetRows' => 5,
            'shareRows' => 5,
            'expectedAction' => MigrateData::SYNC_CLONE,
        ];
        yield 'same as target' => [
            'sameTimestampAsOld' => false,
            'oldRows' => null,
            'sameTimestampAsTarget' => true,
            'targetRows' => 0,
            'shareRows' => 0,
            'expectedAction' => MigrateData::SYNC_SKIP,
        ];
        yield 'same timestamp as target, rows deleted in share' => [
            'sameTimestampAsOld' => false,
            'oldRows' => null,
            'sameTimestampAsTarget' => true,
            'targetRows' => 5,
            'shareRows' => 4,
            'expectedAction' => MigrateData::SYNC_COPY,
        ];
        yield 'same timestamp as target, unknown share rows' => [
            'sameTimestampAsOld' => false,
            'oldRows' => null,
            'sameTimestampAsTarget' => true,
            'targetRows' => 5,
            'shareRows' => null,
            'expectedAction' => MigrateData::SYNC_SKIP,
        ];
        yield 'different timestamp' => [
            'sameTimestampAsOld' => false,
            'oldRows' => 5,
            'sameTimestampAsTarget' => false,
            'targetRows' => 5,
            'shareRows' => 5,
            'expectedAction' => MigrateData::SYNC_COPY,
        ];
    }

    /**
     * @param array<string, list<array<string, string|null>>|Throwable> $fetchResults
     * @param array<string, bool> $parameters
     */
    private function runMigration(
        array $fetchResults,
        array $parameters = [],
        ?ProjectRoles $projectRoles = null,
    ): void {
        $sourceConnection = $this->createMock(Connection::class);
        $sourceConnection->expects(self::once())->method('useRole')->with('MIGRATION_SOURCE');
        $sourceConnection->expects(self::once())
            ->method('getOwnershipRoleOnDatabase')
            ->with('DB')
            ->willReturn(self::OWNER_ROLE);

        $config = new Config(
            ['parameters' => ['action' => Config::ACTION_MIGRATE_DATA, 'migrateDatabases' => ['DB']] + $parameters],
            new ConfigDefinition(),
        );

        $migrateData = new MigrateData(
            $sourceConnection,
            $this->createDestinationConnection($fetchResults),
            $this->logger,
            $config,
            'MIGRATION_SOURCE',
            'MIGRATION_TARGET',
            ['DB'],
        );

        $migrateData->migrate(
            new Role('MAIN', 'ACCOUNTADMIN'),
            [self::OWNER_ROLE => $projectRoles ?? self::createProjectRoles([
                self::createTableGrant('DB.S.T', 'OWNERSHIP'),
                self::createTableGrant('DB.S.T', 'SELECT', 'READER'),
            ])],
        );
    }

    /**
     * @param array<string, list<array<string, string|null>>|Throwable> $fetchResults
     */
    private function createDestinationConnection(array $fetchResults): Connection
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::any())
            ->method('fetchAll')
            ->willReturnCallback(function (string $sql) use ($fetchResults): array {
                self::assertArrayHasKey($sql, $fetchResults, 'Unexpected fetchAll: ' . $sql);
                $result = $fetchResults[$sql];
                if ($result instanceof Throwable) {
                    throw $result;
                }
                return $result;
            });
        $connection->expects(self::any())
            ->method('query')
            ->willReturnCallback(function (string $sql): void {
                $this->queries[] = $sql;
            });
        $connection->expects(self::any())->method('getCurrentRole')->willReturn(self::OWNER_ROLE);
        $connection->expects(self::any())->method('useRole');
        $connection->expects(self::any())->method('useWarehouse')->with('WH_SMALL');
        $connection->expects(self::any())->method('assignGrantToRole');
        return $connection;
    }

    /**
     * Max timestamps are equal in all databases unless overridden.
     * $oldRows null means the OLD database does not exist.
     *
     * @return array<string, list<array<string, string|null>>|Throwable>
     */
    private static function synchronizeResults(
        string $shareRows,
        string $targetRows,
        ?string $oldRows,
        string $targetTimestamp = '2024-01-01',
    ): array {
        $results = [
            self::SHOW_SCHEMAS_SQL => [['name' => 'S']],
            self::SHOW_SHARE_TABLES_SQL => [['name' => 'T', 'rows' => $shareRows]],
            self::SHOW_TARGET_TABLES_SQL => [['name' => 'T', 'rows' => $targetRows]],
            self::timestampSql('DB_SHARE') => [['maxTimestamp' => '2024-01-01']],
            self::timestampSql('DB') => [['maxTimestamp' => $targetTimestamp]],
        ];
        if ($oldRows === null) {
            $results[self::SHOW_OLD_TABLES_SQL] = new RuntimeException('Database "DB_OLD" does not exist');
        } else {
            $results[self::SHOW_OLD_TABLES_SQL] = [['name' => 'T', 'rows' => $oldRows]];
            $results[self::timestampSql('DB_OLD')] = [['maxTimestamp' => '2024-01-01']];
            $results[self::SHOW_OLD_DATABASE_SQL] = [['name' => 'DB_OLD']];
        }
        return $results;
    }

    private static function timestampSql(string $database): string
    {
        return sprintf('SELECT max("_timestamp") as "maxTimestamp" FROM "%s"."S"."T"', $database);
    }

    /**
     * @param list<GrantToRole> $tableGrants
     */
    private static function createProjectRoles(array $tableGrants, bool $withWarehouse = true): ProjectRoles
    {
        $ownerGrants = new RoleGrants();
        $readerGrants = new RoleGrants();
        foreach ($tableGrants as $grant) {
            if ($grant->getGranteeName() === self::OWNER_ROLE) {
                $ownerGrants->addTableGrant($grant);
            } else {
                $readerGrants->addTableGrant($grant);
            }
        }
        if ($withWarehouse) {
            $ownerGrants->addWarehouseGrant(
                new GrantToRole('WH_SMALL', 'USAGE', 'WAREHOUSE', 'ROLE', self::OWNER_ROLE, 'false', 'ACCOUNTADMIN'),
                'WH',
                'WH',
            );
        }

        $owner = new Role(self::OWNER_ROLE, 'ACCOUNTADMIN');
        $owner->setGrants($ownerGrants);
        $reader = new Role('READER', 'ACCOUNTADMIN');
        $reader->setGrants($readerGrants);

        $projectRoles = new ProjectRoles();
        $projectRoles->addRole($owner);
        $projectRoles->addRole($reader);
        return $projectRoles;
    }

    private static function createTableGrant(
        string $name,
        string $privilege,
        string $roleName = self::OWNER_ROLE,
    ): GrantToRole {
        return new GrantToRole($name, $privilege, 'TABLE', 'ROLE', $roleName, 'false', self::OWNER_ROLE);
    }

    /**
     * @return list<string>
     */
    private function getWarnings(): array
    {
        return array_values(array_map(
            fn(array $record) => $record['message'],
            array_filter($this->logger->records, fn(array $record) => $record['level'] === 'warning'),
        ));
    }
}
