<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Generator;
use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\MigrateStructure;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Tests\Snowflake\TestLogger;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\ProjectRoles;
use ProjectMigrationTool\ValueObject\Role;
use ProjectMigrationTool\ValueObject\RoleFutureGrants;
use ProjectMigrationTool\ValueObject\RoleGrants;
use ReflectionProperty;

class MigrateStructureTest extends TestCase
{
    private const MAIN_ROLE = 'KEBOOLA_STORAGE';
    private const DATABASE = 'KEBOOLA_1';
    private const SOURCE_ROLE = 'MIGRATE_SOURCE';
    private const TARGET_ROLE = 'MIGRATE_TARGET';
    private const TARGET_USER = 'MIGRATE_TARGET_USER';
    private const WAREHOUSE = 'KEBOOLA_PROD';

    private const ALL_OBJECTS = [
        'ROLE "KEBOOLA_STORAGE"',
        'WAREHOUSE "KEBOOLA_PROD"',
        'USER KEBOOLA_STORAGE',
        'USER KEBOOLA_1',
        'ROLE "KEBOOLA_1"',
        'USER KEBOOLA_1_RO',
        'DATABASE "KEBOOLA_1"',
        'SCHEMA "KEBOOLA_1"."in.c-main"',
        'TABLE "KEBOOLA_1"."in.c-main"."t1"',
        'PRIMARY KEY "KEBOOLA_1"."in.c-main"."t1"',
        'TABLE "KEBOOLA_1"."in.c-main"."t2"',
        'SCHEMA "KEBOOLA_1"."out.c-main"',
        'TABLE "KEBOOLA_1"."out.c-main"."t3"',
    ];

    private const MAIN_ROLE_OBJECTS = [
        'ROLE "KEBOOLA_STORAGE"',
        'WAREHOUSE "KEBOOLA_PROD"',
        'USER KEBOOLA_STORAGE',
    ];

    /**
     * Statements sent by the code before the structure step became resumable.
     * The synchronize run must still send exactly these.
     */
    private const BASELINE = [
        'target fetch: SHOW ROLES LIKE \'KEBOOLA_STORAGE\'',
        'target: USE ROLE "MIGRATE_TARGET";',
        'target: CREATE ROLE IF NOT EXISTS "KEBOOLA_STORAGE"',
        'target: GRANT ROLE "KEBOOLA_STORAGE" TO USER "MIGRATE_TARGET_USER"',
        'target: GRANT CREATE DATABASE ON ACCOUNT TO ROLE KEBOOLA_STORAGE;',
        'target: GRANT CREATE ROLE ON ACCOUNT TO ROLE KEBOOLA_STORAGE WITH GRANT OPTION;',
        'target: GRANT CREATE USER ON ACCOUNT TO ROLE KEBOOLA_STORAGE WITH GRANT OPTION;',
        'source: USE ROLE "MIGRATE_SOURCE";',
        'source fetch: SHOW WAREHOUSES LIKE \'KEBOOLA_PROD\'',
        'target: CREATE WAREHOUSE IF NOT EXISTS "KEBOOLA_PROD"
    WITH WAREHOUSE_SIZE = \'Small\'
        WAREHOUSE_TYPE = \'STANDARD\'
        AUTO_SUSPEND = 60
        AUTO_RESUME = true
;',
        'target: GRANT USAGE ON WAREHOUSE KEBOOLA_PROD TO ROLE "KEBOOLA_STORAGE" WITH GRANT OPTION',
        'target: CREATE USER IF NOT EXISTS KEBOOLA_STORAGE PASSWORD=\'***\' DEFAULT_ROLE=KEBOOLA_STORAGE',
        'target: GRANT ROLE KEBOOLA_STORAGE TO USER KEBOOLA_STORAGE;',
        'target: GRANT ROLE KEBOOLA_STORAGE TO ROLE SYSADMIN;',
        'source fetch: SHOW GRANTS TO ROLE "MIGRATE_SOURCE"',
        'target: USE ROLE "KEBOOLA_STORAGE";',
        'source fetch: SHOW USERS LIKE \'KEBOOLA_1\'',
        'target: CREATE USER KEBOOLA_1 DEFAULT_ROLE = \'KEBOOLA_1\' TYPE = \'PERSON\' PASSWORD = \'***\'',
        'target: GRANT OWNERSHIP ON USER KEBOOLA_1 TO ROLE "KEBOOLA_STORAGE" WITH GRANT OPTION',
        'source fetch: SHOW GRANTS ON DATABASE "KEBOOLA_1";',
        'target: CREATE ROLE IF NOT EXISTS "KEBOOLA_1"',
        'target: GRANT ROLE "KEBOOLA_1" TO USER "MIGRATE_TARGET_USER"',
        'target: USE ROLE "KEBOOLA_1";',
        'source fetch: SHOW USERS LIKE \'KEBOOLA_1_RO\'',
        'target: CREATE USER KEBOOLA_1_RO DEFAULT_ROLE = \'KEBOOLA_1\' TYPE = \'PERSON\' PASSWORD = \'***\'',
        'source fetch: SHOW GRANTS ON DATABASE "KEBOOLA_1";',
        'target: USE ROLE "KEBOOLA_STORAGE";',
        'target: GRANT USAGE ON WAREHOUSE KEBOOLA_PROD TO ROLE "KEBOOLA_1" WITH GRANT OPTION',
        'target: USE ROLE "KEBOOLA_1";',
        'target: GRANT OWNERSHIP ON USER KEBOOLA_1_RO TO ROLE "KEBOOLA_1" WITH GRANT OPTION',
        'target: USE ROLE "MIGRATE_TARGET";',
        'source fetch: SHOW GRANTS TO USER "KEBOOLA_1"',
        'source fetch: SHOW GRANTS TO USER "KEBOOLA_1_RO"',
        'source fetch: SHOW GRANTS ON DATABASE "KEBOOLA_1";',
        'target: USE ROLE "KEBOOLA_STORAGE";',
        'source fetch: SHOW DATABASES LIKE \'KEBOOLA_1\'',
        'target: CREATE DATABASE "KEBOOLA_1" DATA_RETENTION_TIME_IN_DAYS=7;',
        'target: GRANT OWNERSHIP ON DATABASE KEBOOLA_1 TO ROLE "KEBOOLA_1" WITH GRANT OPTION',
        'target: USE ROLE "KEBOOLA_1";',
        'target: GRANT OWNERSHIP ON DATABASE KEBOOLA_1 TO ROLE "KEBOOLA_1" WITH GRANT OPTION',
        'target: USE ROLE "MIGRATE_TARGET";',
        'target: GRANT IMPORTED PRIVILEGES ON DATABASE "KEBOOLA_1_SHARE" TO ROLE "KEBOOLA_1" ',
        'target: USE ROLE "KEBOOLA_1";',
        'target fetch: SHOW SCHEMAS IN DATABASE "KEBOOLA_1_SHARE";',
        'target: CREATE TRANSIENT SCHEMA "KEBOOLA_1"."in.c-main"  DATA_RETENTION_TIME_IN_DAYS = 1;',
        'target: GRANT OWNERSHIP ON SCHEMA KEBOOLA_1."in.c-main" TO ROLE "KEBOOLA_1" WITH GRANT OPTION',
        'target fetch: SHOW TABLES IN SCHEMA "KEBOOLA_1_SHARE"."in.c-main";',
        'source fetch: SHOW PRIMARY KEYS IN SCHEMA "KEBOOLA_1"."in.c-main";',
        'target: USE ROLE "MIGRATE_TARGET";',
        'target: GRANT IMPORTED PRIVILEGES ON DATABASE "KEBOOLA_1_SHARE" TO ROLE "KEBOOLA_1" ',
        'target: USE ROLE "KEBOOLA_1";',
        'target: CREATE TABLE "KEBOOLA_1"."in.c-main"."t1" LIKE "KEBOOLA_1_SHARE"."in.c-main"."t1";',
        'target: GRANT SELECT ON TABLE KEBOOLA_1."in.c-main"."t1" TO ROLE "KEBOOLA_1" WITH GRANT OPTION',
        'target: ALTER TABLE "KEBOOLA_1"."in.c-main"."t1" ADD PRIMARY KEY ("id");',
        'target: USE ROLE "MIGRATE_TARGET";',
        'target: GRANT IMPORTED PRIVILEGES ON DATABASE "KEBOOLA_1_SHARE" TO ROLE "KEBOOLA_1" ',
        'target: USE ROLE "KEBOOLA_1";',
        'target: CREATE TABLE "KEBOOLA_1"."in.c-main"."t2" LIKE "KEBOOLA_1_SHARE"."in.c-main"."t2";',
        'target: CREATE  SCHEMA "KEBOOLA_1"."out.c-main" WITH MANAGED ACCESS DATA_RETENTION_TIME_IN_DAYS = 7;',
        'target: GRANT OWNERSHIP ON SCHEMA KEBOOLA_1."out.c-main" TO ROLE "KEBOOLA_1" WITH GRANT OPTION',
        'target fetch: SHOW TABLES IN SCHEMA "KEBOOLA_1_SHARE"."out.c-main";',
        'source fetch: SHOW PRIMARY KEYS IN SCHEMA "KEBOOLA_1"."out.c-main";',
        'target: USE ROLE "MIGRATE_TARGET";',
        'target: GRANT IMPORTED PRIVILEGES ON DATABASE "KEBOOLA_1_SHARE" TO ROLE "KEBOOLA_1" ',
        'target: USE ROLE "KEBOOLA_1";',
        'target: CREATE TABLE "KEBOOLA_1"."out.c-main"."t3" LIKE "KEBOOLA_1_SHARE"."out.c-main"."t3";',
        'source: GRANT ROLE "KEBOOLA_1" TO USER "MIGRATE_SOURCE_USER"',
        'source: USE ROLE "KEBOOLA_1";',
        'source fetch: SHOW VIEWS IN DATABASE "KEBOOLA_1";',
        'source fetch: SHOW FUNCTIONS IN DATABASE "KEBOOLA_1";',
        'target: USE DATABASE "KEBOOLA_1";',
        'source fetch: SHOW PROCEDURES IN DATABASE "KEBOOLA_1";',
    ];

    /** @var string[] */
    private array $log = [];

    /** @var string[] */
    private array $existingOnTarget = [];

    /** @var string[] */
    private array $preExistingOnTarget = [];

    /** @var string[] */
    private array $createdOnTarget = [];

    private ?string $targetRole = null;

    private bool $mainRoleGrantedToTargetUser = true;

    /** @var array<string, string> statement prefix => error message */
    private array $failingStatements = [];

    /** @var array<int, array<string, string>> */
    private array $sourceViews = [];

    /** @var array<string, string> view => view it selects from */
    private array $viewDependencies = [];

    private TestLogger $logger;

    public function testSynchronizeRunSendsSameStatementsAsBefore(): void
    {
        $this->runStructureStep(synchronize: true);

        self::assertSame(self::BASELINE, $this->log);
    }

    public function testCleanRunDiffersFromBeforeOnlyByExistenceChecks(): void
    {
        $this->runStructureStep(synchronize: false);

        self::assertContains(
            'target: CREATE DATABASE IF NOT EXISTS "KEBOOLA_1" DATA_RETENTION_TIME_IN_DAYS=7;',
            $this->log,
        );
        self::assertContains(
            'target: CREATE TRANSIENT SCHEMA IF NOT EXISTS "KEBOOLA_1"."in.c-main"  DATA_RETENTION_TIME_IN_DAYS = 1;',
            $this->log,
        );
        self::assertContains(
            'target: CREATE  SCHEMA IF NOT EXISTS "KEBOOLA_1"."out.c-main" WITH MANAGED ACCESS'
            . ' DATA_RETENTION_TIME_IN_DAYS = 7;',
            $this->log,
        );
        self::assertContains(
            "target: CREATE USER IF NOT EXISTS KEBOOLA_1_RO DEFAULT_ROLE = 'KEBOOLA_1' TYPE = 'PERSON'"
            . " PASSWORD = '***'",
            $this->log,
        );
        self::assertSame(self::BASELINE, $this->removeExistenceChecks($this->log));
        self::assertSame(self::ALL_OBJECTS, $this->createdOnTarget);
    }

    /**
     * @dataProvider existingObjectProvider
     * @param string[] $existingObjects
     */
    public function testExistingObjectIsNotCreatedAgainAndRunContinues(array $existingObjects): void
    {
        $this->existingOnTarget = $existingObjects;

        $this->runStructureStep(synchronize: false);

        self::assertSame(
            array_values(array_diff(self::ALL_OBJECTS, $existingObjects)),
            $this->createdOnTarget,
        );
        foreach ($this->log as $statement) {
            self::assertStringNotContainsString('OR REPLACE', $statement);
            self::assertStringNotContainsString('DROP', $statement);
        }
    }

    public static function existingObjectProvider(): Generator
    {
        yield 'nothing exists' => [[]];
        yield 'database exists' => [['DATABASE "KEBOOLA_1"']];
        yield 'first schema exists' => [['DATABASE "KEBOOLA_1"', 'SCHEMA "KEBOOLA_1"."in.c-main"']];
        yield 'project user exists' => [['USER KEBOOLA_1']];
        yield 'user owned by project role exists' => [['USER KEBOOLA_1_RO']];
        yield 'table and primary key exist' => [[
            'DATABASE "KEBOOLA_1"',
            'SCHEMA "KEBOOLA_1"."in.c-main"',
            'TABLE "KEBOOLA_1"."in.c-main"."t1"',
            'PRIMARY KEY "KEBOOLA_1"."in.c-main"."t1"',
        ]];
    }

    public function testExistingDatabaseIsReusedAndLogged(): void
    {
        $this->existingOnTarget = ['DATABASE "KEBOOLA_1"'];

        $this->runStructureStep(synchronize: false);

        self::assertTrue($this->logger->hasInfoThatContains(
            'Database "KEBOOLA_1" already exists on the target account, it will be reused.',
        ));
        self::assertNotContains('DATABASE "KEBOOLA_1"', $this->createdOnTarget);
    }

    public function testDatabaseWithSimilarNameIsNotTreatedAsExisting(): void
    {
        $this->existingOnTarget = ['DATABASE "KEBOOLAX1"'];

        $this->runStructureStep(synchronize: false);

        self::assertFalse($this->logger->hasInfoThatContains('already exists on the target account'));
        self::assertContains('DATABASE "KEBOOLA_1"', $this->createdOnTarget);
    }

    public function testExistingUserIsReusedAndLoggedAsWarning(): void
    {
        $this->existingOnTarget = ['USER KEBOOLA_1_RO'];

        $this->runStructureStep(synchronize: false);

        self::assertSame(
            ['User "KEBOOLA_1_RO" already exists on the target account, it will be reused.'],
            $this->warningsContaining('already exists on the target account'),
        );
        self::assertContains('USER KEBOOLA_1', $this->createdOnTarget);
        self::assertNotContains('USER KEBOOLA_1_RO', $this->createdOnTarget);
    }

    public function testCleanRunLogsNoReusedUser(): void
    {
        $this->runStructureStep(synchronize: false);

        self::assertSame([], $this->warningsContaining('already exists on the target account'));
    }

    public function testUserMatchingOnlyByWildcardOrCaseIsNotTreatedAsExisting(): void
    {
        $this->existingOnTarget = ['USER KEBOOLAX1_RO', 'USER keboola_1_ro'];

        $this->runStructureStep(synchronize: false);

        self::assertSame([], $this->warningsContaining('already exists on the target account'));
        self::assertContains('USER KEBOOLA_1_RO', $this->createdOnTarget);
        self::assertContains("target fetch: SHOW USERS LIKE 'KEBOOLA_1_RO'", $this->log);
    }

    public function testResumeWithExistingMainRoleSendsRemainingMainRoleStatements(): void
    {
        $this->existingOnTarget = ['ROLE "KEBOOLA_STORAGE"'];

        $this->runStructureStep(synchronize: false);

        foreach ([
            'target: CREATE ROLE IF NOT EXISTS "KEBOOLA_STORAGE"',
            'target: GRANT CREATE DATABASE ON ACCOUNT TO ROLE KEBOOLA_STORAGE;',
            'target: GRANT CREATE ROLE ON ACCOUNT TO ROLE KEBOOLA_STORAGE WITH GRANT OPTION;',
            'target: GRANT CREATE USER ON ACCOUNT TO ROLE KEBOOLA_STORAGE WITH GRANT OPTION;',
            'target: GRANT USAGE ON WAREHOUSE KEBOOLA_PROD TO ROLE "KEBOOLA_STORAGE" WITH GRANT OPTION',
            "target: CREATE USER IF NOT EXISTS KEBOOLA_STORAGE PASSWORD='***' DEFAULT_ROLE=KEBOOLA_STORAGE",
            'target: GRANT ROLE KEBOOLA_STORAGE TO USER KEBOOLA_STORAGE;',
            'target: GRANT ROLE KEBOOLA_STORAGE TO ROLE SYSADMIN;',
        ] as $statement) {
            self::assertContains($statement, $this->log);
        }
        self::assertSame(
            array_values(array_diff(self::ALL_OBJECTS, ['ROLE "KEBOOLA_STORAGE"'])),
            $this->createdOnTarget,
        );
    }

    public function testMainRoleNotGrantedToTargetUserStillFails(): void
    {
        $this->existingOnTarget = ['ROLE "KEBOOLA_STORAGE"'];
        $this->mainRoleGrantedToTargetUser = false;

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Main role exists but is not assigned to migrate user.');

        $this->runStructureStep(synchronize: false);
    }

    public function testSynchronizeRunWithExistingMainRoleSkipsMainRoleStatements(): void
    {
        $this->existingOnTarget = ['ROLE "KEBOOLA_STORAGE"'];

        $this->runStructureStep(synchronize: true);

        self::assertSame(
            [
                "target fetch: SHOW ROLES LIKE 'KEBOOLA_STORAGE'",
                'target fetch: SHOW GRANTS TO USER "MIGRATE_TARGET_USER"',
                'source fetch: SHOW GRANTS TO ROLE "MIGRATE_SOURCE"',
            ],
            array_slice($this->log, 0, 3),
        );
        self::assertNotContains('WAREHOUSE "KEBOOLA_PROD"', $this->createdOnTarget);
        self::assertNotContains('USER KEBOOLA_STORAGE', $this->createdOnTarget);
    }

    public function testExistingTableIsNotLoggedAsSkippedButRealErrorIs(): void
    {
        $this->existingOnTarget = [
            'DATABASE "KEBOOLA_1"',
            'SCHEMA "KEBOOLA_1"."in.c-main"',
            'TABLE "KEBOOLA_1"."in.c-main"."t1"',
        ];
        $this->failingStatements = [
            'CREATE TABLE IF NOT EXISTS "KEBOOLA_1"."out.c-main"."t3"' => 'Unsupported data type \'GEOMETRY\'',
        ];

        $this->runStructureStep(synchronize: false);

        self::assertContains(
            'target: CREATE TABLE IF NOT EXISTS "KEBOOLA_1"."in.c-main"."t1" LIKE "KEBOOLA_1_SHARE"."in.c-main"."t1";',
            $this->log,
        );
        self::assertFalse($this->logger->hasInfoThatContains('Skip creating table "KEBOOLA_1"."in.c-main"."t1"'));
        self::assertTrue($this->logger->hasInfoThatContains(
            'Warning: Skip creating table "KEBOOLA_1"."out.c-main"."t3". Error: "Unsupported data type \'GEOMETRY\'".',
        ));
    }

    public function testExistingViewIsNotRetriedOnResume(): void
    {
        $this->addSourceView('v1');
        $this->addSourceView('v2', 'v3');
        $this->addSourceView('v3');
        $this->existingOnTarget = ['VIEW ' . self::viewId('v1')];

        $this->runStructureStep(synchronize: false);

        self::assertSame(0, $this->countViewAttempts('v1'));
        self::assertContains('target fetch: SHOW VIEWS IN DATABASE "KEBOOLA_1";', $this->log);
        self::assertTrue($this->logger->hasInfoThatContains(
            'View "KEBOOLA_1"."in.c-main"."v1" already exists on the target account, it will be reused.',
        ));
        self::assertFalse($this->logger->hasInfoThatContains('Skip creating view "KEBOOLA_1"."in.c-main"."v1"'));
        self::assertSame(2, $this->countViewAttempts('v2'));
        self::assertSame(1, $this->countViewAttempts('v3'));
        self::assertSame(
            ['VIEW ' . self::viewId('v3'), 'VIEW ' . self::viewId('v2')],
            array_values(array_filter($this->createdOnTarget, fn(string $v) => str_starts_with($v, 'VIEW '))),
        );
    }

    public function testViewWithMissingDependencyIsStillRetriedFiveTimes(): void
    {
        $this->addSourceView('v4', 'missing');

        $this->runStructureStep(synchronize: false);

        self::assertSame(5, $this->countViewAttempts('v4'));
        self::assertTrue($this->logger->hasInfoThatContains('Skip creating view "KEBOOLA_1"."in.c-main"."v4"'));
        self::assertNotContains('VIEW ' . self::viewId('v4'), $this->createdOnTarget);
    }

    public function testSynchronizeRunDoesNotListTargetViews(): void
    {
        $this->addSourceView('v2', 'v3');
        $this->addSourceView('v3');

        $this->runStructureStep(synchronize: true);

        self::assertNotContains('target fetch: SHOW VIEWS IN DATABASE "KEBOOLA_1";', $this->log);
        self::assertSame(2, $this->countViewAttempts('v2'));
        self::assertSame(1, $this->countViewAttempts('v3'));
    }

    public function testResumeAfterFailureCreatesOnlyMissingObjects(): void
    {
        $this->existingOnTarget = [
            ...self::MAIN_ROLE_OBJECTS,
            'USER KEBOOLA_1',
            'ROLE "KEBOOLA_1"',
            'USER KEBOOLA_1_RO',
            'DATABASE "KEBOOLA_1"',
            'SCHEMA "KEBOOLA_1"."in.c-main"',
            'TABLE "KEBOOLA_1"."in.c-main"."t1"',
            'PRIMARY KEY "KEBOOLA_1"."in.c-main"."t1"',
        ];

        $this->runStructureStep(synchronize: false);

        self::assertSame(
            [
                'TABLE "KEBOOLA_1"."in.c-main"."t2"',
                'SCHEMA "KEBOOLA_1"."out.c-main"',
                'TABLE "KEBOOLA_1"."out.c-main"."t3"',
            ],
            $this->createdOnTarget,
        );
        self::assertTrue($this->logger->hasInfoThatContains(
            'Warning: Failed grant query "GRANT OWNERSHIP ON DATABASE KEBOOLA_1 TO ROLE "KEBOOLA_1" WITH GRANT OPTION"'
            . ' with role "KEBOOLA_STORAGE"',
        ));
    }

    private function runStructureStep(bool $synchronize): void
    {
        $this->logger = new TestLogger();
        $this->preExistingOnTarget = $this->existingOnTarget;

        $config = new Config(['parameters' => [
            'action' => Config::ACTION_MIGRATE_STRUCTURE,
            'migrateDatabases' => [self::DATABASE],
            'synchronize' => $synchronize,
            'credentials' => [
                'source' => [
                    'host' => 'source.snowflakecomputing.com',
                    'username' => 'MIGRATE_SOURCE_USER',
                    'warehouse' => self::WAREHOUSE,
                    'role' => self::SOURCE_ROLE,
                ],
                'target' => [
                    'host' => 'target.snowflakecomputing.com',
                    'username' => self::TARGET_USER,
                    'warehouse' => self::WAREHOUSE,
                    'role' => self::TARGET_ROLE,
                ],
            ],
        ]], new ConfigDefinition());

        $migrateStructure = new MigrateStructure(
            $this->createSourceConnection(),
            $this->createTargetConnection(),
            $this->logger,
            $config,
            self::SOURCE_ROLE,
            self::TARGET_ROLE,
            [self::DATABASE],
        );

        $mainRole = $this->createMainRole();
        $roles = [self::DATABASE => $this->createProjectRoles()];

        $migrateStructure->createMainRoleAndUser($mainRole);
        $migrateStructure->createProjectRoleAndUser($mainRole);
        $migrateStructure->migrateUsersRolesAndGrants($mainRole, $roles);
        $migrateStructure->grantRoleToUsers();
        $migrateStructure->cloneDatabaseWithGrants($mainRole, $roles);
        $migrateStructure->reApplyFailedGrants();
    }

    private function createSourceConnection(): Connection
    {
        $connection = $this->createConnectionMock();
        $connection->expects(self::any())
            ->method('getCurrentRole')
            ->willReturn(self::SOURCE_ROLE);
        $connection->expects(self::any())
            ->method('query')
            ->willReturnCallback(function (string $sql): void {
                $this->log[] = 'source: ' . $sql;
            });
        $connection->expects(self::any())
            ->method('fetchAll')
            ->willReturnCallback(function (string $sql): array {
                $this->log[] = 'source fetch: ' . $sql;
                return $this->sourceFetch($sql);
            });
        return $connection;
    }

    private function createTargetConnection(): Connection
    {
        $connection = $this->createConnectionMock();
        $connection->expects(self::never())->method('getCurrentRole');
        $connection->expects(self::any())
            ->method('query')
            ->willReturnCallback(function (string $sql): void {
                $this->log[] = 'target: ' . $this->maskPassword($sql);
                $this->targetQuery($sql);
            });
        $connection->expects(self::atLeastOnce())
            ->method('fetchAll')
            ->willReturnCallback(function (string $sql): array {
                $this->log[] = 'target fetch: ' . $sql;
                return $this->targetFetch($sql);
            });
        return $connection;
    }

    private function createConnectionMock(): Connection&MockObject
    {
        $connection = $this->getMockBuilder(Connection::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['query', 'fetchAll', 'getCurrentRole'])
            ->getMock();
        (new ReflectionProperty(Connection::class, 'logger'))->setValue($connection, $this->logger);
        (new ReflectionProperty(Connection::class, 'defaultRole'))->setValue($connection, self::TARGET_ROLE);
        return $connection;
    }

    /**
     * @return array<int, array<string, string>>
     */
    private function sourceFetch(string $sql): array
    {
        return match ($sql) {
            'SHOW GRANTS ON DATABASE "KEBOOLA_1";' => [
                self::grantRow('OWNERSHIP', 'DATABASE', self::DATABASE, self::DATABASE, self::DATABASE),
            ],
            "SHOW DATABASES LIKE 'KEBOOLA_1'" => [['name' => self::DATABASE, 'retention_time' => '7']],
            "SHOW WAREHOUSES LIKE 'KEBOOLA_PROD'" => [[
                'name' => self::WAREHOUSE,
                'size' => 'Small',
                'type' => 'STANDARD',
                'auto_suspend' => '60',
                'auto_resume' => 'true',
            ]],
            'SHOW GRANTS TO ROLE "MIGRATE_SOURCE"' => [
                self::grantRow('USAGE', 'WAREHOUSE', self::WAREHOUSE, self::SOURCE_ROLE, self::SOURCE_ROLE),
            ],
            "SHOW USERS LIKE 'KEBOOLA_1'" => [['name' => 'KEBOOLA_1', 'default_role' => 'KEBOOLA_1', 'type' => '']],
            "SHOW USERS LIKE 'KEBOOLA_1_RO'" => [
                ['name' => 'KEBOOLA_1_RO', 'default_role' => 'KEBOOLA_1', 'type' => ''],
            ],
            'SHOW GRANTS TO USER "KEBOOLA_1"', 'SHOW GRANTS TO USER "KEBOOLA_1_RO"' => [],
            'SHOW PRIMARY KEYS IN SCHEMA "KEBOOLA_1"."in.c-main";' => [['table_name' => 't1', 'column_name' => 'id']],
            'SHOW PRIMARY KEYS IN SCHEMA "KEBOOLA_1"."out.c-main";' => [],
            'SHOW VIEWS IN DATABASE "KEBOOLA_1";' => $this->sourceViews,
            'SHOW FUNCTIONS IN DATABASE "KEBOOLA_1";',
            'SHOW PROCEDURES IN DATABASE "KEBOOLA_1";' => [],
            default => self::fail('Unexpected source fetch: ' . $sql),
        };
    }

    /**
     * @return array<int, array<string, string>>
     */
    private function targetFetch(string $sql): array
    {
        if (preg_match("/^SHOW USERS LIKE '(.+)'$/", $sql, $matches) === 1) {
            return $this->existingUsersLike($matches[1]);
        }
        return match ($sql) {
            'SHOW VIEWS IN DATABASE "KEBOOLA_1";' => $this->existingViews(),
            "SHOW ROLES LIKE 'KEBOOLA_STORAGE'" => in_array('ROLE "KEBOOLA_STORAGE"', $this->existingOnTarget)
                ? [['name' => self::MAIN_ROLE]]
                : [],
            'SHOW GRANTS TO USER "MIGRATE_TARGET_USER"' => in_array('ROLE "KEBOOLA_STORAGE"', $this->existingOnTarget)
                && $this->mainRoleGrantedToTargetUser
                ? [['role' => self::MAIN_ROLE]]
                : [],
            "SHOW DATABASES LIKE 'KEBOOLA_1'" => array_values(array_map(
                fn(string $object) => ['name' => trim(substr($object, strlen('DATABASE ')), '"')],
                array_filter(
                    $this->existingOnTarget,
                    fn(string $object) => preg_match('/^DATABASE "KEBOOLA.1"$/', $object) === 1,
                ),
            )),
            'SHOW SCHEMAS IN DATABASE "KEBOOLA_1_SHARE";' => [
                ['name' => 'INFORMATION_SCHEMA', 'options' => '', 'retention_time' => '1'],
                ['name' => 'PUBLIC', 'options' => '', 'retention_time' => '1'],
                ['name' => 'in.c-main', 'options' => 'TRANSIENT', 'retention_time' => '1'],
                ['name' => 'out.c-main', 'options' => 'MANAGED ACCESS', 'retention_time' => '7'],
            ],
            'SHOW TABLES IN SCHEMA "KEBOOLA_1_SHARE"."in.c-main";' => [['name' => 't1'], ['name' => 't2']],
            'SHOW TABLES IN SCHEMA "KEBOOLA_1_SHARE"."out.c-main";' => [['name' => 't3']],
            default => self::fail('Unexpected target fetch: ' . $sql),
        };
    }

    /**
     * Behaves like the target account: creating an existing object fails unless IF NOT EXISTS is used,
     * and after a resume the main role no longer owns the database it created in the first run.
     */
    private function targetQuery(string $sql): void
    {
        if (preg_match('/^USE ROLE "(.+)";$/', $sql, $matches) === 1) {
            $this->targetRole = $matches[1];
            return;
        }

        foreach ($this->failingStatements as $prefix => $message) {
            if (str_starts_with($sql, $prefix)) {
                throw new RuntimeException($message);
            }
        }

        if (str_starts_with($sql, 'GRANT OWNERSHIP ON DATABASE KEBOOLA_1 ')
            && $this->targetRole === self::MAIN_ROLE
            && in_array('DATABASE "KEBOOLA_1"', $this->preExistingOnTarget)
        ) {
            throw new RuntimeException('Insufficient privileges to operate on database \'KEBOOLA_1\'');
        }

        $object = null;
        $ifNotExists = false;
        $pattern = '/^CREATE\s+(?:TRANSIENT\s+)?(DATABASE|SCHEMA|USER|TABLE|ROLE|WAREHOUSE|VIEW)\s+'
            . '(IF NOT EXISTS\s+)?(\S+)/';
        if (preg_match($pattern, $sql, $matches) === 1) {
            $object = $matches[1] . ' ' . $matches[3];
            $ifNotExists = $matches[2] !== '';
        } elseif (preg_match('/^ALTER TABLE (\S+) ADD PRIMARY KEY/', $sql, $matches) === 1) {
            $object = 'PRIMARY KEY ' . $matches[1];
        }
        if ($object === null) {
            return;
        }

        $dependency = $this->viewDependencies[$object] ?? null;
        if ($dependency !== null && !in_array($dependency, $this->existingOnTarget)) {
            throw new RuntimeException(sprintf('Object \'%s\' does not exist or not authorized.', $dependency));
        }

        if (in_array($object, $this->existingOnTarget)) {
            if ($ifNotExists) {
                return;
            }
            throw new RuntimeException(sprintf('SQL compilation error: Object \'%s\' already exists.', $object));
        }
        $this->existingOnTarget[] = $object;
        $this->createdOnTarget[] = $object;
    }

    /**
     * @param string[] $log
     * @return string[]
     */
    private function removeExistenceChecks(array $log): array
    {
        $log = array_filter($log, fn(string $v) => $v !== "target fetch: SHOW DATABASES LIKE 'KEBOOLA_1'");
        $log = array_filter($log, fn(string $v) => !str_starts_with($v, 'target fetch: SHOW USERS LIKE '));
        return array_values(array_map(
            fn(string $v) => (string) preg_replace(
                [
                    '/^target: CREATE DATABASE IF NOT EXISTS /',
                    '/^target: CREATE (TRANSIENT)? SCHEMA IF NOT EXISTS /',
                    '/^target: CREATE USER IF NOT EXISTS (KEBOOLA_1(_RO)?) /',
                    '/^target: CREATE TABLE IF NOT EXISTS /',
                ],
                [
                    'target: CREATE DATABASE ',
                    'target: CREATE $1 SCHEMA ',
                    'target: CREATE USER $1 ',
                    'target: CREATE TABLE ',
                ],
                $v,
            ),
            $log,
        ));
    }

    /**
     * Works like Snowflake LIKE: case-insensitive, "_" matches one character, "%" any characters.
     *
     * @return array<int, array<string, string>>
     */
    private function existingUsersLike(string $pattern): array
    {
        $regex = '/^' . strtr(preg_quote($pattern, '/'), ['_' => '.', '%' => '.*']) . '$/i';
        $users = [];
        foreach ($this->existingOnTarget as $object) {
            if (!str_starts_with($object, 'USER ')) {
                continue;
            }
            $name = substr($object, strlen('USER '));
            if (preg_match($regex, $name) === 1) {
                $users[] = ['name' => $name];
            }
        }
        return $users;
    }

    /**
     * @return array<int, array<string, string>>
     */
    private function existingViews(): array
    {
        $views = [];
        foreach ($this->existingOnTarget as $object) {
            if (preg_match('/^VIEW "KEBOOLA_1"\."([^"]+)"\."([^"]+)"$/', $object, $matches) === 1) {
                $views[] = ['database_name' => self::DATABASE, 'schema_name' => $matches[1], 'name' => $matches[2]];
            }
        }
        return $views;
    }

    private function addSourceView(string $name, ?string $selectsFromView = null): void
    {
        $from = $selectsFromView === null ? '"KEBOOLA_1"."in.c-main"."t1"' : self::viewId($selectsFromView);
        $this->sourceViews[] = [
            'name' => $name,
            'schema_name' => 'in.c-main',
            'database_name' => self::DATABASE,
            'owner' => self::DATABASE,
            'text' => sprintf('CREATE VIEW %s AS SELECT * FROM %s', self::viewId($name), $from),
        ];
        if ($selectsFromView !== null) {
            $this->viewDependencies['VIEW ' . self::viewId($name)] = 'VIEW ' . self::viewId($selectsFromView);
        }
    }

    private static function viewId(string $name): string
    {
        return sprintf('"KEBOOLA_1"."in.c-main"."%s"', $name);
    }

    private function countViewAttempts(string $name): int
    {
        return count(array_filter(
            $this->log,
            fn(string $v) => str_starts_with($v, 'target: CREATE VIEW ' . self::viewId($name) . ' '),
        ));
    }

    /**
     * @return string[]
     */
    private function warningsContaining(string $needle): array
    {
        $warnings = array_filter(
            $this->logger->records,
            fn(array $record) => $record['level'] === 'warning' && str_contains($record['message'], $needle),
        );
        return array_values(array_column($warnings, 'message'));
    }

    private function maskPassword(string $sql): string
    {
        return (string) preg_replace("/PASSWORD( ?= ?)'[^']*'/", "PASSWORD\$1'***'", $sql);
    }

    private function createMainRole(): Role
    {
        $grants = new RoleGrants();
        $grants->addWarehouseGrant(
            self::grant('USAGE', 'WAREHOUSE', self::WAREHOUSE, self::MAIN_ROLE, self::TARGET_ROLE),
            self::WAREHOUSE,
            self::WAREHOUSE,
        );
        $grants->addUserGrant(self::grant('OWNERSHIP', 'USER', 'KEBOOLA_1', self::MAIN_ROLE, self::MAIN_ROLE));

        $role = new Role(self::MAIN_ROLE, self::TARGET_ROLE);
        $role->setGrants($grants);
        $role->setFutureGrants(new RoleFutureGrants());
        return $role;
    }

    private function createProjectRoles(): ProjectRoles
    {
        $role = self::DATABASE;
        $grants = new RoleGrants();
        $grants->addDatabaseGrant(self::grant('OWNERSHIP', 'DATABASE', 'KEBOOLA_1', $role, $role));
        $grants->addSchemaGrant(self::grant('OWNERSHIP', 'SCHEMA', 'KEBOOLA_1."in.c-main"', $role, $role));
        $grants->addSchemaGrant(self::grant('OWNERSHIP', 'SCHEMA', 'KEBOOLA_1."out.c-main"', $role, $role));
        $grants->addTableGrant(self::grant('OWNERSHIP', 'TABLE', 'KEBOOLA_1."in.c-main"."t1"', $role, $role));
        $grants->addTableGrant(self::grant('SELECT', 'TABLE', 'KEBOOLA_1."in.c-main"."t1"', $role, $role));
        $grants->addTableGrant(self::grant('OWNERSHIP', 'TABLE', 'KEBOOLA_1."in.c-main"."t2"', $role, $role));
        $grants->addTableGrant(self::grant('OWNERSHIP', 'TABLE', 'KEBOOLA_1."out.c-main"."t3"', $role, $role));
        $grants->addWarehouseGrant(
            self::grant('USAGE', 'WAREHOUSE', self::WAREHOUSE, $role, self::MAIN_ROLE),
            self::WAREHOUSE,
            self::WAREHOUSE,
        );
        $grants->addUserGrant(self::grant('OWNERSHIP', 'USER', 'KEBOOLA_1_RO', $role, $role));

        $projectRole = new Role($role, self::MAIN_ROLE);
        $projectRole->setGrants($grants);
        $projectRole->setFutureGrants(new RoleFutureGrants());

        $projectRoles = new ProjectRoles();
        $projectRoles->addRole($projectRole);
        return $projectRoles;
    }

    private static function grant(
        string $privilege,
        string $grantedOn,
        string $name,
        string $granteeName,
        string $grantedBy,
    ): GrantToRole {
        return GrantToRole::fromArray(self::grantRow($privilege, $grantedOn, $name, $granteeName, $grantedBy));
    }

    /**
     * @return array<string, string>
     */
    private static function grantRow(
        string $privilege,
        string $grantedOn,
        string $name,
        string $granteeName,
        string $grantedBy,
    ): array {
        return [
            'name' => $name,
            'privilege' => $privilege,
            'granted_on' => $grantedOn,
            'granted_to' => 'ROLE',
            'grantee_name' => $granteeName,
            'grant_option' => 'true',
            'granted_by' => $grantedBy,
        ];
    }
}
