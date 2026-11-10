<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Closure;
use Generator;
use Keboola\Component\UserException;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\MigrationChecker;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Tests\Snowflake\TestLogger;

class MigrationCheckerTest extends TestCase
{
    private TestLogger $logger;

    /** @var string[] */
    private array $targetUsedRoles = [];

    protected function setUp(): void
    {
        $this->logger = new TestLogger();
        $this->targetUsedRoles = [];
    }

    public function testNoDifferencesPasses(): void
    {
        $this->runCheck(
            FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'),
            FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'),
        );

        self::assertTrue($this->logger->hasInfoThatContains(
            'Post-migration check passed. Compared items: Databases: 1, Schemas: 1, Tables: 1, Views: 1, Users: 1, '
            . 'Grants of roles: 2, Roles: 2, Grants to roles: 5.',
        ));
        self::assertSame([], $this->getWarnings());
        self::assertContains('KEBOOLA_1', $this->targetUsedRoles);
    }

    public function testAccountGrantWithDifferentAccountNameIsNotReported(): void
    {
        $this->runCheck(
            FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'),
            FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT'),
        );

        self::assertTrue($this->logger->hasInfoThatContains('Post-migration check passed.'));
        self::assertSame([], $this->getWarnings());
    }

    /**
     * @dataProvider singleDifferenceProvider
     */
    public function testSingleDifferenceIsReported(Closure $changeTarget, string $expectedDifference): void
    {
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $changeTarget($target);

        try {
            $this->runCheck(FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'), $target);
            self::fail('UserException was expected.');
        } catch (UserException $e) {
            self::assertStringContainsString('Post-migration check found 1 differences', $e->getMessage());
            self::assertStringContainsString('- ' . $expectedDifference, $e->getMessage());
        }
        self::assertSame(['Post-migration check difference: ' . $expectedDifference], $this->getWarnings());
    }

    public static function singleDifferenceProvider(): Generator
    {
        yield 'table missing on target' => [
            function (FakeSnowflakeAccount $target): void {
                $target->tables['DB1.S1'] = [];
            },
            'Tables "DB1.S1.T1": missing in target account',
        ];
        yield 'table missing on source' => [
            function (FakeSnowflakeAccount $target): void {
                $target->tables['DB1.S1'][] = FakeSnowflakeAccount::table('S1', 'T2', '0', 'KEBOOLA_1');
            },
            'Tables "DB1.S1.T2": missing in source account',
        ];
        yield 'row count mismatch' => [
            function (FakeSnowflakeAccount $target): void {
                $target->tables['DB1.S1'] = [FakeSnowflakeAccount::table('S1', 'T1', '9', 'KEBOOLA_1')];
            },
            'Tables "DB1.S1.T1": rows: 10 -> 9',
        ];
        yield 'table owner mismatch' => [
            function (FakeSnowflakeAccount $target): void {
                $target->tables['DB1.S1'] = [FakeSnowflakeAccount::table('S1', 'T1', '10', 'KEBOOLA_1_RO')];
            },
            'Tables "DB1.S1.T1": owner: KEBOOLA_1 -> KEBOOLA_1_RO',
        ];
        yield 'view missing on target' => [
            function (FakeSnowflakeAccount $target): void {
                $target->views['DB1.S1'] = [];
            },
            'Views "DB1.S1.V1": missing in target account',
        ];
        yield 'grant option mismatch' => [
            function (FakeSnowflakeAccount $target): void {
                $target->grantsToRole['KEBOOLA_1_RO'] = [
                    FakeSnowflakeAccount::grantToRole('SELECT', 'TABLE', 'DB1.S1.T1', 'KEBOOLA_1_RO', 'true'),
                ];
            },
            'Grants to roles "SELECT ON TABLE DB1.S1.T1 TO ROLE KEBOOLA_1_RO": grant_option: false -> true',
        ];
        yield 'grant of role missing on target' => [
            function (FakeSnowflakeAccount $target): void {
                $target->grantsOfRole['KEBOOLA_1'] = [];
            },
            'Grants of roles "KEBOOLA_1 TO USER KEBOOLA_1 BY KEBOOLA_STORAGE": missing in target account',
        ];
        yield 'role owner mismatch' => [
            function (FakeSnowflakeAccount $target): void {
                $target->roles[1] = FakeSnowflakeAccount::role('KEBOOLA_1_RO', 'KEBOOLA_STORAGE');
            },
            'Roles "KEBOOLA_1_RO": owner: KEBOOLA_1 -> KEBOOLA_STORAGE',
        ];
        yield 'value moved between two fields' => [
            function (FakeSnowflakeAccount $target): void {
                $target->users[0] = FakeSnowflakeAccount::user('KEBOOLA_1', 'KEBOOLA_1', 'Doe', 'John');
            },
            'Users "KEBOOLA_1": first_name: John -> Doe, last_name: Doe -> John',
        ];
    }

    public function testGrantsOfRolesToMigrationUserAreExcluded(): void
    {
        $source = FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT');
        $source->grantsOfRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantOfRole('KEBOOLA_1', 'USER', 'SOURCE_MIGRATE');
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $target->grantsOfRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantOfRole('KEBOOLA_1', 'USER', 'TARGET_MIGRATE');
        $target->grantsOfRole['KEBOOLA_1_RO'][] = FakeSnowflakeAccount::grantOfRole(
            'KEBOOLA_1_RO',
            'USER',
            'TARGET_MIGRATE',
        );

        $this->runCheck($source, $target);

        self::assertTrue($this->logger->hasInfoThatContains('Post-migration check passed.'));
        self::assertTrue($this->logger->hasInfoThatContains(
            'Excluded 1 grants of roles to the migration user "SOURCE_MIGRATE" on the source account.',
        ));
        self::assertTrue($this->logger->hasInfoThatContains(
            'Excluded 2 grants of roles to the migration user "TARGET_MIGRATE" on the target account.',
        ));
        self::assertSame([], $this->getWarnings());
    }

    /**
     * @dataProvider grantOfRoleToOtherGranteeProvider
     */
    public function testGrantOfRoleToOtherGranteeIsReported(
        string $grantedTo,
        string $granteeName,
        string $expectedDifference,
    ): void {
        $source = FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT');
        $source->grantsOfRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantOfRole('KEBOOLA_1', $grantedTo, $granteeName);

        try {
            $this->runCheck($source, FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT'));
            self::fail('UserException was expected.');
        } catch (UserException $e) {
            self::assertStringContainsString('- ' . $expectedDifference, $e->getMessage());
        }
        self::assertSame(['Post-migration check difference: ' . $expectedDifference], $this->getWarnings());
    }

    public static function grantOfRoleToOtherGranteeProvider(): Generator
    {
        yield 'migration user of the other account' => [
            'grantedTo' => 'USER',
            'granteeName' => 'TARGET_MIGRATE',
            'expectedDifference' => 'Grants of roles "KEBOOLA_1 TO USER TARGET_MIGRATE BY KEBOOLA_STORAGE": '
                . 'missing in target account',
        ];
        yield 'user with a similar name' => [
            'grantedTo' => 'USER',
            'granteeName' => 'SOURCE_MIGRATE_2',
            'expectedDifference' => 'Grants of roles "KEBOOLA_1 TO USER SOURCE_MIGRATE_2 BY KEBOOLA_STORAGE": '
                . 'missing in target account',
        ];
        yield 'role with the migration user name' => [
            'grantedTo' => 'ROLE',
            'granteeName' => 'SOURCE_MIGRATE',
            'expectedDifference' => 'Grants of roles "KEBOOLA_1 TO ROLE SOURCE_MIGRATE BY KEBOOLA_STORAGE": '
                . 'missing in target account',
        ];
        yield 'migration role' => [
            'grantedTo' => 'ROLE',
            'granteeName' => 'SOURCE_MAIN_MIGRATE',
            'expectedDifference' => 'Grants of roles "KEBOOLA_1 TO ROLE SOURCE_MAIN_MIGRATE BY KEBOOLA_STORAGE": '
                . 'missing in target account',
        ];
    }

    public function testRowsWithSameIdentityArePairedRegardlessOfOrder(): void
    {
        $grantedByA = FakeSnowflakeAccount::grantToRole('SELECT', 'TABLE', 'DB1.S1.T1', 'KEBOOLA_1_RO', 'false', 'A');
        $grantedByB = FakeSnowflakeAccount::grantToRole('SELECT', 'TABLE', 'DB1.S1.T1', 'KEBOOLA_1_RO', 'false', 'B');
        $source = FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT');
        $source->grantsToRole['KEBOOLA_1_RO'] = [$grantedByA, $grantedByB];
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $target->grantsToRole['KEBOOLA_1_RO'] = [$grantedByB, $grantedByA];

        $this->runCheck($source, $target);

        self::assertTrue($this->logger->hasInfoThatContains('Post-migration check passed.'));
        self::assertSame([], $this->getWarnings());
    }

    public function testDatabaseMissingOnTargetIsReported(): void
    {
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $target->databases = ['db1', 'DB1_SHARE'];

        try {
            $this->runCheck(FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'), $target);
            self::fail('UserException was expected.');
        } catch (UserException $e) {
            self::assertStringContainsString(
                'Post-migration check found 1 differences (Databases: 1).',
                $e->getMessage(),
            );
        }
        self::assertSame(
            ['Post-migration check difference: Databases "DB1": missing in target account'],
            $this->getWarnings(),
        );
    }

    public function testRoleDiscoveredOnlyOnSourceIsReported(): void
    {
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        unset($target->roles[1], $target->grantsToRole['KEBOOLA_1_RO'], $target->grantsOfRole['KEBOOLA_1_RO']);
        $target->grantsToRole['KEBOOLA_1'] = array_values(array_filter(
            $target->grantsToRole['KEBOOLA_1'],
            fn(array $grant) => $grant['granted_on'] !== 'ROLE',
        ));

        try {
            $this->runCheck(FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'), $target);
            self::fail('UserException was expected.');
        } catch (UserException $e) {
            self::assertStringContainsString(
                'Post-migration check found 4 differences (Grants of roles: 1, Roles: 1, Grants to roles: 2).',
                $e->getMessage(),
            );
        }
        self::assertSame(
            [
                'Post-migration check difference: Grants to roles "OWNERSHIP ON ROLE KEBOOLA_1_RO TO ROLE KEBOOLA_1": '
                . 'missing in target account',
                'Post-migration check difference: Grants to roles "SELECT ON TABLE DB1.S1.T1 TO ROLE KEBOOLA_1_RO": '
                . 'missing in target account',
                'Post-migration check difference: Grants of roles "KEBOOLA_1_RO TO ROLE KEBOOLA_1 BY KEBOOLA_STORAGE": '
                . 'missing in target account',
                'Post-migration check difference: Roles "KEBOOLA_1_RO": missing in target account',
            ],
            $this->getWarnings(),
        );
    }

    public function testExceptionListsOnlyFirstFiftyDifferences(): void
    {
        $source = FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT');
        for ($i = 1; $i <= 60; $i++) {
            $source->tables['DB1.S1'][] = FakeSnowflakeAccount::table('S1', sprintf('NEW_%02d', $i), '1', 'KEBOOLA_1');
        }

        try {
            $this->runCheck($source, FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'));
            self::fail('UserException was expected.');
        } catch (UserException $e) {
            self::assertStringStartsWith(
                "Post-migration check found 60 differences (Tables: 60).\n"
                . "The first 50 differences are listed below. The full list is in the job log.\n"
                . '- Tables "DB1.S1.NEW_01": missing in target account',
                $e->getMessage(),
            );
            self::assertSame(50, substr_count($e->getMessage(), "\n- "));
            self::assertStringContainsString('NEW_50', $e->getMessage());
            self::assertStringNotContainsString('NEW_51', $e->getMessage());
        }
        self::assertCount(60, $this->getWarnings());
    }

    public function testReportOnlyLogsDifferencesWithoutFailing(): void
    {
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $target->tables['DB1.S1'] = [];

        $this->runCheck(FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'), $target, true);

        self::assertSame(
            [
                'Post-migration check difference: Tables "DB1.S1.T1": missing in target account',
                'Post-migration check found 1 differences (Tables: 1). '
                . 'The job does not fail because "checkReportOnly" is enabled.',
            ],
            $this->getWarnings(),
        );
    }

    private function runCheck(
        FakeSnowflakeAccount $source,
        FakeSnowflakeAccount $target,
        bool $reportOnly = false,
    ): void {
        $sourceConnection = $this->createMock(Connection::class);
        $sourceConnection->expects(self::atLeastOnce())
            ->method('fetchAll')
            ->willReturnCallback(fn(string $sql) => $source->fetchAll($sql));
        $sourceConnection->expects(self::once())
            ->method('getOwnershipRoleOnDatabase')
            ->with('DB1')
            ->willReturn('KEBOOLA_1');
        $sourceConnection->expects(self::atLeastOnce())
            ->method('useRole')
            ->with('SOURCE_MAIN_MIGRATE');
        $sourceConnection->expects(self::never())->method('query');

        $targetConnection = $this->createMock(Connection::class);
        $targetConnection->expects(self::atLeastOnce())
            ->method('fetchAll')
            ->willReturnCallback(fn(string $sql) => $target->fetchAll($sql));
        $targetConnection->expects(self::atLeastOnce())
            ->method('useRole')
            ->willReturnCallback(function (string $role): void {
                $this->targetUsedRoles[] = $role;
            });
        $targetConnection->expects(self::never())->method('query');

        $config = new Config(
            [
                'parameters' => [
                    'action' => Config::ACTION_CHECK,
                    'migrateDatabases' => ['DB1'],
                    'checkReportOnly' => $reportOnly,
                    'credentials' => [
                        'source' => $this->credentials('SOURCE_MIGRATE', 'SOURCE_MAIN_MIGRATE'),
                        'target' => $this->credentials('TARGET_MIGRATE', 'TARGET_MAIN_MIGRATE'),
                    ],
                ],
            ],
            new ConfigDefinition(),
        );

        $checker = new MigrationChecker($sourceConnection, $targetConnection, $config, $this->logger, ['DB1']);
        $checker->postMigrationCheckStructure();
    }

    /**
     * @return array<string, string>
     */
    private function credentials(string $user, string $role): array
    {
        return [
            'host' => 'host',
            'username' => $user,
            '#password' => 'password',
            'warehouse' => 'WH',
            'role' => $role,
        ];
    }

    /**
     * @return string[]
     */
    private function getWarnings(): array
    {
        $warnings = array_filter($this->logger->records, fn(array $record) => $record['level'] === 'warning');
        return array_values(array_map(fn(array $record) => $record['message'], $warnings));
    }
}
