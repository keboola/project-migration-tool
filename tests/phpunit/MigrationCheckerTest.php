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
use RuntimeException;

class MigrationCheckerTest extends TestCase
{
    private TestLogger $logger;

    /** @var string[] */
    private array $sourceUsedRoles = [];

    /** @var string[] */
    private array $targetUsedRoles = [];

    /** @var array<int, array{sql: string, role: string, error: ?string}> */
    private array $expectedTargetQueries = [];

    private string $sourceWarehouse = 'WH';

    private string $targetWarehouse = 'WH';

    protected function setUp(): void
    {
        $this->logger = new TestLogger();
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
                $target->users[0]['login_name'] = 'KEBOOLA_1 User';
                $target->users[0]['display_name'] = 'KEBOOLA_1';
            },
            'Users "KEBOOLA_1": login_name: KEBOOLA_1 -> KEBOOLA_1 User, display_name: KEBOOLA_1 User -> KEBOOLA_1',
        ];
        yield 'user default warehouse is not renamed by the migration' => [
            function (FakeSnowflakeAccount $target): void {
                $target->users[0]['default_warehouse'] = 'OTHER_WH';
            },
            'Users "KEBOOLA_1": default_warehouse: WH -> OTHER_WH',
        ];
    }

    /**
     * @dataProvider differenceCreatedByMigrationProvider
     */
    public function testDifferenceCreatedByMigrationIsNotReported(
        Closure $changeAccounts,
        string $expectedInfo,
    ): void {
        $source = FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT');
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $changeAccounts($source, $target, $this);

        $this->runCheck($source, $target);

        self::assertSame([], $this->getWarnings());
        self::assertTrue($this->logger->hasInfoThatContains('Post-migration check passed.'));
        self::assertTrue($this->logger->hasInfoThatContains($expectedInfo), $expectedInfo);
    }

    public static function differenceCreatedByMigrationProvider(): Generator
    {
        yield 'USER$ grants on both accounts' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $source->grantsToRole['KEBOOLA_1_RO'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'USER',
                    'USER$KEBOOLA_WORKSPACE_1',
                    'KEBOOLA_1_RO',
                );
                $target->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'USER',
                    'USER$KEBOOLA_WORKSPACE_2',
                    'KEBOOLA_1',
                );
            },
            'Post-migration check passed.',
        ];
        yield 'warehouse grant renamed to the target warehouse' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target, self $test): void {
                $test->sourceWarehouse = 'SRC_WH';
                $test->targetWarehouse = 'TGT_WH';
                $source->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'WAREHOUSE',
                    'SRC_WH_SMALL',
                    'KEBOOLA_1',
                );
                $target->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'WAREHOUSE',
                    'TGT_WH_SMALL',
                    'KEBOOLA_1',
                );
            },
            'Grants to roles: 6.',
        ];
        yield 'user fields the migration does not copy' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $target->users[0]['first_name'] = '';
                $target->users[0]['last_name'] = '';
                $target->users[0]['email'] = 'other@example.com';
                $target->roles[0]['comment'] = 'other comment';
            },
            'Post-migration check passed.',
        ];
        yield 'grants on the share and old databases' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $target->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'IMPORTED PRIVILEGES',
                    'DATABASE',
                    'DB1_SHARE',
                    'KEBOOLA_1',
                );
                $target->grantsToRole['KEBOOLA_1_RO'][] = FakeSnowflakeAccount::grantToRole(
                    'SELECT',
                    'TABLE',
                    'DB1_OLD.S1.T1',
                    'KEBOOLA_1_RO',
                );
                $source->grantsToRole['KEBOOLA_1_RO'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'SCHEMA',
                    '"DB1_SHARE".S1',
                    'KEBOOLA_1_RO',
                );
            },
            'Excluded 2 grants on the share and old databases on the target account.',
        ];
        yield 'granted by ACCOUNTADMIN on the source' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $source->grantsToRole['KEBOOLA_1_RO'] = [
                    FakeSnowflakeAccount::grantToRole(
                        'SELECT',
                        'TABLE',
                        'DB1.S1.T1',
                        'KEBOOLA_1_RO',
                        'false',
                        'ACCOUNTADMIN',
                    ),
                ];
                $target->grantsToRole['KEBOOLA_1_RO'] = [
                    FakeSnowflakeAccount::grantToRole(
                        'SELECT',
                        'TABLE',
                        'DB1.S1.T1',
                        'KEBOOLA_1_RO',
                        'false',
                        'TARGET_MAIN_MIGRATE',
                    ),
                ];
                $source->grantsOfRole['KEBOOLA_1'] = [
                    FakeSnowflakeAccount::grantOfRole('KEBOOLA_1', 'USER', 'KEBOOLA_1', 'ACCOUNTADMIN'),
                ];
                $target->grantsOfRole['KEBOOLA_1'] = [
                    FakeSnowflakeAccount::grantOfRole('KEBOOLA_1', 'USER', 'KEBOOLA_1', 'TARGET_MAIN_MIGRATE'),
                ];
            },
            'Post-migration check passed.',
        ];
    }

    /**
     * @dataProvider differenceNearMigrationCaseProvider
     */
    public function testDifferenceNearMigrationCaseIsReported(
        Closure $changeAccounts,
        string $expectedDifference,
    ): void {
        $source = FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT');
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $changeAccounts($source, $target, $this);

        try {
            $this->runCheck($source, $target);
            self::fail('UserException was expected.');
        } catch (UserException $e) {
            self::assertStringContainsString('- ' . $expectedDifference, $e->getMessage());
        }
        self::assertSame(['Post-migration check difference: ' . $expectedDifference], $this->getWarnings());
    }

    public static function differenceNearMigrationCaseProvider(): Generator
    {
        yield 'grant name with USER$ not at the start' => [
            function (FakeSnowflakeAccount $source): void {
                $source->grantsToRole['KEBOOLA_1_RO'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'USER',
                    'KEBOOLA_USER$1',
                    'KEBOOLA_1_RO',
                );
            },
            'Grants to roles "USAGE ON USER KEBOOLA_USER$1 TO ROLE KEBOOLA_1_RO": missing in target account',
        ];
        yield 'warehouse grant renamed to a different warehouse' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target, self $test): void {
                $test->sourceWarehouse = 'SRC_WH';
                $test->targetWarehouse = 'TGT_WH';
                $source->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'WAREHOUSE',
                    'SRC_WH_SMALL',
                    'KEBOOLA_1',
                );
                $target->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'WAREHOUSE',
                    'TGT_WH_SMALL',
                    'KEBOOLA_1',
                );
                $target->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'WAREHOUSE',
                    'SRC_WH_LARGE',
                    'KEBOOLA_1',
                );
            },
            'Grants to roles "USAGE ON WAREHOUSE SRC_WH_LARGE TO ROLE KEBOOLA_1": missing in source account',
        ];
        yield 'grant on a database with a similar name' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $target->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'USAGE',
                    'DATABASE',
                    'DB1_SHARED',
                    'KEBOOLA_1',
                );
            },
            'Grants to roles "USAGE ON DATABASE DB1_SHARED TO ROLE KEBOOLA_1": missing in source account',
        ];
        yield 'grant on the share database of a database that is not migrated' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $target->grantsToRole['KEBOOLA_1'][] = FakeSnowflakeAccount::grantToRole(
                    'IMPORTED PRIVILEGES',
                    'DATABASE',
                    'DB2_SHARE',
                    'KEBOOLA_1',
                );
            },
            'Grants to roles "IMPORTED PRIVILEGES ON DATABASE DB2_SHARE TO ROLE KEBOOLA_1": missing in source account',
        ];
        yield 'granted by ACCOUNTADMIN, target granted by another role' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $source->grantsToRole['KEBOOLA_1_RO'] = [
                    FakeSnowflakeAccount::grantToRole(
                        'SELECT',
                        'TABLE',
                        'DB1.S1.T1',
                        'KEBOOLA_1_RO',
                        'false',
                        'ACCOUNTADMIN',
                    ),
                ];
            },
            'Grants to roles "SELECT ON TABLE DB1.S1.T1 TO ROLE KEBOOLA_1_RO": '
                . 'granted_by: TARGET_MAIN_MIGRATE -> KEBOOLA_STORAGE',
        ];
        yield 'granted by ACCOUNTADMIN on the target' => [
            function (FakeSnowflakeAccount $source, FakeSnowflakeAccount $target): void {
                $target->grantsToRole['KEBOOLA_1_RO'] = [
                    FakeSnowflakeAccount::grantToRole(
                        'SELECT',
                        'TABLE',
                        'DB1.S1.T1',
                        'KEBOOLA_1_RO',
                        'false',
                        'ACCOUNTADMIN',
                    ),
                ];
            },
            'Grants to roles "SELECT ON TABLE DB1.S1.T1 TO ROLE KEBOOLA_1_RO": '
                . 'granted_by: KEBOOLA_STORAGE -> ACCOUNTADMIN',
        ];
    }

    public function testDatabaseObjectsAreListedWithTheProjectRoleOnBothAccounts(): void
    {
        $this->runCheck(
            FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'),
            FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT'),
        );

        self::assertContains('KEBOOLA_1', $this->sourceUsedRoles);
        self::assertContains('KEBOOLA_1', $this->targetUsedRoles);
        self::assertTrue($this->logger->hasInfoThatContains('Post-migration check passed.'));
    }

    public function testProjectRoleIsGrantedAndRevokedWhenUserDoesNotHaveIt(): void
    {
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $target->rolesOfUser['TARGET_MIGRATE'] = ['TARGET_MAIN_MIGRATE'];
        $this->expectedTargetQueries = [
            [
                'sql' => 'GRANT ROLE "KEBOOLA_1" TO USER "TARGET_MIGRATE"',
                'role' => 'TARGET_MAIN_MIGRATE',
                'error' => null,
            ],
            [
                'sql' => 'REVOKE ROLE "KEBOOLA_1" FROM USER "TARGET_MIGRATE"',
                'role' => 'TARGET_MAIN_MIGRATE',
                'error' => null,
            ],
        ];

        $this->runCheck(FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'), $target);

        self::assertContains('KEBOOLA_1', $this->targetUsedRoles);
        self::assertTrue($this->logger->hasInfoThatContains('Post-migration check passed.'));
    }

    public function testFailingProjectRoleGrantEndsWithUserException(): void
    {
        $target = FakeSnowflakeAccount::keboolaProject('TARGET_ACCOUNT');
        $target->rolesOfUser['TARGET_MIGRATE'] = ['TARGET_MAIN_MIGRATE'];
        $this->expectedTargetQueries = [
            [
                'sql' => 'GRANT ROLE "KEBOOLA_1" TO USER "TARGET_MIGRATE"',
                'role' => 'TARGET_MAIN_MIGRATE',
                'error' => 'Insufficient privileges',
            ],
        ];

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'The migration user "TARGET_MIGRATE" does not have the role "KEBOOLA_1" on the target account '
            . 'and the check cannot grant it: Insufficient privileges',
        );
        $this->runCheck(FakeSnowflakeAccount::keboolaProject('SOURCE_ACCOUNT'), $target);
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
            ->willReturnCallback(function (string $role): void {
                $this->sourceUsedRoles[] = $role;
            });
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
        $expectedTargetQueries = $this->expectedTargetQueries;
        $targetConnection->expects(self::exactly(count($expectedTargetQueries)))
            ->method('query')
            ->willReturnCallback(function (string $sql) use (&$expectedTargetQueries): void {
                $expected = array_shift($expectedTargetQueries);
                self::assertNotNull($expected);
                self::assertSame($expected['sql'], $sql);
                self::assertSame($expected['role'], end($this->targetUsedRoles));
                if ($expected['error'] !== null) {
                    throw new RuntimeException($expected['error']);
                }
            });

        $config = new Config(
            [
                'parameters' => [
                    'action' => Config::ACTION_CHECK,
                    'migrateDatabases' => ['DB1'],
                    'checkReportOnly' => $reportOnly,
                    'credentials' => [
                        'source' => $this->credentials('SOURCE_MIGRATE', 'SOURCE_MAIN_MIGRATE', $this->sourceWarehouse),
                        'target' => $this->credentials('TARGET_MIGRATE', 'TARGET_MAIN_MIGRATE', $this->targetWarehouse),
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
    private function credentials(string $user, string $role, string $warehouse): array
    {
        return [
            'host' => 'host',
            'username' => $user,
            '#password' => 'password',
            'warehouse' => $warehouse,
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
