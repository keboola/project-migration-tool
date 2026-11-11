<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\ValueObject;

use Generator;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\ValueObject\FutureGrantToRole;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\ProjectRoles;
use ProjectMigrationTool\ValueObject\Role;
use ProjectMigrationTool\ValueObject\RoleFutureGrants;
use ProjectMigrationTool\ValueObject\RoleGrants;

class ProjectRolesTest extends TestCase
{
    private const DATABASES = ['DB1', 'db2', '"DB3"', 'D.B4'];
    private const SCHEMAS = ['S1', 'WORK', '"S.DOT"', 'my schema'];
    private const TABLES = ['T1', 'Table_2', 'my table', 'a.b', '"QUOTED"', 'T"X', '"x""y"', 'ěščř'];

    public function testGetTableGrantsMatchesOldFilter(): void
    {
        $projectRoles = $this->createProjectRolesWithAllQuotingVariants();

        $lookups = 0;
        foreach (self::lookupTriples() as [$database, $schema, $table]) {
            $expected = array_values(self::oldFilter($projectRoles, $database, $schema, $table));

            self::assertSame(
                $expected,
                $projectRoles->getTableGrants($database, $schema, $table),
                sprintf('Lookup "%s" / "%s" / "%s"', $database, $schema, $table),
            );
            $lookups++;
        }
        self::assertGreaterThan(200, $lookups);
    }

    public function testFixtureCoversMatchesAndNonMatches(): void
    {
        $projectRoles = $this->createProjectRolesWithAllQuotingVariants();
        self::assertGreaterThan(300, count($projectRoles->getTableGrantsFromAllRoles()));

        $withGrants = 0;
        $withMultipleRoles = 0;
        foreach (self::lookupTriples() as [$database, $schema, $table]) {
            $grants = $projectRoles->getTableGrants($database, $schema, $table);
            if ($grants !== []) {
                $withGrants++;
            }
            $roles = array_unique(array_map(fn(GrantToRole $v) => $v->getGranteeName(), $grants));
            if (count($roles) > 1) {
                $withMultipleRoles++;
            }
        }
        self::assertGreaterThan(50, $withGrants);
        self::assertGreaterThan(10, $withMultipleRoles);
    }

    public function testGetTableGrantsReturnsEmptyArrayForTableWithoutGrants(): void
    {
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRole('ROLE_A', [
            self::createGrant('DB1.S1.T1', 'ROLE_A', 'OWNERSHIP'),
        ]));

        self::assertSame([], $projectRoles->getTableGrants('DB1', 'S1', 'T2'));
    }

    public function testGetTableGrantsReturnsEmptyArrayWithoutRoles(): void
    {
        self::assertSame([], (new ProjectRoles())->getTableGrants('DB1', 'S1', 'T1'));
    }

    /**
     * @param array{string, string, string} $lookup
     * @param string[] $expectedGrantNames
     * @dataProvider sameTableNameProvider
     */
    public function testGetTableGrantsDoesNotMixSchemasOrDatabases(array $lookup, array $expectedGrantNames): void
    {
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRole('ROLE_A', [
            self::createGrant('DB1.S1.T', 'ROLE_A', 'OWNERSHIP'),
            self::createGrant('DB1.S2.T', 'ROLE_A', 'OWNERSHIP'),
            self::createGrant('DB2.S1.T', 'ROLE_A', 'OWNERSHIP'),
            self::createGrant('"DB1"."S1"."T"', 'ROLE_A', 'SELECT'),
        ]));

        $grants = $projectRoles->getTableGrants(...$lookup);

        self::assertSame($expectedGrantNames, array_map(fn(GrantToRole $v) => $v->getName(), $grants));
    }

    public static function sameTableNameProvider(): Generator
    {
        yield 'DB1.S1' => [['DB1', 'S1', 'T'], ['DB1.S1.T', '"DB1"."S1"."T"']];
        yield 'DB1.S2' => [['DB1', 'S2', 'T'], ['DB1.S2.T']];
        yield 'DB2.S1' => [['DB2', 'S1', 'T'], ['DB2.S1.T']];
        yield 'DB2.S2' => [['DB2', 'S2', 'T'], []];
    }

    public function testGetTableGrantsReturnsSameObjects(): void
    {
        $ownership = self::createGrant('DB1.S1.T1', 'ROLE_A', 'OWNERSHIP');
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRole('ROLE_A', [$ownership]));

        $grants = $projectRoles->getTableGrants('DB1', 'S1', 'T1');

        self::assertCount(1, $grants);
        self::assertSame($ownership, $grants[0]);
    }

    public function testGetTableGrantsSeesRoleAddedAfterLookup(): void
    {
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRole('ROLE_A', [
            self::createGrant('DB1.S1.T1', 'ROLE_A', 'OWNERSHIP'),
        ]));
        self::assertCount(1, $projectRoles->getTableGrants('DB1', 'S1', 'T1'));

        $projectRoles->addRole(self::createRole('ROLE_B', [
            self::createGrant('DB1.S1.T1', 'ROLE_B', 'SELECT'),
        ]));

        $grants = $projectRoles->getTableGrants('DB1', 'S1', 'T1');
        self::assertSame(['ROLE_A', 'ROLE_B'], array_map(fn(GrantToRole $v) => $v->getGranteeName(), $grants));
    }

    public function testGetTableGrantsSeesReplacedRole(): void
    {
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRole('ROLE_A', [
            self::createGrant('DB1.S1.T1', 'ROLE_A', 'OWNERSHIP'),
        ]));
        self::assertCount(1, $projectRoles->getTableGrants('DB1', 'S1', 'T1'));

        $projectRoles->addRole(self::createRole('ROLE_A', [
            self::createGrant('DB1.S1.T2', 'ROLE_A', 'OWNERSHIP'),
        ]));

        self::assertSame([], $projectRoles->getTableGrants('DB1', 'S1', 'T1'));
        self::assertCount(1, $projectRoles->getTableGrants('DB1', 'S1', 'T2'));
    }

    public function testGetTableGrantsScalesWithManyTablesAndGrants(): void
    {
        $roleNames = ['ROLE_A', 'ROLE_B', 'ROLE_C'];
        $grantsByRole = array_fill_keys($roleNames, []);
        for ($i = 0; $i < 5000; $i++) {
            foreach ($roleNames as $roleName) {
                $grantsByRole[$roleName][] = self::createGrant(
                    sprintf('KBC_DB."in.c-bucket-%d".TABLE_%d', $i % 50, $i),
                    $roleName,
                    $roleName === 'ROLE_A' ? 'OWNERSHIP' : 'SELECT',
                );
            }
        }
        $projectRoles = new ProjectRoles();
        foreach ($grantsByRole as $roleName => $grants) {
            $projectRoles->addRole(self::createRole($roleName, $grants));
        }

        $start = microtime(true);
        for ($i = 0; $i < 5000; $i++) {
            $grants = $projectRoles->getTableGrants('KBC_DB', 'in.c-bucket-' . ($i % 50), 'TABLE_' . $i);
            self::assertCount(3, $grants);
        }
        $duration = microtime(true) - $start;

        // Loose bound on purpose: the old filter needs minutes here, a busy CI runner must not fail this.
        self::assertLessThan(10.0, $duration, sprintf('5,000 lookups took %.2f s', $duration));
    }

    public function testGetSchemaGrantsMatchesOldFilter(): void
    {
        $projectRoles = $this->createProjectRolesWithAllSchemaQuotingVariants();

        $lookups = 0;
        $withGrants = 0;
        foreach (self::lookupPairs() as [$database, $schema]) {
            $expected = array_values(
                self::oldSchemaFilter($database, $schema, $projectRoles->getSchemaGrantsFromAllRoles()),
            );
            $grants = $projectRoles->getSchemaGrants($database, $schema);

            self::assertSame($expected, $grants, sprintf('Lookup "%s" / "%s"', $database, $schema));
            $lookups++;
            if ($grants !== []) {
                $withGrants++;
            }
        }
        self::assertGreaterThan(50, $lookups);
        self::assertGreaterThan(20, $withGrants);
    }

    public function testGetFutureTableGrantsMatchesOldFilter(): void
    {
        $projectRoles = $this->createProjectRolesWithAllSchemaQuotingVariants();

        $lookups = 0;
        $withGrants = 0;
        foreach (self::lookupPairs() as [$database, $schema]) {
            $expected = array_values(
                self::oldSchemaFilter($database, $schema, $projectRoles->getFutureTableGrantsFromAllRoles()),
            );
            $grants = $projectRoles->getFutureTableGrants($database, $schema);

            self::assertSame($expected, $grants, sprintf('Lookup "%s" / "%s"', $database, $schema));
            $lookups++;
            if ($grants !== []) {
                $withGrants++;
            }
        }
        self::assertGreaterThan(50, $lookups);
        self::assertGreaterThan(20, $withGrants);
    }

    public function testGetSchemaGrantsDoNotMixDatabases(): void
    {
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRoleWithSchemaGrants(
            'ROLE_A',
            [
                self::createSchemaGrant('DB1.S', 'ROLE_A', 'OWNERSHIP'),
                self::createSchemaGrant('DB2.S', 'ROLE_A', 'OWNERSHIP'),
                self::createSchemaGrant('"DB1"."S"', 'ROLE_A', 'USAGE'),
            ],
            [
                self::createFutureTableGrant('DB1.S', 'ROLE_A', 'SELECT'),
                self::createFutureTableGrant('DB2.S', 'ROLE_A', 'SELECT'),
            ],
        ));

        $names = fn(array $grants) => array_map(fn(GrantToRole|FutureGrantToRole $v) => $v->getName(), $grants);
        self::assertSame(['DB1.S', '"DB1"."S"'], $names($projectRoles->getSchemaGrants('DB1', 'S')));
        self::assertSame(['DB2.S'], $names($projectRoles->getSchemaGrants('DB2', 'S')));
        self::assertSame([], $projectRoles->getSchemaGrants('DB3', 'S'));
        self::assertSame(['DB1.S'], $names($projectRoles->getFutureTableGrants('DB1', 'S')));
        self::assertSame(['DB2.S'], $names($projectRoles->getFutureTableGrants('DB2', 'S')));
        self::assertSame([], $projectRoles->getFutureTableGrants('DB3', 'S'));
    }

    public function testGetSchemaGrantsReturnsEmptyArrayWithoutGrants(): void
    {
        $projectRoles = new ProjectRoles();
        self::assertSame([], $projectRoles->getSchemaGrants('DB1', 'S1'));
        self::assertSame([], $projectRoles->getFutureTableGrants('DB1', 'S1'));

        $projectRoles->addRole(self::createRoleWithSchemaGrants('ROLE_A', [], []));
        self::assertSame([], $projectRoles->getSchemaGrants('DB1', 'S1'));
        self::assertSame([], $projectRoles->getFutureTableGrants('DB1', 'S1'));
    }

    public function testGetSchemaGrantsReturnsSameObjects(): void
    {
        $ownership = self::createSchemaGrant('DB1.S1', 'ROLE_A', 'OWNERSHIP');
        $future = self::createFutureTableGrant('DB1.S1', 'ROLE_A', 'SELECT');
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRoleWithSchemaGrants('ROLE_A', [$ownership], [$future]));

        $schemaGrants = $projectRoles->getSchemaGrants('DB1', 'S1');
        $futureGrants = $projectRoles->getFutureTableGrants('DB1', 'S1');

        self::assertCount(1, $schemaGrants);
        self::assertSame($ownership, $schemaGrants[0]);
        self::assertCount(1, $futureGrants);
        self::assertSame($future, $futureGrants[0]);
    }

    public function testGetSchemaGrantsSeesRoleAddedAfterLookup(): void
    {
        $projectRoles = new ProjectRoles();
        $projectRoles->addRole(self::createRoleWithSchemaGrants(
            'ROLE_A',
            [self::createSchemaGrant('DB1.S1', 'ROLE_A', 'OWNERSHIP')],
            [self::createFutureTableGrant('DB1.S1', 'ROLE_A', 'SELECT')],
        ));
        self::assertCount(1, $projectRoles->getSchemaGrants('DB1', 'S1'));
        self::assertCount(1, $projectRoles->getFutureTableGrants('DB1', 'S1'));

        $projectRoles->addRole(self::createRoleWithSchemaGrants(
            'ROLE_B',
            [self::createSchemaGrant('DB1.S1', 'ROLE_B', 'USAGE')],
            [self::createFutureTableGrant('DB1.S1', 'ROLE_B', 'SELECT')],
        ));

        $granteeNames = fn(array $grants) => array_map(
            fn(GrantToRole|FutureGrantToRole $v) => $v->getGranteeName(),
            $grants,
        );
        self::assertSame(['ROLE_A', 'ROLE_B'], $granteeNames($projectRoles->getSchemaGrants('DB1', 'S1')));
        self::assertSame(['ROLE_A', 'ROLE_B'], $granteeNames($projectRoles->getFutureTableGrants('DB1', 'S1')));
    }

    /**
     * @template T of GrantToRole|FutureGrantToRole
     * @param T[] $schemasGrants
     * @return T[]
     */
    private static function oldSchemaFilter(string $database, string $schemaName, array $schemasGrants): array
    {
        return array_filter(
            $schemasGrants,
            function (GrantToRole|FutureGrantToRole $v) use ($database, $schemaName) {
                $validSchema = Helper::generateFormattedQuoteCombinations($database, $schemaName);
                return in_array($v->getName(), $validSchema);
            }
        );
    }

    /**
     * @return Generator<array{string, string}>
     */
    private static function lookupPairs(): Generator
    {
        foreach (self::withQuotedVariants(self::DATABASES) as $database) {
            foreach (self::withQuotedVariants(self::SCHEMAS) as $schema) {
                yield [$database, $schema];
            }
        }
        yield ['DB1', 'MISSING'];
        yield ['MISSING', 'S1'];
        yield ['D', 'B4.S1'];
        yield ['D.B4', 'S1'];
    }

    private function createProjectRolesWithAllSchemaQuotingVariants(): ProjectRoles
    {
        mt_srand(20261111);
        $roleNames = ['ROLE_A', 'ROLE_B', 'ROLE_C', 'ROLE_D'];
        $schemaGrantsByRole = array_fill_keys($roleNames, []);
        $futureGrantsByRole = array_fill_keys($roleNames, []);
        $privileges = ['OWNERSHIP', 'USAGE', 'CREATE TABLE', 'MONITOR'];

        foreach (self::DATABASES as $database) {
            foreach (self::SCHEMAS as $schema) {
                $names = Helper::generateFormattedQuoteCombinations($database, $schema);
                $names[] = strtolower(sprintf('%s.%s', $database, $schema));
                $names[] = sprintf('%s.%s ', $database, $schema);
                foreach ($names as $name) {
                    if (mt_rand(0, 9) < 5) {
                        $roleName = $roleNames[mt_rand(0, count($roleNames) - 1)];
                        $schemaGrantsByRole[$roleName][] = self::createSchemaGrant(
                            $name,
                            $roleName,
                            $privileges[mt_rand(0, count($privileges) - 1)],
                        );
                    }
                    if (mt_rand(0, 9) < 5) {
                        $roleName = $roleNames[mt_rand(0, count($roleNames) - 1)];
                        $futureGrantsByRole[$roleName][] = self::createFutureTableGrant($name, $roleName, 'SELECT');
                    }
                }
            }
        }
        foreach ($roleNames as $roleName) {
            $schemaGrantsByRole[$roleName][] = self::createSchemaGrant('DB1.S1', $roleName, 'USAGE');
            $futureGrantsByRole[$roleName][] = self::createFutureTableGrant('"DB1"."S1"', $roleName, 'SELECT');
        }

        $projectRoles = new ProjectRoles();
        foreach ($roleNames as $roleName) {
            $schemaGrants = $schemaGrantsByRole[$roleName];
            $futureGrants = $futureGrantsByRole[$roleName];
            shuffle($schemaGrants);
            shuffle($futureGrants);
            $projectRoles->addRole(self::createRoleWithSchemaGrants($roleName, $schemaGrants, $futureGrants));
        }
        return $projectRoles;
    }

    /**
     * @param GrantToRole[] $schemaGrants
     * @param FutureGrantToRole[] $futureTableGrants
     */
    private static function createRoleWithSchemaGrants(
        string $name,
        array $schemaGrants,
        array $futureTableGrants,
    ): Role {
        $roleGrants = new RoleGrants();
        foreach ($schemaGrants as $grant) {
            $roleGrants->addSchemaGrant($grant);
        }
        $roleFutureGrants = new RoleFutureGrants();
        foreach ($futureTableGrants as $grant) {
            $roleFutureGrants->addTableGrant($grant);
        }
        $role = new Role($name, 'ACCOUNTADMIN');
        $role->setGrants($roleGrants);
        $role->setFutureGrants($roleFutureGrants);
        return $role;
    }

    private static function createSchemaGrant(string $name, string $roleName, string $privilege): GrantToRole
    {
        return new GrantToRole($name, $privilege, 'SCHEMA', 'ROLE', $roleName, 'false', 'ACCOUNTADMIN');
    }

    private static function createFutureTableGrant(string $name, string $roleName, string $privilege): FutureGrantToRole
    {
        return new FutureGrantToRole($privilege, 'TABLE', $name, 'ROLE', $roleName, 'false');
    }

    /**
     * @return GrantToRole[]
     */
    private static function oldFilter(
        ProjectRoles $projectRoles,
        string $database,
        string $schema,
        string $table,
    ): array {
        return array_filter(
            $projectRoles->getTableGrantsFromAllRoles(),
            function (GrantToRole $v) use ($database, $schema, $table) {
                $validSchema = Helper::generateFormattedQuoteCombinations($database, $schema, $table);
                return in_array($v->getName(), $validSchema);
            }
        );
    }

    /**
     * @return Generator<array{string, string, string}>
     */
    private static function lookupTriples(): Generator
    {
        foreach (self::withQuotedVariants(self::DATABASES) as $database) {
            foreach (self::withQuotedVariants(self::SCHEMAS) as $schema) {
                foreach (self::withQuotedVariants(self::TABLES) as $table) {
                    yield [$database, $schema, $table];
                }
            }
        }
        yield ['DB1', 'S1', 'MISSING'];
        yield ['MISSING', 'S1', 'T1'];
        yield ['D', 'B4.S1', 'T1'];
        yield ['D.B4.S1', 'a', 'b'];
    }

    /**
     * @param string[] $identifiers
     * @return string[]
     */
    private static function withQuotedVariants(array $identifiers): array
    {
        return array_values(array_unique(array_merge(
            $identifiers,
            array_map(fn(string $v) => Helper::quoteIdentifier($v), $identifiers),
        )));
    }

    private function createProjectRolesWithAllQuotingVariants(): ProjectRoles
    {
        mt_srand(20261110);
        $roleNames = ['ROLE_A', 'ROLE_B', 'ROLE_C', 'ROLE_D'];
        $grantsByRole = array_fill_keys($roleNames, []);
        $privileges = ['OWNERSHIP', 'SELECT', 'INSERT', 'UPDATE'];

        foreach (self::DATABASES as $database) {
            foreach (self::SCHEMAS as $schema) {
                foreach (self::TABLES as $table) {
                    $names = Helper::generateFormattedQuoteCombinations($database, $schema, $table);
                    $names[] = strtolower(sprintf('%s.%s.%s', $database, $schema, $table));
                    $names[] = sprintf('%s.%s.%s ', $database, $schema, $table);
                    foreach ($names as $name) {
                        if (mt_rand(0, 9) < 7) {
                            continue;
                        }
                        $roleName = $roleNames[mt_rand(0, count($roleNames) - 1)];
                        $grantsByRole[$roleName][] = self::createGrant(
                            $name,
                            $roleName,
                            $privileges[mt_rand(0, count($privileges) - 1)],
                        );
                    }
                }
            }
        }
        $grantsByRole['ROLE_A'][] = self::createGrant('DB1.S1.T1', 'ROLE_A', 'OWNERSHIP');
        $grantsByRole['ROLE_B'][] = self::createGrant('DB1.S1.T1', 'ROLE_B', 'SELECT');
        $grantsByRole['ROLE_C'][] = self::createGrant('"DB1"."S1"."T1"', 'ROLE_C', 'SELECT');
        $grantsByRole['ROLE_D'][] = self::createGrant('DB1.S1.T1', 'ROLE_D', 'SELECT');

        foreach ($grantsByRole as $roleName => $grants) {
            shuffle($grants);
            $grantsByRole[$roleName] = $grants;
        }

        $projectRoles = new ProjectRoles();
        foreach ($grantsByRole as $roleName => $grants) {
            $projectRoles->addRole(self::createRole($roleName, $grants));
        }
        return $projectRoles;
    }

    /**
     * @param GrantToRole[] $tableGrants
     */
    private static function createRole(string $name, array $tableGrants): Role
    {
        $roleGrants = new RoleGrants();
        foreach ($tableGrants as $grant) {
            $roleGrants->addTableGrant($grant);
        }
        $role = new Role($name, 'ACCOUNTADMIN');
        $role->setGrants($roleGrants);
        return $role;
    }

    private static function createGrant(string $name, string $roleName, string $privilege): GrantToRole
    {
        return new GrantToRole($name, $privilege, 'TABLE', 'ROLE', $roleName, 'false', 'ACCOUNTADMIN');
    }
}
