<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Keboola\Component\UserException;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\MetadataFetcher;
use ProjectMigrationTool\Snowflake\Connection;

class MetadataFetcherTest extends TestCase
{
    public function testMainRoleNotVisibleOnSourceEndsWithUserException(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('getOwnershipRoleOnDatabase')
            ->with('DB1')
            ->willReturn('KEBOOLA_1');
        $expectedQueries = [
            [
                'sql' => 'SHOW GRANTS ON ROLE "KEBOOLA_1"',
                'rows' => [
                    FakeSnowflakeAccount::grantToRole('OWNERSHIP', 'ROLE', 'KEBOOLA_1', 'KEBOOLA_STORAGE'),
                ],
            ],
            [
                'sql' => "SHOW ROLES LIKE 'KEBOOLA_STORAGE'",
                'rows' => [FakeSnowflakeAccount::role('keboola_storage', 'ACCOUNTADMIN')],
            ],
        ];
        $connection->expects(self::exactly(count($expectedQueries)))
            ->method('fetchAll')
            ->willReturnCallback(function (string $sql) use (&$expectedQueries): array {
                $expected = array_shift($expectedQueries);
                self::assertNotNull($expected);
                self::assertSame($expected['sql'], $sql);
                return $expected['rows'];
            });

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Role "KEBOOLA_STORAGE" not found on the source account. The source role "SOURCE_MAIN_MIGRATE" '
            . 'probably cannot see it, for example because it does not have the MANAGE GRANTS privilege.',
        );
        (new MetadataFetcher($connection, $this->createConfig()))->getMainRoleWithGrants();
    }

    public function testProjectRoleNotVisibleOnSourceEndsWithUserException(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('getOwnershipRoleOnDatabase')
            ->with('DB1')
            ->willReturn('KEBOOLA_1');
        $connection->expects(self::once())
            ->method('fetchAll')
            ->with("SHOW ROLES LIKE 'KEBOOLA_1'")
            ->willReturn([]);

        $this->expectException(UserException::class);
        $this->expectExceptionMessage(
            'Role "KEBOOLA_1" not found on the source account. The source role "SOURCE_MAIN_MIGRATE" '
            . 'probably cannot see it, for example because it does not have the MANAGE GRANTS privilege.',
        );
        (new MetadataFetcher($connection, $this->createConfig()))->getRolesWithGrants();
    }

    private function createConfig(): Config
    {
        return new Config(
            [
                'parameters' => [
                    'action' => Config::ACTION_CHECK,
                    'migrateDatabases' => ['DB1'],
                    'credentials' => [
                        'source' => [
                            'host' => 'host',
                            'username' => 'SOURCE_MIGRATE',
                            '#password' => 'password',
                            'warehouse' => 'WH',
                            'role' => 'SOURCE_MAIN_MIGRATE',
                        ],
                        'target' => [
                            'host' => 'host',
                            'username' => 'TARGET_MIGRATE',
                            '#password' => 'password',
                            'warehouse' => 'WH',
                            'role' => 'TARGET_MAIN_MIGRATE',
                        ],
                    ],
                ],
            ],
            new ConfigDefinition(),
        );
    }
}
