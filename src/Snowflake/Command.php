<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use Psr\Log\LoggerInterface;

class Command
{

    private const MIGRATION_SHARE_PREFIX = 'MIGRATION_SHARE_';

    private const SKIP_CLONE_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];

    public static function useRole(Connection $connection, string $role): void
    {
        $connection->query(sprintf('USE ROLE "%s";', $role));
    }

    public static function getRegion(Connection $connection): string
    {
        $region = $connection->fetchAll('SELECT CURRENT_REGION() AS "region";');

        return $region[0]['region'];
    }

    public static function getAccount(Connection $connection): string
    {
        $account = $connection->fetchAll('SELECT CURRENT_ACCOUNT() AS "account";');

        return $account[0]['account'];
    }

    public static function createShare(Connection $connection, array $databases, string $destinationAccount): void
    {
        foreach ($databases as $database) {
            $shareName = sprintf('%s%s', self::MIGRATION_SHARE_PREFIX, strtoupper($database));

            $connection->query(sprintf(
                'DROP SHARE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'CREATE SHARE %s;',
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON DATABASE %s TO SHARE %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON ALL SCHEMAS IN DATABASE %s TO SHARE %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT SELECT ON ALL TABLES IN DATABASE %s TO SHARE %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'ALTER SHARE %s ADD ACCOUNT=%s;',
                QueryBuilder::quoteIdentifier($shareName),
                $destinationAccount
            ));
        }
    }

    public static function createDatabasesFromShares(
        Connection $connection,
        array $databases,
        string $sourceAccount
    ): void {
        foreach ($databases as $database) {
            $shareDbName = $database . '_SHARE';

            $connection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($shareDbName)
            ));

            $connection->query(sprintf(
                'CREATE DATABASE %s FROM SHARE IDENTIFIER(\'%s.%s\');',
                QueryBuilder::quoteIdentifier($shareDbName),
                $sourceAccount,
                self::MIGRATION_SHARE_PREFIX . $database
            ));
        }
    }

    public static function cloneDatabaseFromShared(
        LoggerInterface $logger,
        Config $config,
        Connection $sourceConnection,
        Connection $destinationConnection,
        string $mainRole,
        array $databases,
        array $grants
    ): void {
        foreach ($databases as $database) {
            $databaseRole = self::getMainRoleOnDatabase($sourceConnection, $database);
            [
                'databases' => $databaseGrants,
                'schemas' => $schemasGrants,
                'tables' => $tablesGrants,
                'roles' => $rolesGrants,
                'account' => $accountGrants,
                'warehouse' => $warehouseGrants,
                'user' => $userGrants,
                'other' => $otherGrants,
            ] = Helper::parseGrantsToObjects($grants[$databaseRole]);

            self::createRole(
                $destinationConnection,
                [
                    'name' => $databaseRole,
                    'granted_by' => $mainRole,
                    'privilege' => 'OWNERSHIP',
                ]
            );
            foreach ($accountGrants as $grant) {
                self::assignGrantToRole($destinationConnection, $grant);
            }

            foreach ($rolesGrants as $rolesGrant) {
                if ($rolesGrant['privilege'] === 'OWNERSHIP') {
                    self::createRole($destinationConnection, $rolesGrant);
                }
                self::assignGrantToRole($destinationConnection, $rolesGrant);
            }

            foreach ($userGrants as $userGrant) {
                self::createUser($logger, $destinationConnection, $userGrant, $config->getPasswordOfUsers());
                self::assignGrantToRole($destinationConnection, $userGrant);
            }

            foreach ($warehouseGrants as $warehouseGrant) {
                self::assignGrantToRole($destinationConnection, $warehouseGrant);
            }

            self::useRole($destinationConnection, $mainRole);

            $shareDbName = $database . '_SHARE';

            $destinationConnection->query(sprintf(
                'CREATE DATABASE %s;',
                QueryBuilder::quoteIdentifier($database)
            ));

            foreach ($databaseGrants as $databaseGrant) {
                if ($databaseGrant['privilege'] === 'OWNERSHIP') {
                    self::assignGrantToRole(
                        $destinationConnection,
                        array_merge($databaseGrant, ['granted_by' => $mainRole])
                    );
                }
                self::assignGrantToRole($destinationConnection, $databaseGrant);
            }

            self::assignSharePrivilegesToRole($destinationConnection, $database, $databaseRole);

            self::useRole($destinationConnection, $databaseRole);

            $schemas = $destinationConnection->fetchAll(sprintf(
                'SHOW SCHEMAS IN DATABASE %s;',
                QueryBuilder::quoteIdentifier($shareDbName)
            ));

            foreach ($schemas as $k => $schema) {
                if (in_array($schema['name'], self::SKIP_CLONE_SCHEMAS)) {
                    continue;
                }
                $schemaName = $schema['name'];

                $schemaGrants = array_filter(
                    $schemasGrants,
                    function (array $v) use ($database, $schemaName) {
                        $validSchema = [
                            sprintf('%s.%s', $database, $schemaName),
                            sprintf('%s.%s', $database, QueryBuilder::quoteIdentifier($schemaName)),
                            sprintf('%s.%s', QueryBuilder::quoteIdentifier($database), $schemaName),
                            sprintf(
                                '%s.%s',
                                QueryBuilder::quoteIdentifier($database),
                                QueryBuilder::quoteIdentifier($schemaName)
                            ),
                        ];
                        return in_array($v['name'], $validSchema);
                    }
                );
                $ownershipOnSchema = array_filter($schemaGrants, fn($v) => $v['privilege'] === 'OWNERSHIP');
                assert(count($ownershipOnSchema) === 1);

                self::useRole($destinationConnection, current($ownershipOnSchema)['granted_by']);
                $destinationConnection->query(sprintf(
                    'CREATE SCHEMA %s.%s;',
                    QueryBuilder::quoteIdentifier($database),
                    QueryBuilder::quoteIdentifier($schemaName)
                ));

                foreach ($schemaGrants as $schemaGrant) {
                    self::assignGrantToRole($destinationConnection, $schemaGrant);
                }

                $tables = $destinationConnection->fetchAll(sprintf(
                    'SHOW TABLES IN SCHEMA %s.%s;',
                    QueryBuilder::quoteIdentifier($shareDbName),
                    QueryBuilder::quoteIdentifier($schemaName)
                ));

                foreach ($tables as $table) {
                    $tableName = $table['name'];

                    $tableGrants = array_filter(
                        $tablesGrants,
                        function (array $v) use ($database, $schemaName, $tableName) {
                            $validSchema = [
                                sprintf('%s.%s.%s', $database, $schemaName, $tableName),
                                sprintf('%s.%s.%s', $database, $schemaName, QueryBuilder::quoteIdentifier($tableName)),
                                sprintf('%s.%s.%s', $database, QueryBuilder::quoteIdentifier($schemaName), $tableName),
                                sprintf(
                                    '%s.%s.%s',
                                    $database,
                                    QueryBuilder::quoteIdentifier($schemaName),
                                    QueryBuilder::quoteIdentifier($tableName)
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    QueryBuilder::quoteIdentifier($database),
                                    $schemaName,
                                    $tableName
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    QueryBuilder::quoteIdentifier($database),
                                    $schemaName,
                                    QueryBuilder::quoteIdentifier($tableName)
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    QueryBuilder::quoteIdentifier($database),
                                    QueryBuilder::quoteIdentifier($schemaName),
                                    $tableName
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    QueryBuilder::quoteIdentifier($database),
                                    QueryBuilder::quoteIdentifier($schemaName),
                                    QueryBuilder::quoteIdentifier($tableName)
                                ),
                            ];
                            return in_array($v['name'], $validSchema);
                        }
                    );
                    $ownershipOnTable = array_filter($tableGrants, fn($v) => $v['privilege'] === 'OWNERSHIP');
                    assert(count($ownershipOnTable) === 1);

                    $ownershipOnTable = current($ownershipOnTable);

                    self::assignSharePrivilegesToRole(
                        $destinationConnection,
                        $database,
                        $ownershipOnTable['granted_by']
                    );
                    self::useRole($destinationConnection, $ownershipOnTable['granted_by']);

                    $destinationConnection->query(sprintf(
                        'CREATE TABLE %s.%s.%s AS SELECT * FROM %s.%s.%s;',
                        QueryBuilder::quoteIdentifier($database),
                        QueryBuilder::quoteIdentifier($schemaName),
                        QueryBuilder::quoteIdentifier($tableName),
                        QueryBuilder::quoteIdentifier($shareDbName),
                        QueryBuilder::quoteIdentifier($schemaName),
                        QueryBuilder::quoteIdentifier($tableName),
                    ));

                    foreach ($tableGrants as $tableGrant) {
                        self::assignGrantToRole($destinationConnection, $tableGrant);
                    }
                }
            }
        }
    }

    public static function createUser(
        LoggerInterface $logger,
        Connection $connection,
        array $userGrant,
        array $passwordOfUsers
    ): void {
        self::useRole($connection, $userGrant['granted_by']);

        if (isset($passwordOfUsers[$userGrant['name']])) {
            $connection->query(sprintf(
                'CREATE USER %s PASSWORD=\'%s\' DEFAULT_ROLE = %s',
                $userGrant['name'],
                $passwordOfUsers[$userGrant['name']],
                $userGrant['name'],
            ));
        } else {
            $password = Helper::generateRandomString();
            $logger->alert(sprintf(
                'User "%s" has been created with password "%s". Please change it immediately!',
                $userGrant['name'],
                $password
            ));
            $connection->query(sprintf(
                'CREATE USER %s PASSWORD=\'%s\' DEFAULT_ROLE = %s MUST_CHANGE_PASSWORD = true',
                $userGrant['name'],
                $password,
                $userGrant['name'],
            ));
        }
    }

    public static function createRole(Connection $connection, array $role): void
    {
        assert($role['privilege'] === 'OWNERSHIP');

        if (isset($role['granted_by'])) {
            self::useRole($connection, $role['granted_by']);
        }

        $connection->query(sprintf(
            'CREATE ROLE %s',
            $role['name']
        ));

        self::grantRoleToUser(
            $connection,
            (string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'),
            $role['name']
        );
    }

    public static function exportUsersAndRolesGrants(Connection $connection, array $databases): array
    {
        $tmp = [];
        foreach ($databases as $database) {
            $databaseRole = self::getMainRoleOnDatabase($connection, $database);

            $roles = $connection->fetchAll(sprintf(
                'SHOW ROLES LIKE %s',
                QueryBuilder::quote($databaseRole)
            ));

            $roles = self::getOtherRolesToMainProjectRole($connection, $roles);

            foreach ($roles as $role) {
                $tmp[$databaseRole][] = array_merge(
                    $role,
                    [
                        'assignedGrants' => $connection->fetchAll(sprintf(
                            'SHOW GRANTS TO ROLE %s;',
                            $role['name']
                        )),
                        'assignedFutureGrants' => $connection->fetchAll(sprintf(
                            'SHOW FUTURE GRANTS TO ROLE %s;',
                            $role['name']
                        )),
                    ]
                );
            }
        }
        return $tmp;
    }

    private static function getOtherRolesToMainProjectRole(
        Connection $connection,
        array $roles
    ): array {
        foreach ($roles as $role) {
            $grantsToRole = $connection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s;',
                $role['name']
            ));

            $ownershipToRole = array_filter($grantsToRole, fn($v) => $v['privilege'] === 'OWNERSHIP');

            $rolesInRole = array_filter($ownershipToRole, fn($v) => $v['granted_on'] === 'ROLE');
            $filteredRolesInRole = array_filter($rolesInRole, fn($v) => !in_array($v['name'], $roles));
            $roles = array_merge($roles, array_combine(
                array_map(fn($v) => $v['name'], $filteredRolesInRole),
                $filteredRolesInRole
            ));
        }

        return $roles;
    }

    public static function grantRoleToUser(Connection $connection, string $user, string $role): void
    {
        $connection->query(sprintf(
            'GRANT ROLE %s TO USER %s',
            QueryBuilder::quoteIdentifier($role),
            QueryBuilder::quoteIdentifier($user)
        ));
    }

    public static function assignGrantToRole(Connection $connection, array $grant): void
    {
        self::useRole($connection, $grant['granted_by']);

        if ($grant['privilege'] === 'USAGE' && $grant['granted_on'] === 'ROLE') {
            $sql = sprintf(
                'GRANT %s %s TO %s %s %s',
                $grant['granted_on'],
                $grant['name'],
                $grant['granted_to'],
                $grant['grantee_name'],
                $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
            );
        } else {
            $sql = sprintf(
                'GRANT %s ON %s %s TO %s %s %s',
                $grant['privilege'],
                $grant['granted_on'],
                $grant['granted_on'] !== 'ACCOUNT' ? $grant['name'] : '',
                $grant['granted_to'],
                $grant['grantee_name'],
                $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
            );
        }

        $connection->query($sql);
    }

    public static function createMainRole(
        LoggerInterface $logger,
        Connection $connectionSourceProject,
        Connection $connectionDestinationProject,
        array $mainRoleWithGrants,
        array $databases,
        array $users
    ): void {
        $user = $mainRole = $mainRoleWithGrants['name'];

        self::createRole($connectionDestinationProject, ['name' => $mainRole, 'privilege' => 'OWNERSHIP']);

        $mainRoleGrants = [
            'GRANT CREATE DATABASE ON ACCOUNT TO ROLE %s;',
            'GRANT CREATE ROLE ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
            'GRANT CREATE USER ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
        ];

        foreach ($mainRoleGrants as $mainRoleGrant) {
            $connectionDestinationProject->query(sprintf($mainRoleGrant, $mainRole));
        }

        $warehouses = array_filter($mainRoleWithGrants['assignedGrants'], fn($v) => $v['granted_on'] === 'WAREHOUSE');
        $useWarehouse = false;
        foreach ($warehouses as $warehouse) {
            $warehouseSize = self::createWarehouse(
                $connectionSourceProject,
                $connectionDestinationProject,
                $warehouse
            );
            self::assignGrantToRole($connectionDestinationProject, $warehouse);

            if ($useWarehouse === false || $warehouseSize === 'X-Small') {
                $useWarehouse = $warehouse;
            }
        }

        $connectionDestinationProject->query(sprintf(
            'USE WAREHOUSE %s',
            QueryBuilder::quoteIdentifier($useWarehouse['name'])
        ));

        $connectionDestinationProject->query(sprintf(
            'CREATE USER %s PASSWORD=%s DEFAULT_ROLE=%s',
            $user,
            QueryBuilder::quote($users[$user]),
            $mainRole
        ));

        $connectionDestinationProject->query(sprintf(
            'GRANT ROLE %s TO USER %s;',
            $mainRole,
            $user
        ));

        $projectUsers = array_filter($mainRoleWithGrants['assignedGrants'], function ($v) use ($databases) {
            if ($v['privilege'] !== 'OWNERSHIP') {
                return false;
            }
            if ($v['granted_on'] !== 'USER') {
                return false;
            }
            return in_array($v['name'], $databases);
        });

        foreach ($projectUsers as $projectUser) {
            self::createUser($logger, $connectionDestinationProject, $projectUser, $users);
            self::assignGrantToRole($connectionDestinationProject, $projectUser);
        }

        self::useRole($connectionDestinationProject, 'ACCOUNTADMIN');

        $connectionDestinationProject->query(sprintf(
            'GRANT ROLE %s TO ROLE SYSADMIN;',
            $mainRole
        ));
    }

    public static function cleanupProject(Connection $connection): void
    {
        self::useRole($connection, 'ACCOUNTADMIN');

        $databases = [
            'SAPI_9472_OLD',
            'SAPI_9473_OLD',
        ];
        foreach ($databases as $database) {
            $connection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($database)
            ));
        }

        $warehouses = [
            'MIGRATE',
            'MIGRATE_SMALL',
            'MIGRATE_MEDIUM',
            'MIGRATE_LARGE',
        ];

        foreach ($warehouses as $warehouse) {
            $connection->query(sprintf(
                'DROP WAREHOUSE IF EXISTS %s',
                $warehouse
            ));
        }

        $connection->query('DROP ROLE IF EXISTS KEBOOLA_STORAGE');
        $connection->query('DROP USER IF EXISTS KEBOOLA_STORAGE');
    }

    private static function assignSharePrivilegesToRole(Connection $connection, string $database, string $role): void
    {
        self::assignGrantToRole($connection, [
            'privilege' => 'IMPORTED PRIVILEGES',
            'granted_on' => 'DATABASE',
            'name' => $database . '_SHARE',
            'granted_to' => 'ROLE',
            'grantee_name' => $role,
            'grant_option' => 'false',
            'granted_by' => 'ACCOUNTADMIN',
        ]);
    }

    public static function getMainRoleWithGrants(Connection $connection, array $databases): array
    {
        $grantsOfRoles = [];
        foreach ($databases as $database) {
            $roleName = self::getMainRoleOnDatabase($connection, $database);

            $grantedOnDatabaseRole = $connection->fetchAll(sprintf(
                'SHOW GRANTS ON ROLE %s',
                $roleName
            ));

            $ownershipOfRole = array_filter($grantedOnDatabaseRole, fn($v) => $v['privilege'] === 'OWNERSHIP');

            $ownershipOfRole = array_map(fn($v) => $v['grantee_name'], $ownershipOfRole);

            $grantsOfRoles = array_merge($grantsOfRoles, array_unique($ownershipOfRole));
        }

        $uniqueMainRoles = array_unique($grantsOfRoles);

        assert(count($uniqueMainRoles) === 1);

        $mainRole = current($uniqueMainRoles);

        return [
            'name' => $mainRole,
            'assignedGrants' => $connection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s;',
                $mainRole
            )),
        ];
    }

    private static function createWarehouse(
        Connection $connectionSourceProject,
        Connection $connectionDestinationProject,
        array $warehouse
    ): string {
        $warehouseInfo = $connectionSourceProject->fetchAll(sprintf(
            'SHOW WAREHOUSES LIKE %s',
            QueryBuilder::quote($warehouse['name'])
        ));
        assert(count($warehouseInfo) === 1);

        $warehouseInfo = current($warehouseInfo);

        $sqlTemplate = <<<SQL
CREATE WAREHOUSE %s
    WITH WAREHOUSE_SIZE = %s
        WAREHOUSE_TYPE = %s
        AUTO_SUSPEND = %s
        AUTO_RESUME = %s
        %s
        %s
;
SQL;

        $sql = sprintf(
            $sqlTemplate,
            QueryBuilder::quoteIdentifier($warehouseInfo['name']),
            QueryBuilder::quote($warehouseInfo['size']),
            QueryBuilder::quote($warehouseInfo['type']),
            $warehouseInfo['auto_suspend'],
            $warehouseInfo['auto_resume'],
            isset($warehouseInfo['min_cluster_count']) ?
                'MIN_CLUSTER_COUNT = ' . $warehouseInfo['min_cluster_count'] :
                '',
            isset($warehouseInfo['max_cluster_count']) ?
                'MAX_CLUSTER_COUNT = ' . $warehouseInfo['max_cluster_count'] :
                '',
        );

        $connectionDestinationProject->query($sql);

        return $warehouseInfo['size'];
    }

    public static function grantRoleToUsers(
        Connection $sourceConnection,
        Connection $destinationConnection,
        string $mainUser
    ): void {
        self::useRole($destinationConnection, 'ACCOUNTADMIN');
        self::useRole($sourceConnection, 'ACCOUNTADMIN');

        $users = $destinationConnection->fetchAll('SHOW USERS');

        $filteredUsers = array_filter($users, function ($v) use ($mainUser) {
            if ($v['owner'] === 'ACCOUNTADMIN' && $v['name'] !== $mainUser) {
                return false;
            }
            if ($v['owner'] === '') {
                return false;
            }
            return true;
        });

        foreach ($filteredUsers as $filteredUser) {
            $grants = $sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS TO USER %s',
                QueryBuilder::quoteIdentifier($filteredUser['name'])
            ));

            foreach ($grants as $grant) {
                self::useRole($destinationConnection, $grant['granted_by']);
                $destinationConnection->query(sprintf(
                    'GRANT ROLE %s TO %s %s',
                    $grant['role'],
                    $grant['granted_to'],
                    $grant['grantee_name'],
                ));
            }
        }
    }

    private static function getMainRoleOnDatabase(Connection $connection, string $database): string
    {
        $grantsOnDatabase = $connection->fetchAll(sprintf(
            'SHOW GRANTS ON DATABASE %s',
            $database
        ));

        $ownershipOnDatabase = array_filter($grantsOnDatabase, fn($v) => $v['privilege'] === 'OWNERSHIP');
        assert(count($ownershipOnDatabase) === 1);

        return current($ownershipOnDatabase)['grantee_name'];
    }

    public static function cleanupAccount(
        LoggerInterface $logger,
        Connection $connection,
        array $databases,
        bool $dryRun = true
    ): void {
        $sqls = [];
        $currentRole = 'ACCOUNTADMIN';
        foreach ($databases as $database) {
            $databaseRole = self::getMainRoleOnDatabase($connection, $database);
            $data = self::getDataToRemove($connection, $databaseRole);

            foreach ($data['USER'] ?? [] as $user) {
                if ($user['granted_by'] !== $currentRole) {
                    $currentRole = $user['granted_by'];
                    $sqls[] = sprintf('USE ROLE %s;', $currentRole);
                }
                $sqls[] = sprintf('DROP USER IF EXISTS %s;', $user['name']);
            }

            foreach ($data['ROLE'] ?? [] as $user) {
                if ($user['granted_by'] !== $currentRole) {
                    $currentRole = $user['granted_by'];
                    $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
                }
                $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', $user['name']);
            }

            $grantsOfRole = $connection->fetchAll(sprintf('SHOW GRANTS OF ROLE %s', $databaseRole));
            $filteredGrantsOfRole = array_filter(
                $grantsOfRole,
                fn($v) => strtoupper($v['grantee_name']) === strtoupper($databaseRole)
            );
            assert(count($filteredGrantsOfRole) === 1);
            $grantOfRole = current($grantsOfRole);
            if ($grantOfRole['granted_by'] !== $currentRole) {
                $currentRole = $grantOfRole['granted_by'];
                $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
            }
            $sqls[] = sprintf(
                'DROP USER IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($grantOfRole['grantee_name'])
            );
            $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', QueryBuilder::quoteIdentifier($databaseRole));

            if ($currentRole !== 'ACCOUNTADMIN') {
                $currentRole = 'ACCOUNTADMIN';
                $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
            }
            $sqls[] = sprintf(
                'ALTER DATABASE IF EXISTS %s RENAME TO %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($database . '_OLD'),
            );
        }

        foreach ($sqls as $sql) {
            if ($dryRun) {
                $logger->info($sql);
            } else {
                $connection->query($sql);
            }
        }
    }

    private static function getDataToRemove(Connection $connection, string $role): array
    {
        $grants = $connection->fetchAll(sprintf(
            'SHOW GRANTS TO ROLE %s',
            QueryBuilder::quoteIdentifier($role)
        ));

        $filteredGrants = array_filter($grants, function ($v) {
            $usageWarehouse = $v['privilege'] === 'USAGE' && $v['granted_on'] === 'WAREHOUSE';
            $ownership = $v['privilege'] === 'OWNERSHIP' && (in_array($v['granted_on'], ['USER', 'ROLE']));

            return $ownership || $usageWarehouse;
        });

        $mapGrants = [];
        foreach ($filteredGrants as $filteredGrant) {
            $mapGrants[$filteredGrant['granted_on']][$filteredGrant['name']] = $filteredGrant;
        }

        if (isset($mapGrants['ROLE'])) {
            $roleGrants = $mapGrants['ROLE'];
            foreach ($roleGrants as $roleGrant) {
                $mapGrants = array_merge_recursive(
                    self::getDataToRemove($connection, $roleGrant['name']),
                    $mapGrants,
                );
            }
        }

        return $mapGrants;
    }
}
