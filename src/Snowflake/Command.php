<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

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
        Connection $connection,
        string $mainRole,
        array $databases,
        array $grants
    ): void {
        foreach ($databases as $database) {
            [
                'databases' => $databaseGrants,
                'schemas' => $schemasGrants,
                'tables' => $tablesGrants,
                'roles' => $rolesGrants,
                'account' => $accountGrants,
                'warehouse' => $warehouseGrants,
                'other' => $otherGrants,
            ] = Helper::parseGrantsToObjects($grants[$database]);

            self::createRole($connection, ['name' => $database, 'granted_by' => $mainRole]);
            foreach ($accountGrants as $grant) {
                self::assignGrantToRole($connection, $grant);
            }

            foreach ($rolesGrants as $rolesGrant) {
                if ($rolesGrant['privilege'] === 'OWNERSHIP') {
                    self::createRole($connection, $rolesGrant);
                }
                self::assignGrantToRole($connection, $rolesGrant);
            }

            foreach ($warehouseGrants as $warehouseGrant) {
                self::assignGrantToRole($connection, $warehouseGrant);
            }

            self::useRole($connection, $mainRole);

            $shareDbName = $database . '_SHARE';

            $connection->query(sprintf(
                'CREATE DATABASE %s;',
                QueryBuilder::quoteIdentifier($database)
            ));

            foreach ($databaseGrants as $databaseGrant) {
                if ($databaseGrant['privilege'] === 'OWNERSHIP') {
                    self::assignGrantToRole(
                        $connection,
                        array_merge($databaseGrant, ['granted_by' => $mainRole])
                    );
                }
                self::assignGrantToRole($connection, $databaseGrant);
            }

            self::assignSharePrivilegesToRole($connection, $database, $database);

            self::useRole($connection, $database);

            $schemas = $connection->fetchAll(sprintf(
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
                        ];
                        return in_array($v['name'], $validSchema);
                    }
                );
                $ownershipOnSchema = current(array_filter($schemaGrants, fn($v) => $v['privilege'] === 'OWNERSHIP'));

                self::useRole($connection, $ownershipOnSchema['granted_by']);
                $connection->query(sprintf(
                    'CREATE SCHEMA %s.%s;',
                    QueryBuilder::quoteIdentifier($database),
                    QueryBuilder::quoteIdentifier($schemaName)
                ));

                foreach ($schemaGrants as $schemaGrant) {
                    self::assignGrantToRole($connection, $schemaGrant);
                }

                $tables = $connection->fetchAll(sprintf(
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
                            ];
                            return in_array($v['name'], $validSchema);
                        }
                    );
                    $ownershipOnTable = current(array_filter($tableGrants, fn($v) => $v['privilege'] === 'OWNERSHIP'));

                    self::assignSharePrivilegesToRole($connection, $database, $ownershipOnTable['granted_by']);
                    self::useRole($connection, $ownershipOnTable['granted_by']);

                    $connection->query(sprintf(
                        'CREATE TABLE %s.%s.%s AS SELECT * FROM %s.%s.%s;',
                        QueryBuilder::quoteIdentifier($database),
                        QueryBuilder::quoteIdentifier($schemaName),
                        QueryBuilder::quoteIdentifier($tableName),
                        QueryBuilder::quoteIdentifier($shareDbName),
                        QueryBuilder::quoteIdentifier($schemaName),
                        QueryBuilder::quoteIdentifier($tableName),
                    ));

                    foreach ($tableGrants as $tableGrant) {
                        self::assignGrantToRole($connection, $tableGrant);
                    }
                }
            }
        }
    }

    public static function createRole(Connection $connection, array $role): void
    {
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

    public static function exportGrantsUsersAndRoles(Connection $connection, array $databases): array
    {
        $tmp = [];
        foreach ($databases as $database) {
            $roles = $connection->fetchAll(sprintf(
                'SHOW ROLES LIKE %s',
                QueryBuilder::quote($database . '%')
            ));

            [
                'roles' => $roles,
                'users' => $users,
            ] = self::assignWorkspacesUsersAndRoles($connection, $roles, [$database]);

            foreach ($users as $user) {
                $tmp[$database]['users'][] = [
                    'name' => $user,
                    'assignedGrants' => $connection->fetchAll(sprintf(
                        'SHOW GRANTS TO USER %s;',
                        $user
                    )),
                ];
            }

            foreach ($roles as $role) {
                $tmp[$database]['roles'][] = array_merge(
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

    private static function assignWorkspacesUsersAndRoles(Connection $connection, array $roles, array $users): array
    {
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

            $usersInRole = array_filter($ownershipToRole, fn($v) => $v['granted_on'] === 'USER');
            $filteredUsersInRole = array_filter($usersInRole, fn($v) => !in_array($v['name'], $users));
            $users = array_merge($users, array_map(fn($v) => $v['name'], $filteredUsersInRole));
        }

        return ['roles' => $roles, 'users' => $users];
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
        Connection $connection,
        string $mainRole,
        string $warehouse,
        array $users
    ): void {
        $user = $mainRole;

        self::createRole($connection, ['name' => $mainRole]);

        $connection->query(sprintf(
            'DROP WAREHOUSE IF EXISTS %s',
            $warehouse
        ));

        $connection->query(sprintf(
            'GRANT CREATE DATABASE ON ACCOUNT TO ROLE %s;',
            $mainRole
        ));

        $connection->query(sprintf(
            'GRANT CREATE ROLE ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
            $mainRole
        ));

        $connection->query(sprintf(
            'GRANT CREATE USER ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
            $mainRole
        ));

        $createWarehouseSql = <<<SQL
CREATE WAREHOUSE %s WITH WAREHOUSE_SIZE = 'XSMALL' WAREHOUSE_TYPE = 'STANDARD' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE;
SQL;

        $connection->query(sprintf(
            $createWarehouseSql,
            $warehouse
        ));

        $connection->query(sprintf(
            'GRANT USAGE ON WAREHOUSE %s TO ROLE %s WITH GRANT OPTION;',
            $warehouse,
            $mainRole
        ));

        $connection->query(sprintf(
            'USE WAREHOUSE %s',
            $warehouse
        ));

        $connection->query(sprintf(
            'DROP USER IF EXISTS %s',
            $user
        ));

        $connection->query(sprintf(
            'CREATE USER %s PASSWORD=%s DEFAULT_ROLE=%s',
            $user,
            QueryBuilder::quoteIdentifier($users[$user]),
            $mainRole
        ));

        $connection->query(sprintf(
            'GRANT ROLE %s TO USER %s;',
            $mainRole,
            $user
        ));

        $connection->query(sprintf(
            'GRANT ROLE %s TO ROLE SYSADMIN;',
            $mainRole
        ));
    }

    public static function cleanupProject(Connection $connection): void
    {
        try {
            self::useRole($connection, 'SAPI_9472');
            $dropRoles = [
                'SAPI_9472_1073748_SHARE',
                'SAPI_9472_1073763_SHARE',
                'SAPI_9472_1075089_SHARE',
                'SAPI_9472_RO',
                'SAPI_WORKSPACE_941797557',
                'SAPI_WORKSPACE_942116815',
            ];

            foreach ($dropRoles as $dropRole) {
                $connection->query(sprintf(
                    'DROP ROLE IF EXISTS %s',
                    $dropRole
                ));
            }
        } catch (CannotAccessObjectException $e) {
            var_dump($e->getMessage());
        }

        self::useRole($connection, 'ACCOUNTADMIN');
        $dropRoles = [
            'SAPI_9472',
            'SAPI_9473',
            'KEBOOLA_STORAGE',
        ];

        foreach ($dropRoles as $dropRole) {
            $connection->query(sprintf(
                'DROP ROLE IF EXISTS %s',
                $dropRole
            ));
        }

        $databases = [
            'SAPI_9472',
            'SAPI_9473',
        ];
        foreach ($databases as $database) {
            $connection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($database)
            ));
        }
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

    public static function getMainRole(Connection $connection, array $databases): string
    {
        $grantsOfRoles = [];
        foreach ($databases as $database) {
            $grantsOfRole = $connection->fetchAll(sprintf(
                'SHOW GRANTS OF ROLE %s',
                $database
            ));
            $grantedByRole = array_map(fn($v) => $v['granted_by'], $connection->fetchAll(sprintf(
                'SHOW GRANTS OF ROLE %s',
                $database
            )));
            $grantsOfRoles = array_merge($grantsOfRoles, array_unique($grantedByRole));
        }

        $uniqueMainRoles = array_unique($grantsOfRoles);

        assert(count($uniqueMainRoles) === 1);

        return current($uniqueMainRoles);
    }
}
