<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\Helper;
use Psr\Log\LoggerInterface;

class Migrate
{
    private LoggerInterface $logger;

    private Connection $sourceConnection;

    private Connection $migrateConnection;

    private Connection $destinationConnection;

    private array $databases;

    private string $mainMigrationRole;

    private const MIGRATION_SHARE_PREFIX = 'MIGRATION_SHARE_';

    private const SKIP_CLONE_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];

    private array $usedUsers = [];

    public function __construct(
        LoggerInterface $logger,
        Connection $sourceConnection,
        Connection $migrateConnection,
        Connection $destinationConnection,
        array $databases,
        string $mainMigrationRole
    ) {
        $this->logger = $logger;
        $this->sourceConnection = $sourceConnection;
        $this->migrateConnection = $migrateConnection;
        $this->destinationConnection = $destinationConnection;
        $this->databases = $databases;
        $this->mainMigrationRole = $mainMigrationRole;
    }

    public function createReplication(): void
    {
        foreach ($this->databases as $database) {
            //            Allow replication on source database
            $this->sourceConnection->query(sprintf(
                'ALTER DATABASE %s ENABLE REPLICATION TO ACCOUNTS %s.%s;',
                QueryBuilder::quoteIdentifier($database),
                $this->migrateConnection->getRegion(),
                $this->migrateConnection->getAccount()
            ));

            //            Waiting for previous SQL query
            sleep(1);

            //            Migration database sqls
            $this->migrateConnection->query(sprintf(
                'CREATE DATABASE IF NOT EXISTS %s AS REPLICA OF %s.%s.%s;',
                QueryBuilder::quoteIdentifier($database),
                $this->sourceConnection->getRegion(),
                $this->sourceConnection->getAccount(),
                QueryBuilder::quoteIdentifier($database)
            ));

            $this->migrateConnection->query(sprintf(
                'USE DATABASE %s',
                QueryBuilder::quoteIdentifier($database)
            ));

            $this->migrateConnection->query('USE SCHEMA PUBLIC');

            //            Create and use warehouse for replicate data
            $sql = <<<SQL
CREATE WAREHOUSE IF NOT EXISTS "migrate"
    WITH WAREHOUSE_SIZE = 'Small'
        WAREHOUSE_TYPE = 'STANDARD'
        AUTO_SUSPEND = 300
        AUTO_RESUME = true
;
SQL;
            $this->migrateConnection->query($sql);

            $this->migrateConnection->query('USE WAREHOUSE "migrate";');

            //            Run replicate of data
            $this->migrateConnection->query(sprintf(
                'ALTER DATABASE %s REFRESH',
                QueryBuilder::quoteIdentifier($database)
            ));
        }
    }

    public function createShare(): void
    {
        $sourceRegion = $this->sourceConnection->getRegion();
        $destinationRegion = $this->destinationConnection->getRegion();

        $connection = $this->sourceConnection;
        if ($sourceRegion !== $destinationRegion) {
            $connection = $this->migrateConnection;
        }

        foreach ($this->databases as $database) {
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
                $this->destinationConnection->getAccount()
            ));
        }
    }

    public function createDatabasesFromShares(): void
    {
        $sourceRegion = $this->sourceConnection->getRegion();
        $destinationRegion = $this->destinationConnection->getRegion();

        foreach ($this->databases as $database) {
            $shareDbName = $database . '_SHARE';

            $this->destinationConnection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($shareDbName)
            ));

            $this->destinationConnection->query(sprintf(
                'CREATE DATABASE %s FROM SHARE IDENTIFIER(\'%s.%s\');',
                QueryBuilder::quoteIdentifier($shareDbName),
                $sourceRegion !== $destinationRegion ?
                    $this->migrateConnection->getAccount() :
                    $this->sourceConnection->getAccount(),
                self::MIGRATION_SHARE_PREFIX . $database
            ));
        }
    }

    public function cloneDatabaseWithGrants(
        Config $config,
        string $mainRole,
        array $grants,
        bool $isSynchronizeRun
    ): void {
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
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

            $this->destinationConnection->createRole([
                'name' => $databaseRole,
                'granted_by' => $mainRole,
                'privilege' => 'OWNERSHIP',
            ]);
            foreach ($accountGrants as $grant) {
                $this->destinationConnection->assignGrantToRole($grant);
            }

            if ($isSynchronizeRun) {
                $this->grantsPrivilegesToOldDatabase($database, $databaseRole);
            }

            foreach ($rolesGrants as $rolesGrant) {
                if ($rolesGrant['privilege'] === 'OWNERSHIP') {
                    $this->destinationConnection->createRole($rolesGrant);
                    if ($isSynchronizeRun) {
                        $this->grantsPrivilegesToOldDatabase($database, $rolesGrant['name']);
                    }
                }
                $this->destinationConnection->assignGrantToRole($rolesGrant);
            }

            foreach ($userGrants as $userGrant) {
                $this->createUser($userGrant, $config->getPasswordOfUsers());
                $this->destinationConnection->assignGrantToRole($userGrant);
            }

            foreach ($warehouseGrants as $warehouseGrant) {
                $this->destinationConnection->assignGrantToRole($warehouseGrant);
            }

            $this->destinationConnection->useRole($mainRole);

            $shareDbName = $database . '_SHARE';
            $oldDbName = $database . '_OLD';

            $this->destinationConnection->query(sprintf(
                'CREATE DATABASE %s;',
                QueryBuilder::quoteIdentifier($database)
            ));

            foreach ($databaseGrants as $databaseGrant) {
                if ($databaseGrant['privilege'] === 'OWNERSHIP') {
                    $this->destinationConnection->assignGrantToRole(array_merge(
                        $databaseGrant,
                        ['granted_by' => $mainRole]
                    ));
                }
                $this->destinationConnection->assignGrantToRole($databaseGrant);
            }

            self::assignSharePrivilegesToRole($database, $databaseRole);

            $this->destinationConnection->useRole($databaseRole);

            $schemas = $this->destinationConnection->fetchAll(sprintf(
                'SHOW SCHEMAS IN DATABASE %s;',
                QueryBuilder::quoteIdentifier($shareDbName)
            ));

            foreach ($schemas as $schema) {
                if (in_array($schema['name'], self::SKIP_CLONE_SCHEMAS)) {
                    continue;
                }
                $schemaName = $schema['name'];

                $this->logger->info(sprintf('Migrate schema "%s".', $schemaName));

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

                $this->destinationConnection->useRole(current($ownershipOnSchema)['granted_by']);
                $this->destinationConnection->query(sprintf(
                    'CREATE SCHEMA %s.%s;',
                    QueryBuilder::quoteIdentifier($database),
                    QueryBuilder::quoteIdentifier($schemaName)
                ));

                foreach ($schemaGrants as $schemaGrant) {
                    $this->destinationConnection->assignGrantToRole($schemaGrant);
                }

                $tables = $this->destinationConnection->fetchAll(sprintf(
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
                        $database,
                        $ownershipOnTable['granted_by']
                    );
                    $this->destinationConnection->useRole($ownershipOnTable['granted_by']);

                    if ($this->canCloneTable($database, $schemaName, $tableName)) {
                        $this->logger->info(sprintf('Clone table "%s" from OLD database', $tableName));
                        $this->destinationConnection->query(sprintf(
                            'CREATE TABLE %s.%s.%s CLONE %s.%s.%s;',
                            QueryBuilder::quoteIdentifier($database),
                            QueryBuilder::quoteIdentifier($schemaName),
                            QueryBuilder::quoteIdentifier($tableName),
                            QueryBuilder::quoteIdentifier($oldDbName),
                            QueryBuilder::quoteIdentifier($schemaName),
                            QueryBuilder::quoteIdentifier($tableName),
                        ));
                    } else {
                        $this->logger->info(sprintf('Create table "%s" from SHARE database', $tableName));
                        $this->destinationConnection->query(sprintf(
                            'CREATE TABLE %s.%s.%s AS SELECT * FROM %s.%s.%s;',
                            QueryBuilder::quoteIdentifier($database),
                            QueryBuilder::quoteIdentifier($schemaName),
                            QueryBuilder::quoteIdentifier($tableName),
                            QueryBuilder::quoteIdentifier($shareDbName),
                            QueryBuilder::quoteIdentifier($schemaName),
                            QueryBuilder::quoteIdentifier($tableName),
                        ));
                    }

                    foreach ($tableGrants as $tableGrant) {
                        $this->destinationConnection->assignGrantToRole($tableGrant);
                    }
                }
            }
        }
    }

    public function getMainRoleWithGrants(): array
    {
        $grantsOfRoles = [];
        foreach ($this->databases as $database) {
            $roleName = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $grantedOnDatabaseRole = $this->sourceConnection->fetchAll(sprintf(
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
            'assignedGrants' => $this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s;',
                $mainRole
            )),
        ];
    }

    public function exportRolesGrants(): array
    {
        $tmp = [];
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $roles = $this->sourceConnection->fetchAll(sprintf(
                'SHOW ROLES LIKE %s',
                QueryBuilder::quote($databaseRole)
            ));

            $roles = $this->getOtherRolesToMainProjectRole($roles);

            foreach ($roles as $role) {
                $tmp[$databaseRole][] = array_merge(
                    $role,
                    [
                        'assignedGrants' => $this->sourceConnection->fetchAll(sprintf(
                            'SHOW GRANTS TO ROLE %s;',
                            $role['name']
                        )),
                        'assignedFutureGrants' => $this->sourceConnection->fetchAll(sprintf(
                            'SHOW FUTURE GRANTS TO ROLE %s;',
                            $role['name']
                        )),
                    ]
                );
            }
        }
        return $tmp;
    }

    public function createMainRole(array $mainRoleWithGrants, array $users): void
    {
        $user = $mainRole = $mainRoleWithGrants['name'];

        $this->destinationConnection->createRole(['name' => $mainRole, 'privilege' => 'OWNERSHIP']);

        $mainRoleGrants = [
            'GRANT CREATE DATABASE ON ACCOUNT TO ROLE %s;',
            'GRANT CREATE ROLE ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
            'GRANT CREATE USER ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
        ];

        foreach ($mainRoleGrants as $mainRoleGrant) {
            $this->destinationConnection->query(sprintf($mainRoleGrant, $mainRole));
        }

        $warehouses = array_filter($mainRoleWithGrants['assignedGrants'], fn($v) => $v['granted_on'] === 'WAREHOUSE');
        $useWarehouse = false;
        foreach ($warehouses as $warehouse) {
            $warehouseSize = self::createWarehouse($warehouse);
            $this->destinationConnection->assignGrantToRole($warehouse);

            if ($useWarehouse === false || $warehouseSize === 'X-Small') {
                $useWarehouse = $warehouse;
            }
        }

        $this->destinationConnection->query(sprintf(
            'USE WAREHOUSE %s',
            QueryBuilder::quoteIdentifier($useWarehouse['name'])
        ));

        $this->destinationConnection->query(sprintf(
            'CREATE USER IF NOT EXISTS %s PASSWORD=%s DEFAULT_ROLE=%s',
            $user,
            QueryBuilder::quote($users[$user]),
            $mainRole
        ));

        $this->destinationConnection->query(sprintf(
            'GRANT ROLE %s TO USER %s;',
            $mainRole,
            $user
        ));

        $projectUsers = array_filter($mainRoleWithGrants['assignedGrants'], function ($v) {
            if ($v['privilege'] !== 'OWNERSHIP') {
                return false;
            }
            if ($v['granted_on'] !== 'USER') {
                return false;
            }
            return in_array($v['name'], $this->databases);
        });

        foreach ($projectUsers as $projectUser) {
            $this->createUser($projectUser, $users);
            $this->destinationConnection->assignGrantToRole($projectUser);
        }

        $this->destinationConnection->useRole($this->mainMigrationRole);

        $this->destinationConnection->query(sprintf(
            'GRANT ROLE %s TO ROLE SYSADMIN;',
            $mainRole
        ));
    }

    public function cleanupProject(): void
    {
        // !!!!! TESTING METHOD !!!!!!!
        $this->destinationConnection->useRole($this->mainMigrationRole);

        $databases = [
            'SAPI_9472_OLD',
            'SAPI_9473_OLD',
        ];
        foreach ($databases as $database) {
            $this->destinationConnection->query(sprintf(
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
            $this->destinationConnection->query(sprintf(
                'DROP WAREHOUSE IF EXISTS %s',
                $warehouse
            ));
        }

        $this->destinationConnection->query('DROP ROLE IF EXISTS KEBOOLA_STORAGE');
        $this->destinationConnection->query('DROP USER IF EXISTS KEBOOLA_STORAGE');
    }

    private function createWarehouse(array $warehouse): string
    {
        $warehouseInfo = $this->sourceConnection->fetchAll(sprintf(
            'SHOW WAREHOUSES LIKE %s',
            QueryBuilder::quote($warehouse['name'])
        ));
        assert(count($warehouseInfo) === 1);

        $warehouseInfo = current($warehouseInfo);

        $sqlTemplate = <<<SQL
CREATE WAREHOUSE IF NOT EXISTS %s
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

        $this->destinationConnection->query($sql);

        return $warehouseInfo['size'];
    }

    public function grantRoleToUsers(): void
    {
        $this->destinationConnection->useRole($this->mainMigrationRole);
        $this->sourceConnection->useRole($this->mainMigrationRole);

        foreach ($this->usedUsers as $user) {
            $grants = $this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS TO USER %s',
                QueryBuilder::quoteIdentifier($user)
            ));

            foreach ($grants as $grant) {
                $this->destinationConnection->useRole($grant['granted_by']);
                $this->destinationConnection->query(sprintf(
                    'GRANT ROLE %s TO %s %s',
                    $grant['role'],
                    $grant['granted_to'],
                    $grant['grantee_name'],
                ));
            }
        }
    }

    public function cleanupAccount(bool $dryRun = true): void
    {
        $sqls = [];
        $currentRole = $this->mainMigrationRole;
        foreach ($this->databases as $database) {
            $dbExists = $this->destinationConnection->fetchAll(sprintf(
                'SHOW DATABASES LIKE %s;',
                QueryBuilder::quote($database)
            ));
            if (!$dbExists) {
                continue;
            }
            $databaseRole = $this->destinationConnection->getOwnershipRoleOnDatabase($database);
            $data = self::getDataToRemove($databaseRole);

            foreach ($data['USER'] ?? [] as $user) {
                if ($user['granted_by'] !== $currentRole) {
                    $currentRole = $user['granted_by'];
                    $sqls[] = sprintf(
                        'GRANT ROLE %s TO USER %s;',
                        QueryBuilder::quoteIdentifier($currentRole),
                        QueryBuilder::quoteIdentifier((string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'))
                    );
                    $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
                }
                $sqls[] = sprintf('DROP USER IF EXISTS %s;', QueryBuilder::quoteIdentifier($user['name']));
            }

            foreach ($data['ROLE'] ?? [] as $user) {
                if ($user['granted_by'] !== $currentRole) {
                    $currentRole = $user['granted_by'];
                    $sqls[] = sprintf(
                        'GRANT ROLE %s TO USER %s;',
                        QueryBuilder::quoteIdentifier($currentRole),
                        QueryBuilder::quoteIdentifier((string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'))
                    );
                    $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
                }
                $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', QueryBuilder::quoteIdentifier($user['name']));
            }

            $grantsOfRole = $this->destinationConnection->fetchAll(sprintf('SHOW GRANTS OF ROLE %s', $databaseRole));
            $filteredGrantsOfRole = array_filter(
                $grantsOfRole,
                fn($v) => strtoupper($v['grantee_name']) === strtoupper($databaseRole)
            );
            assert(count($filteredGrantsOfRole) === 1);
            $grantOfRole = current($grantsOfRole);
            if ($grantOfRole['granted_by'] !== $currentRole) {
                $currentRole = $grantOfRole['granted_by'];
                $sqls[] = sprintf(
                    'GRANT ROLE %s TO USER %s;',
                    QueryBuilder::quoteIdentifier($currentRole),
                    QueryBuilder::quoteIdentifier((string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'))
                );
                $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
            }
            $sqls[] = sprintf(
                'DROP USER IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($grantOfRole['grantee_name'])
            );
            $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', QueryBuilder::quoteIdentifier($databaseRole));

            if ($currentRole !== $this->mainMigrationRole) {
                $currentRole = $this->mainMigrationRole;
                $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
            }

            $sqls[] = sprintf(
                'DROP DATABASE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($database . '_OLD')
            );

            $sqls[] = sprintf(
                'ALTER DATABASE IF EXISTS %s RENAME TO %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($database . '_OLD'),
            );
        }

        foreach ($sqls as $sql) {
            if ($dryRun) {
                $this->logger->info($sql);
            } else {
                $this->destinationConnection->query($sql);
            }
        }
        if ($dryRun && $sqls) {
            throw new UserException('!!! PLEASE RUN SQLS ON TARGET SNOWFLAKE ACCOUNT !!!');
        }
    }

    private function createUser(array $userGrant, array $passwordOfUsers): void
    {
        $this->destinationConnection->useRole($userGrant['granted_by']);

        if (!in_array($userGrant['name'], $this->usedUsers)) {
            $this->usedUsers[] = $userGrant['name'];
        }

        if (isset($passwordOfUsers[$userGrant['name']])) {
            $this->destinationConnection->query(sprintf(
                'CREATE USER %s PASSWORD=\'%s\' DEFAULT_ROLE = %s',
                $userGrant['name'],
                $passwordOfUsers[$userGrant['name']],
                $userGrant['name'],
            ));
        } else {
            $password = Helper::generateRandomString();
            $this->logger->alert(sprintf(
                'User "%s" has been created with password "%s". Please change it immediately!',
                $userGrant['name'],
                $password
            ));
            $this->destinationConnection->query(sprintf(
                'CREATE USER %s PASSWORD=\'%s\' DEFAULT_ROLE = %s MUST_CHANGE_PASSWORD = true',
                $userGrant['name'],
                $password,
                $userGrant['name'],
            ));
        }
    }

    private function getOtherRolesToMainProjectRole(array $roles): array
    {
        foreach ($roles as $role) {
            $grantsToRole = $this->sourceConnection->fetchAll(sprintf(
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

    private function assignSharePrivilegesToRole(string $database, string $role): void
    {
        $this->destinationConnection->assignGrantToRole([
            'privilege' => 'IMPORTED PRIVILEGES',
            'granted_on' => 'DATABASE',
            'name' => $database . '_SHARE',
            'granted_to' => 'ROLE',
            'grantee_name' => $role,
            'grant_option' => 'false',
            'granted_by' => $this->mainMigrationRole,
        ]);
    }

    private function getDataToRemove(string $role): array
    {
        $grants = $this->destinationConnection->fetchAll(sprintf(
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
                    self::getDataToRemove($roleGrant['name']),
                    $mapGrants,
                );
            }
        }

        return $mapGrants;
    }

    private function canCloneTable(string $database, string $schema, string $table): bool
    {
        $sqlTemplate = 'SELECT max("_timestamp") as "maxTimestamp" FROM %s.%s.%s';

        try {
            $lastUpdateTableInOldDatabase = $this->destinationConnection->fetchAll(sprintf(
                $sqlTemplate,
                QueryBuilder::quoteIdentifier($database . '_OLD'),
                QueryBuilder::quoteIdentifier($schema),
                QueryBuilder::quoteIdentifier($table)
            ));
            $lastUpdateTableInShareDatabase = $this->destinationConnection->fetchAll(sprintf(
                $sqlTemplate,
                QueryBuilder::quoteIdentifier($database . '_SHARE'),
                QueryBuilder::quoteIdentifier($schema),
                QueryBuilder::quoteIdentifier($table)
            ));
        } catch (RuntimeException $e) {
            return false;
        }

        return $lastUpdateTableInOldDatabase[0]['maxTimestamp'] === $lastUpdateTableInShareDatabase[0]['maxTimestamp'];
    }

    private function grantsPrivilegesToOldDatabase(string $database, string $databaseRole): void
    {
        $currentRole = $this->destinationConnection->getCurrentRole();
        $this->destinationConnection->useRole($this->mainMigrationRole);
        $dbExists = $this->destinationConnection->fetchAll(sprintf(
            'SHOW DATABASES LIKE %s',
            QueryBuilder::quote($database . '_OLD')
        ));
        if (!$dbExists) {
            return;
        }
        $sqls = [
            sprintf(
                'grant all on database %s to role %s;',
                QueryBuilder::quoteIdentifier($database . '_OLD'),
                $databaseRole
            ),
            sprintf(
                'grant all on all schemas in database %s to role %s;',
                QueryBuilder::quoteIdentifier($database . '_OLD'),
                $databaseRole
            ),
            sprintf(
                'grant all on all tables in database %s to role %s;',
                QueryBuilder::quoteIdentifier($database . '_OLD'),
                $databaseRole
            ),
        ];

        foreach ($sqls as $sql) {
            $this->destinationConnection->query($sql);
        }
        $this->destinationConnection->useRole($currentRole);
    }

    public function postMigrationCleanup(): void
    {
        $this->destinationConnection->useRole($this->mainMigrationRole);
        foreach ($this->databases as $database) {
            $removeDatabases = [
                $database . '_OLD',
                $database . '_SHARE',
            ];

            foreach ($removeDatabases as $removeDatabase) {
                $this->destinationConnection->query(sprintf(
                    'DROP DATABASE IF EXISTS %s',
                    QueryBuilder::quoteIdentifier($removeDatabase)
                ));
            }

            $userRoles = $this->destinationConnection->fetchAll(sprintf(
                'SHOW GRANTS TO USER %s',
                QueryBuilder::quoteIdentifier((string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'))
            ));

            foreach ($userRoles as $userRole) {
                if ($userRole['role'] === $this->mainMigrationRole) {
                    continue;
                }
                $this->destinationConnection->query(sprintf(
                    'REVOKE ROLE %s FROM USER %s',
                    QueryBuilder::quoteIdentifier($userRole['role']),
                    QueryBuilder::quoteIdentifier($userRole['grantee_name']),
                ));
            }
        }
    }
}
