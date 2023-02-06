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

    private array $useWarehouse;

    private string $mainMigrationRoleSourceAccount;

    private string $mainMigrationRoleTargetAccount;

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
        string $mainMigrationRoleSourceAccount,
        string $mainMigrationRoleTargetAccount
    ) {
        $this->logger = $logger;
        $this->sourceConnection = $sourceConnection;
        $this->migrateConnection = $migrateConnection;
        $this->destinationConnection = $destinationConnection;
        $this->databases = $databases;
        $this->mainMigrationRoleSourceAccount = $mainMigrationRoleSourceAccount;
        $this->mainMigrationRoleTargetAccount = $mainMigrationRoleTargetAccount;
    }

    public function cleanupAccount(bool $dryRun = true): void
    {
        $sqls = [];
        $currentRole = $this->mainMigrationRoleTargetAccount;
        foreach ($this->databases as $database) {
            $dbExists = $this->destinationConnection->fetchAll(sprintf(
                'SHOW DATABASES LIKE %s;',
                QueryBuilder::quote($database)
            ));
            if (!$dbExists) {
                continue;
            }
            $databaseRole = $this->destinationConnection->getOwnershipRoleOnDatabase($database);
            $data = $this->getDataToRemove($databaseRole);

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

            foreach ($data['ROLE'] ?? [] as $role) {
                if ($role['granted_by'] !== $currentRole) {
                    $currentRole = $role['granted_by'];
                    $sqls[] = sprintf(
                        'GRANT ROLE %s TO USER %s;',
                        QueryBuilder::quoteIdentifier($currentRole),
                        QueryBuilder::quoteIdentifier((string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'))
                    );
                    $sqls[] = sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($currentRole));
                }
                $this->destinationConnection->useRole($role['granted_by']);
                $futureGrants = $this->destinationConnection->fetchAll(sprintf(
                    'SHOW FUTURE GRANTS TO ROLE %s',
                    $role['name']
                ));
                foreach ($futureGrants as $futureGrant) {
                    $sqls[] = sprintf(
                        'REVOKE %s ON FUTURE TABLES IN SCHEMA %s FROM ROLE %s',
                        $futureGrant['privilege'],
                        Helper::removeStringFromEnd($futureGrant['name'], '.<TABLE>'),
                        QueryBuilder::quoteIdentifier($futureGrant['grantee_name']),
                    );
                }
                $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', QueryBuilder::quoteIdentifier($role['name']));
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

        $this->destinationConnection->query(sprintf(
            'USE ROLE %s',
            QueryBuilder::quoteIdentifier($this->mainMigrationRoleTargetAccount)
        ));
    }

    public function createReplication(): void
    {
        if ($this->sourceConnection->getRegion() === $this->destinationConnection->getRegion()) {
            return;
        }
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

            $this->migrateConnection->query('USE WAREHOUSE "MIGRATE";');

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

    public function migrateUsersRolesAndGrants(Config $config, string $mainRole, array $grants): void
    {
        $this->logger->info('Migrating users and roles.');
        // first step - migrate users and roles (without grants)
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            [
                'grants' => [
                    'roles' => $rolesGrants,
                    'account' => $accountGrants,
                    'user' => $userGrants,
                ],
            ] = Helper::parseGrantsToObjects($grants[$databaseRole]);

            $this->destinationConnection->createRole([
                'name' => $databaseRole,
                'granted_by' => $mainRole,
                'privilege' => 'OWNERSHIP',
            ]);

            foreach ($accountGrants as $grant) {
                $this->destinationConnection->assignGrantToRole($grant);
            }

            foreach ($rolesGrants as $rolesGrant) {
                if ($rolesGrant['privilege'] === 'OWNERSHIP') {
                    $this->destinationConnection->createRole($rolesGrant);
                    if ($config->getSynchronizeRun()) {
                        $this->grantsPrivilegesToOldDatabase($database, $rolesGrant['name']);
                    }
                }
            }

            foreach ($userGrants as $userGrant) {
                $this->createUser($userGrant, $config->getPasswordOfUsers());
            }
        }

        $this->logger->info('Migrating grants of warehouses/users and roles.');
        // second step - migrate all grants of roles/users/warehouses/account
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            if ($config->getSynchronizeRun()) {
                $this->grantsPrivilegesToOldDatabase($database, $databaseRole);
            }

            [
                'grants' => [
                    'roles' => $rolesGrants,
                    'warehouse' => $warehouseGrants,
                    'user' => $userGrants,
                ],
            ] = Helper::parseGrantsToObjects($grants[$databaseRole]);

            foreach ($rolesGrants as $rolesGrant) {
                $this->destinationConnection->assignGrantToRole($rolesGrant);
            }

            foreach ($warehouseGrants as $warehouseGrant) {
                $this->destinationConnection->assignGrantToRole($warehouseGrant);
            }

            foreach ($userGrants as $userGrant) {
                $this->destinationConnection->assignGrantToRole($userGrant);
            }
        }
    }

    public function cloneDatabaseWithGrants(string $mainRole, array $grants): void
    {
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            [
                'grants' => [
                    'databases' => $databaseGrants,
                    'schemas' => $schemasGrants,
                    'tables' => $tablesGrants,
                ],
                'futureGrants' => [
                    'tables' => $tablesFutureGrants,
                ],
            ] = Helper::parseGrantsToObjects($grants[$databaseRole]);

            $this->destinationConnection->useRole($mainRole);

            $shareDbName = $database . '_SHARE';
            $oldDbName = $database . '_OLD';

            $this->logger->info(sprintf('Migrate database "%s".', $database));
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

                $schemaGrants = Helper::filterSchemaGrants($database, $schemaName, $schemasGrants);
                $schemaFutureGrants = Helper::filterSchemaGrants($database, $schemaName, $tablesFutureGrants);
                $ownershipOnSchema = array_filter($schemaGrants, fn($v) => $v['privilege'] === 'OWNERSHIP');
                assert(count($ownershipOnSchema) === 1);

                $schemaOptions = array_map(fn($v) => trim($v), explode(',', $schema['options']));

                $this->destinationConnection->useRole(current($ownershipOnSchema)['granted_by']);
                $this->destinationConnection->query(sprintf(
                    'CREATE %s SCHEMA %s.%s %s DATA_RETENTION_TIME_IN_DAYS = %s;',
                    in_array('TRANSIENT', $schemaOptions) ? 'TRANSIENT' : '',
                    QueryBuilder::quoteIdentifier($database),
                    QueryBuilder::quoteIdentifier($schemaName),
                    in_array('MANAGED ACCESS', $schemaOptions) ? 'WITH MANAGED ACCESS' : '',
                    $schema['retention_time']
                ));

                foreach ($schemaGrants as $schemaGrant) {
                    $this->destinationConnection->assignGrantToRole($schemaGrant);
                }
                foreach ($schemaFutureGrants as $schemaFutureGrant) {
                    $this->destinationConnection->assignFutureGrantToRole($schemaFutureGrant);
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
                    $this->destinationConnection->useWarehouse($ownershipOnTable['granted_by']);

                    if ($this->canCloneTable($database, $schemaName, $tableName)) {
                        $this->logger->info(sprintf('Cloning table "%s" from OLD database', $tableName));
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
                        $this->logger->info(sprintf('Creating table "%s" from SHARE database', $tableName));
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

                    $tableGrants = array_filter($tableGrants, fn($v) => $v['privilege'] !== 'OWNERSHIP');
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
        foreach ($warehouses as $warehouse) {
            $warehouseSize = self::createWarehouse($warehouse);
            $this->destinationConnection->assignGrantToRole($warehouse);

            if (!isset($this->useWarehouse) || $warehouseSize === 'X-Small') {
                $this->useWarehouse = $warehouse;
            }
        }

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

        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);

        $this->destinationConnection->query(sprintf(
            'GRANT ROLE %s TO ROLE SYSADMIN;',
            $mainRole
        ));
    }

    private function createWarehouse(array $warehouse): string
    {
        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
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
        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
        $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);

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

    public function postMigrationCleanup(): void
    {
        foreach ($this->databases as $database) {
            $databases = [
                $database . '_OLD',
                $database . '_SHARE',
            ];

            $removeDatabases = array_filter(
                $this->destinationConnection->fetchAll('SHOW DATABASES;'),
                fn($v) => in_array($v['name'], $databases)
            );

            foreach ($removeDatabases as $removeDatabase) {
                $this->destinationConnection->useRole($removeDatabase['owner']);
                $this->destinationConnection->query(sprintf(
                    'DROP DATABASE IF EXISTS %s',
                    QueryBuilder::quoteIdentifier($removeDatabase['name'])
                ));
            }

            $userRoles = $this->destinationConnection->fetchAll(sprintf(
                'SHOW GRANTS TO USER %s',
                QueryBuilder::quoteIdentifier((string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'))
            ));

            $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
            foreach ($userRoles as $userRole) {
                if ($userRole['role'] === $this->mainMigrationRoleTargetAccount) {
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

    public function postMigrationCheck(array $mainRoleWithGrants): void
    {

        $warehouses = array_filter($mainRoleWithGrants['assignedGrants'], fn($v) => $v['granted_on'] === 'WAREHOUSE');

        $useWarehouse = sprintf(
            'USE WAREHOUSE %s',
            QueryBuilder::quoteIdentifier(current($warehouses)['name'])
        );
        $this->sourceConnection->query($useWarehouse);
        $this->destinationConnection->query($useWarehouse);

        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $rolesAndUsers = $this->listRolesAndUsers($databaseRole);
            $rolesAndUsers = array_merge_recursive(
                $rolesAndUsers,
                ['users' => [$databaseRole], 'roles' => [$databaseRole]]
            );

            $compares = [];
            // phpcs:disable Generic.Files.LineLength
//            Compare TABLES
            $compares[] = [
                'group' => 'Tables',
                'itemNameKey' => 'TABLE_NAME',
                'sql' => sprintf(
                    'SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.TABLES WHERE DELETED IS NULL AND TABLE_CATALOG = %s ORDER BY TABLE_SCHEMA, TABLE_NAME;',
                    implode(',', [
                        'CONCAT(TABLE_SCHEMA, \'.\', TABLE_NAME) AS ID',
                        'TABLE_NAME',
                        'TABLE_SCHEMA',
                        'TABLE_OWNER',
                        'TABLE_TYPE',
                        'ROW_COUNT',
                        // 'BYTES',
                    ]),
                    QueryBuilder::quote($database)
                ),
            ];

//            Compare USERS
            $compares[] = [
                'group' => 'Users',
                'itemNameKey' => 'NAME',
                'sql' => sprintf(
                    'SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.USERS WHERE DELETED_ON IS NULL AND NAME IN (%s) ORDER BY NAME;',
                    implode(',', [
                        'NAME AS ID',
                        'NAME',
                        'LOGIN_NAME',
                        'DISPLAY_NAME',
                        'FIRST_NAME',
                        'LAST_NAME',
                        'EMAIL',
                        'DEFAULT_WAREHOUSE',
                        'DEFAULT_NAMESPACE',
                        'DEFAULT_ROLE',
                        'OWNER',
                    ]),
                    implode(', ', array_map(fn($v) => QueryBuilder::quote($v), $rolesAndUsers['roles']))
                ),
            ];

//            Compare GRANTS TO USERS
            $compares[] = [
                'group' => 'Grants to user',
                'itemNameKey' => 'ROLE',
                'sql' => sprintf(
                    'SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS WHERE DELETED_ON IS NULL AND ROLE IN (%s) ORDER BY ROLE, GRANTED_BY;',
                    implode(',', [
                        'CONCAT(ROLE, GRANTED_TO, GRANTEE_NAME, GRANTED_BY) AS ID',
                        'ROLE',
                        'GRANTED_TO',
                        'GRANTEE_NAME',
                        'GRANTED_BY',
                    ]),
                    implode(', ', array_map(fn($v) => QueryBuilder::quote($v), $rolesAndUsers['users']))
                ),
            ];

//            Compare ROLES
            $compares[] = [
                'group' => 'Roles',
                'itemNameKey' => 'NAME',
                'sql' => sprintf(
                    'SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES WHERE DELETED_ON IS NULL AND NAME IN (%s) ORDER BY NAME;',
                    implode(',', [
                        'CONCAT(NAME, OWNER) AS ID',
                        'NAME',
                        'COMMENT',
                        'OWNER',
                    ]),
                    implode(', ', array_map(fn($v) => QueryBuilder::quote($v), $rolesAndUsers['roles']))
                ),
            ];

//            Compare GRANTS TO ROLES
            $compares[] = [
                'group' => 'Grants to roles',
                'itemNameKey' => 'ID',
                'sql' => sprintf(
                    'SELECT %s FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_ROLES WHERE DELETED_ON IS NULL AND GRANTEE_NAME IN (%s) ORDER BY GRANTEE_NAME, PRIVILEGE, NAME;',
                    implode(',', [
                        'CONCAT(PRIVILEGE, GRANTED_ON, NAME, GRANTED_TO, GRANTEE_NAME, GRANTED_BY, CASE WHEN TABLE_CATALOG IS NULL THEN \'\' ELSE TABLE_CATALOG END, CASE WHEN TABLE_SCHEMA IS NULL THEN \'\' ELSE TABLE_SCHEMA END) AS ID',
                        'PRIVILEGE',
                        'GRANTED_ON',
                        'NAME',
                        'TABLE_CATALOG',
                        'TABLE_SCHEMA',
                        'GRANTED_TO',
                        'GRANTEE_NAME',
                        'GRANT_OPTION',
                        'GRANTED_BY',
                    ]),
                    implode(', ', array_map(fn($v) => QueryBuilder::quote($v), $rolesAndUsers['roles']))
                ),
            ];
            // phpcs:enable Generic.Files.LineLength

            foreach ($compares as $compare) {
                $this->compareData($compare['group'], $compare['itemNameKey'], $compare['sql']);
            }
        }
    }

    private function compareData(string $group, string $itemNameKey, string $sql): void
    {
        $sourceData = $this->sourceConnection->fetchAll($sql);
        $targetData = $this->destinationConnection->fetchAll($sql);

        if (count($sourceData) !== count($targetData)) {
            $this->logger->alert(sprintf(
                '%s: Source data count (%s) not equils target data count (%s)',
                $group,
                count($sourceData),
                count($targetData)
            ));
        }
        $sourceData = array_combine(array_map(fn($v) => $v['ID'], $sourceData), $sourceData);
        $targetData = array_combine(array_map(fn($v) => $v['ID'], $targetData), $targetData);

        array_walk($sourceData, fn(&$v) => $v = serialize($v));
        array_walk($targetData, fn(&$v) => $v = serialize($v));

        $diffs = [
            array_diff($sourceData, $targetData),
            array_diff($targetData, $sourceData),
        ];
        $print = [];
        foreach ($diffs as $diff) {
            foreach ($diff as $k => $serializeItem) {
                if (!isset($sourceData[$k])) {
                    $this->logger->alert(sprintf('%s: Item "%s" doesn\'t exists in source account', $group, $k));
                    continue;
                }
                if (!isset($targetData[$k])) {
                    $this->logger->alert(sprintf('%s: Item "%s" doesn\'t exists in target account', $group, $k));
                    continue;
                }
                $itemSource = (array) unserialize($sourceData[$k]);
                $itemName = strval($itemSource[$itemNameKey]);
                if (in_array($itemName, $print)) {
                    continue;
                }
                $itemTarget = (array) unserialize($targetData[$k]);
                $itemDiffs = [
                    'target' => array_diff($itemSource, $itemTarget),
                    'source' => array_diff($itemTarget, $itemSource),
                ];

                foreach ($itemDiffs as $missingIn => $itemDiff) {
                    $this->printDiffAlert($group, $itemName, $missingIn, $itemDiff);
                }

                $print[] = $itemName;
            }
        }
    }

    private function printDiffAlert(string $group, string $name, string $missingIn, array $data): void
    {
        if (!$data) {
            return;
        }
        array_walk($data, fn(&$v, $k) => $v = sprintf('%s: %s', $k, $v));

        $this->logger->alert(sprintf(
            '%s: "%s" is not same. Missing in %s account (%s)',
            $group,
            $name,
            $missingIn,
            implode(';', $data)
        ));
    }

    private function listRolesAndUsers(string $role): array
    {
        $grants = $this->destinationConnection->fetchAll(sprintf(
            'SHOW GRANTS TO ROLE %s',
            QueryBuilder::quoteIdentifier($role)
        ));

        $filteredGrants = array_filter(
            $grants,
            fn($v) => $v['privilege'] === 'OWNERSHIP' && (in_array($v['granted_on'], ['USER', 'ROLE']))
        );

        $tmp = [
            'users' => [],
            'roles' => [],
        ];
        foreach ($filteredGrants as $filteredGrant) {
            switch ($filteredGrant['granted_on']) {
                case 'USER':
                    $tmp['users'][] = $filteredGrant['name'];
                    break;
                case 'ROLE':
                    $tmp['roles'][] = $filteredGrant['name'];
                    $childRoles = $this->listRolesAndUsers($filteredGrant['name']);
                    $tmp = array_merge_recursive($tmp, $childRoles);
                    break;
            }
        }

        return $tmp;
    }

    private function createUser(array $userGrant, array $passwordOfUsers): void
    {
        $this->destinationConnection->useRole($userGrant['granted_by']);

        if (!in_array($userGrant['name'], $this->usedUsers)) {
            $this->usedUsers[] = $userGrant['name'];
        }

        $describeUser = $this->sourceConnection->fetchAll(sprintf(
            'SHOW USERS LIKE %s',
            QueryBuilder::quote($userGrant['name'])
        ));
        assert(count($describeUser) === 1);

        $allowOptions = [
            'default_secondary_roles',
            'default_role',
            'default_namespace',
            'default_warehouse',
            'display_name',
            'login_name',
        ];

        $describeUser = (array) array_filter(
            current($describeUser),
            fn($k) => in_array($k, $allowOptions),
            ARRAY_FILTER_USE_KEY
        );

        $describeUser = array_filter(
            $describeUser,
            fn($v) => $v !== ''
        );

        array_walk(
            $describeUser,
            fn(&$item, $k) => $item = sprintf('%s = %s', strtoupper($k), QueryBuilder::quote($item))
        );

        if (isset($passwordOfUsers[$userGrant['name']])) {
            $password = $passwordOfUsers[$userGrant['name']];
        } else {
            $password = Helper::generateRandomString();
            $this->logger->alert(sprintf(
                'User "%s" has been created with password "%s". Please change it immediately!',
                $userGrant['name'],
                $password
            ));
        }

        $describeUser['password'] = sprintf(
            'PASSWORD = %s',
            QueryBuilder::quote($password)
        );

        $this->destinationConnection->query(sprintf(
            'CREATE USER %s %s',
            $userGrant['name'],
            implode(' ', $describeUser),
        ));
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
            'granted_by' => $this->mainMigrationRoleTargetAccount,
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
                    $this->getDataToRemove($roleGrant['name']),
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
        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
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
}
