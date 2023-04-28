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
use Symfony\Component\Process\Process;

class Migrate
{
    private LoggerInterface $logger;

    private Config $config;

    private Connection $sourceConnection;

    private ?Connection $migrateConnection;

    private Connection $destinationConnection;

    private array $databases;

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
        Config $config,
        Connection $sourceConnection,
        ?Connection $migrateConnection,
        Connection $destinationConnection,
        array $databases,
        string $mainMigrationRoleSourceAccount,
        string $mainMigrationRoleTargetAccount
    ) {
        $this->logger = $logger;
        $this->config = $config;
        $this->sourceConnection = $sourceConnection;
        $this->migrateConnection = $migrateConnection;
        $this->destinationConnection = $destinationConnection;
        $this->databases = $databases;
        $this->mainMigrationRoleSourceAccount = $mainMigrationRoleSourceAccount;
        $this->mainMigrationRoleTargetAccount = $mainMigrationRoleTargetAccount;
    }

    public function cleanupAccount(string $mainRoleName, bool $dryRun = true): void
    {
        $sqls = [];
        $currentRole = $this->mainMigrationRoleTargetAccount;
        $this->destinationConnection->grantRoleToUser($this->config->getTargetSnowflakeUser(), $mainRoleName);
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

            $currentUser = $this->destinationConnection->fetchAll('SELECT CURRENT_USER() AS "user";');
            foreach ($data['USER'] ?? [] as $user) {
                if ($user['granted_by'] !== $currentRole) {
                    $currentRole = $user['granted_by'];
                    $sqls[] = sprintf(
                        'GRANT ROLE %s TO USER %s;',
                        Helper::quoteIdentifier($currentRole),
                        Helper::quoteIdentifier($this->config->getTargetSnowflakeUser())
                    );
                    $sqls[] = sprintf('USE ROLE %s;', Helper::quoteIdentifier($currentRole));
                }

                if ($currentUser[0]['user'] !== $user['name']) {
                    $sqls[] = sprintf('DROP USER IF EXISTS %s;', Helper::quoteIdentifier($user['name']));
                }
            }

            foreach ($data['ROLE'] ?? [] as $role) {
                if ($role['granted_by'] !== $currentRole) {
                    $currentRole = $role['granted_by'];
                    $sqls[] = sprintf('USE ROLE %s;', Helper::quoteIdentifier($currentRole));
                }
                $this->destinationConnection->useRole($mainRoleName);
                $this->destinationConnection->query(sprintf(
                    'GRANT ROLE %s TO USER %s;',
                    Helper::quoteIdentifier($currentRole),
                    Helper::quoteIdentifier($this->config->getTargetSnowflakeUser())
                ));
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
                        Helper::quoteIdentifier($futureGrant['grantee_name']),
                    );
                }
                $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', Helper::quoteIdentifier($role['name']));
            }

            $grantsOfRole = $this->destinationConnection->fetchAll(sprintf('SHOW GRANTS OF ROLE %s', $databaseRole));
            $filteredGrantsOfRole = array_filter(
                $grantsOfRole,
                fn($v) => strtoupper($v['grantee_name']) === strtoupper($databaseRole)
            );
            assert(count($filteredGrantsOfRole) === 1);
            $grantOfRole = current($filteredGrantsOfRole);
            if ($grantOfRole['granted_by'] !== $currentRole) {
                $currentRole = $grantOfRole['granted_by'];
                $sqls[] = sprintf(
                    'GRANT ROLE %s TO USER %s;',
                    Helper::quoteIdentifier($currentRole),
                    Helper::quoteIdentifier($this->config->getTargetSnowflakeUser())
                );
                $sqls[] = sprintf('USE ROLE %s;', Helper::quoteIdentifier($currentRole));
            }
            $sqls[] = sprintf(
                'DROP USER IF EXISTS %s;',
                Helper::quoteIdentifier($grantOfRole['grantee_name'])
            );

            $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', Helper::quoteIdentifier($databaseRole));

            $sqls[] = sprintf(
                'DROP DATABASE IF EXISTS %s;',
                Helper::quoteIdentifier($database . '_OLD')
            );

            $sqls[] = sprintf(
                'ALTER DATABASE IF EXISTS %s RENAME TO %s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($database . '_OLD'),
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
            Helper::quoteIdentifier($this->mainMigrationRoleTargetAccount)
        ));
    }

    public function createReplication(): void
    {
        if ($this->sourceConnection->getRegion() === $this->destinationConnection->getRegion()) {
            return;
        }
        if (!$this->migrateConnection) {
            throw new UserException('Migration connection is not set');
        }
        foreach ($this->databases as $database) {
            //            Allow replication on source database
            $this->sourceConnection->query(sprintf(
                'ALTER DATABASE %s ENABLE REPLICATION TO ACCOUNTS %s.%s;',
                Helper::quoteIdentifier($database),
                $this->migrateConnection->getRegion(),
                $this->migrateConnection->getAccount()
            ));
            $this->sourceConnection->useRole($this->sourceConnection->getOwnershipRoleOnDatabase($database));

            //            Waiting for previous SQL query
            sleep(1);

            //            Migration database sqls
            $this->migrateConnection->query(sprintf(
                'CREATE DATABASE IF NOT EXISTS %s AS REPLICA OF %s.%s.%s;',
                Helper::quoteIdentifier($database),
                $this->sourceConnection->getRegion(),
                $this->sourceConnection->getAccount(),
                Helper::quoteIdentifier($database)
            ));

            $this->migrateConnection->query(sprintf(
                'USE DATABASE %s',
                Helper::quoteIdentifier($database)
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
                Helper::quoteIdentifier($database)
            ));
        }
    }

    public function createShare(): void
    {
        $sourceRegion = $this->sourceConnection->getRegion();
        $destinationRegion = $this->destinationConnection->getRegion();

        $connection = $this->sourceConnection;
        if ($sourceRegion !== $destinationRegion) {
            if (!$this->migrateConnection) {
                throw new UserException('Migration connection is not set');
            }
            $connection = $this->migrateConnection;
        }

        foreach ($this->databases as $database) {
            $shareName = sprintf('%s%s', self::MIGRATION_SHARE_PREFIX, strtoupper($database));

            $connection->query(sprintf(
                'CREATE SHARE IF NOT EXISTS %s;',
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON DATABASE %s TO SHARE %s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON ALL SCHEMAS IN DATABASE %s TO SHARE %s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT SELECT ON ALL TABLES IN DATABASE %s TO SHARE %s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'ALTER SHARE %s ADD ACCOUNT=%s;',
                Helper::quoteIdentifier($shareName),
                $this->destinationConnection->getAccount()
            ));
        }
    }

    public function createDatabasesFromShares(): void
    {
        $sourceRegion = $this->sourceConnection->getRegion();
        $destinationRegion = $this->destinationConnection->getRegion();

        $connection = $this->sourceConnection;
        if ($sourceRegion !== $destinationRegion) {
            if (!$this->migrateConnection) {
                throw new UserException('Migration connection is not set');
            }
            $connection = $this->migrateConnection;
        }

        foreach ($this->databases as $database) {
            $shareDbName = $database . '_SHARE';

            $this->destinationConnection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                Helper::quoteIdentifier($shareDbName)
            ));

            $this->destinationConnection->query(sprintf(
                'CREATE DATABASE %s FROM SHARE IDENTIFIER(\'%s.%s\');',
                Helper::quoteIdentifier($shareDbName),
                $connection->getAccount(),
                self::MIGRATION_SHARE_PREFIX . $database
            ));
        }
    }

    public function migrateUsersRolesAndGrants(string $mainRole, array $grants): void
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
            ], $this->config->getTargetSnowflakeUser());

            foreach ($accountGrants as $grant) {
                $this->destinationConnection->assignGrantToRole($grant);
            }

            foreach ($rolesGrants as $rolesGrant) {
                if ($rolesGrant['privilege'] === 'OWNERSHIP') {
                    $this->destinationConnection->createRole($rolesGrant, $this->config->getTargetSnowflakeUser());
                    if ($this->config->getSynchronizeRun()) {
                        $this->grantsPrivilegesToOldDatabase($database, $rolesGrant['name'], $mainRole);
                    }
                }
            }

            foreach ($userGrants as $userGrant) {
                $this->createUser($userGrant, $this->config->getPasswordOfUsers());
            }
        }

        $this->logger->info('Migrating grants of warehouses/users and roles.');
        // second step - migrate all grants of roles/users/warehouses/account
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            if ($this->config->getSynchronizeRun()) {
                $this->grantsPrivilegesToOldDatabase($database, $databaseRole, $mainRole);
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

            $sourceDatabases = $this->sourceConnection->fetchAll(sprintf(
                'SHOW DATABASES LIKE %s',
                QueryBuilder::quote($database)
            ));
            assert(count($sourceDatabases) === 1);
            $sourceDatabase = current($sourceDatabases);

            $this->logger->info(sprintf('Migrate database "%s".', $database));
            $this->destinationConnection->query(sprintf(
                'CREATE DATABASE %s DATA_RETENTION_TIME_IN_DAYS=%s;',
                Helper::quoteIdentifier($database),
                '1'
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
                Helper::quoteIdentifier($shareDbName)
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
                    Helper::quoteIdentifier($database),
                    Helper::quoteIdentifier($schemaName),
                    in_array('MANAGED ACCESS', $schemaOptions) ? 'WITH MANAGED ACCESS' : '',
                    '1'
                ));

                foreach ($schemaGrants as $schemaGrant) {
                    $this->destinationConnection->assignGrantToRole($schemaGrant);
                }
                foreach ($schemaFutureGrants as $schemaFutureGrant) {
                    $this->destinationConnection->assignFutureGrantToRole($schemaFutureGrant);
                }

                $tables = $this->destinationConnection->fetchAll(sprintf(
                    'SHOW TABLES IN SCHEMA %s.%s;',
                    Helper::quoteIdentifier($shareDbName),
                    Helper::quoteIdentifier($schemaName)
                ));

                foreach ($tables as $table) {
                    $tableName = $table['name'];

                    $tableGrants = array_filter(
                        $tablesGrants,
                        function (array $v) use ($database, $schemaName, $tableName) {
                            $validSchema = [
                                sprintf('%s.%s.%s', $database, $schemaName, $tableName),
                                sprintf('%s.%s.%s', $database, $schemaName, Helper::quoteIdentifier($tableName)),
                                sprintf('%s.%s.%s', $database, Helper::quoteIdentifier($schemaName), $tableName),
                                sprintf(
                                    '%s.%s.%s',
                                    $database,
                                    Helper::quoteIdentifier($schemaName),
                                    Helper::quoteIdentifier($tableName)
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    Helper::quoteIdentifier($database),
                                    $schemaName,
                                    $tableName
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    Helper::quoteIdentifier($database),
                                    $schemaName,
                                    Helper::quoteIdentifier($tableName)
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    Helper::quoteIdentifier($database),
                                    Helper::quoteIdentifier($schemaName),
                                    $tableName
                                ),
                                sprintf(
                                    '%s.%s.%s',
                                    Helper::quoteIdentifier($database),
                                    Helper::quoteIdentifier($schemaName),
                                    Helper::quoteIdentifier($tableName)
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
                            Helper::quoteIdentifier($database),
                            Helper::quoteIdentifier($schemaName),
                            Helper::quoteIdentifier($tableName),
                            Helper::quoteIdentifier($oldDbName),
                            Helper::quoteIdentifier($schemaName),
                            Helper::quoteIdentifier($tableName),
                        ));
                    } else {
                        $this->logger->info(sprintf('Creating table "%s" from SHARE database', $tableName));
                        $this->destinationConnection->query(sprintf(
                            'CREATE TABLE %s.%s.%s AS SELECT * FROM %s.%s.%s;',
                            Helper::quoteIdentifier($database),
                            Helper::quoteIdentifier($schemaName),
                            Helper::quoteIdentifier($tableName),
                            Helper::quoteIdentifier($shareDbName),
                            Helper::quoteIdentifier($schemaName),
                            Helper::quoteIdentifier($tableName),
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
                Helper::quoteIdentifier($roleName)
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
                Helper::quoteIdentifier($mainRole)
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
                            Helper::quoteIdentifier($role['name'])
                        )),
                        'assignedFutureGrants' => $this->sourceConnection->fetchAll(sprintf(
                            'SHOW FUTURE GRANTS TO ROLE %s;',
                            Helper::quoteIdentifier($role['name'])
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

        $this->destinationConnection->createRole(
            ['name' => $mainRole, 'privilege' => 'OWNERSHIP'],
            $this->config->getTargetSnowflakeUser()
        );

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
        }

        if (isset($users[$user])) {
            $password = $users[$user];
        } else {
            $password = Helper::generateRandomString();
        }

        $this->destinationConnection->query(sprintf(
            'CREATE USER IF NOT EXISTS %s PASSWORD=%s DEFAULT_ROLE=%s',
            $user,
            QueryBuilder::quote($password),
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

        $sourceGrants = $this->sourceConnection->fetchAll(sprintf(
            'SHOW GRANTS TO ROLE %s',
            Helper::quoteIdentifier($this->sourceConnection->getCurrentRole())
        ));

        $sourceGrants = array_filter(
            $sourceGrants,
            fn($v) => $v['granted_on'] === 'WAREHOUSE' && $v['privilege'] === 'USAGE'
        );
        assert(count($sourceGrants) > 0);

        $this->sourceConnection->query(sprintf('USE WAREHOUSE %s', current($sourceGrants)['name']));

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
;
SQL;

        $sql = sprintf(
            $sqlTemplate,
            Helper::quoteIdentifier($warehouseInfo['name']),
            QueryBuilder::quote($warehouseInfo['size']),
            QueryBuilder::quote($warehouseInfo['type']),
            $warehouseInfo['auto_suspend'],
            $warehouseInfo['auto_resume']
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
                Helper::quoteIdentifier($user)
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
                    Helper::quoteIdentifier($removeDatabase['name'])
                ));
            }

            $userRoles = $this->destinationConnection->fetchAll(sprintf(
                'SHOW GRANTS TO USER %s',
                Helper::quoteIdentifier($this->config->getTargetSnowflakeUser())
            ));

            foreach (array_reverse($userRoles) as $userRole) {
                if ($userRole['role'] === $this->mainMigrationRoleTargetAccount) {
                    continue;
                }
                try {
                    if ($userRole['granted_by']) {
                        $this->destinationConnection->useRole($userRole['granted_by']);
                    } else {
                        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
                    }
                    $this->destinationConnection->query(sprintf(
                        'REVOKE ROLE %s FROM USER %s',
                        Helper::quoteIdentifier($userRole['role']),
                        Helper::quoteIdentifier($userRole['grantee_name']),
                    ));
                } catch (RuntimeException $e) {
                    $this->logger->warning(sprintf(
                        'Query failed, please check manually: %s',
                        $e->getMessage()
                    ));
                }
            }
        }
    }

    public function postMigrationCheckStructure(array $mainRoleWithGrants): void
    {
        $warehouses = array_filter($mainRoleWithGrants['assignedGrants'], fn($v) => $v['granted_on'] === 'WAREHOUSE');

        $useWarehouse = sprintf(
            'USE WAREHOUSE %s',
            Helper::quoteIdentifier(current($warehouses)['name'])
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

    public function postMigrationCheckData(array $mainRoleWithGrants): void
    {
        $sourceRegion = $this->sourceConnection->fetchAll(sprintf(
            'SHOW regions LIKE %s',
            QueryBuilder::quote($this->sourceConnection->getRegion())
        ));

        $targetRegion = $this->destinationConnection->fetchAll(sprintf(
            'SHOW regions LIKE %s',
            QueryBuilder::quote($this->destinationConnection->getRegion())
        ));

        $sourceAccount = sprintf(
            '%s.%s',
            $this->sourceConnection->getAccount(),
            $sourceRegion[0]['region']
        );
        $targetAccount = sprintf(
            '%s.%s',
            $this->destinationConnection->getAccount(),
            $targetRegion[0]['region']
        );

        $warehousesGrant = array_values(array_filter(
            $mainRoleWithGrants['assignedGrants'],
            fn($v) => $v['granted_on'] === 'WAREHOUSE' && $v['privilege'] === 'USAGE'
        ));
        assert(count($warehousesGrant) > 0);
        $this->sourceConnection->grantRoleToUser($this->config->getSourceSnowflakeUser(), $mainRoleWithGrants['name']);
        $this->destinationConnection->grantRoleToUser(
            $this->config->getTargetSnowflakeUser(),
            $mainRoleWithGrants['name']
        );

        foreach ($this->config->getDatabases() as $database) {
            $schemas = $this->sourceConnection->fetchAll(sprintf(
                'SHOW SCHEMAS IN DATABASE %s',
                Helper::quoteIdentifier($database)
            ));
            foreach ($schemas as $schema) {
                if (in_array($schema['name'], ['INFORMATION_SCHEMA', 'PUBLIC'])) {
                    continue;
                }
                $this->sourceConnection->useRole($mainRoleWithGrants['name']);
                $this->destinationConnection->useRole($mainRoleWithGrants['name']);
                $this->sourceConnection->grantRoleToUser($this->config->getSourceSnowflakeUser(), $schema['owner']);
                $this->destinationConnection->grantRoleToUser(
                    $this->config->getTargetSnowflakeUser(),
                    $schema['owner']
                );
                $this->sourceConnection->useRole($schema['owner']);

                $tables = $this->sourceConnection->fetchAll(sprintf(
                    'SHOW TABLES IN SCHEMA %s.%s',
                    Helper::quoteIdentifier($database),
                    Helper::quoteIdentifier($schema['name'])
                ));
                foreach ($tables as $table) {
                    $primaryKeys = $this->sourceConnection->fetchAll(sprintf(
                        'SHOW PRIMARY KEYS IN TABLE %s.%s.%s',
                        Helper::quoteIdentifier($database),
                        Helper::quoteIdentifier($schema['name']),
                        Helper::quoteIdentifier($table['name'])
                    ));
                    if (!$primaryKeys) {
                        $this->logger->warning(sprintf(
                            'Table %s.%s.%s has no primary key. Skipping',
                            $database,
                            $schema['name'],
                            $table['name']
                        ));
                        continue;
                    }
                    $primaryKeys = array_map(fn($v) => $v['column_name'], $primaryKeys);

                    $columns = $this->sourceConnection->fetchAll(sprintf(
                        'SHOW COLUMNS IN TABLE %s.%s.%s',
                        Helper::quoteIdentifier($database),
                        Helper::quoteIdentifier($schema['name']),
                        Helper::quoteIdentifier($table['name'])
                    ));
                    $columns = array_map(fn($v) => $v['column_name'], $columns);
                    $columns = array_filter($columns, fn($v) => !in_array($v, $primaryKeys));
                    $columns = array_filter($columns, fn($v) => $v !== '_timestamp');

                    $arguments = [
                        'sourceAccount' => $sourceAccount,
                        'sourceUser' => $this->config->getSourceSnowflakeUser(),
                        'sourcePassword' => $this->config->getSourceSnowflakePassword(),
                        'targetAccount' => $targetAccount,
                        'targetUser' => $this->config->getTargetSnowflakeUser(),
                        'targetPassword' => $this->config->getTargetSnowflakePassword(),
                        'role' => $schema['owner'],
                        'warehouse' => $warehousesGrant[0]['name'],
                        'database' => $database,
                        'schema' => $schema['name'],
                        'table' => $table['name'],
                        'extraColumns' => implode(',', $columns),
                        'primaryKeys' => implode(',', $primaryKeys),
                    ];

                    array_walk($arguments, function (&$value, $key): void {
                        $value = sprintf('--%s "%s"', $key, $value);
                    });

                    $process = Process::fromShellCommandline(
                        'python3 ' . __DIR__ . '/dataDiff.py ' . implode(' ', $arguments)
                    );
                    $process->setTimeout(null);
                    $process->run();
                    if (!$process->isSuccessful()) {
                        $this->logger->warning(sprintf(
                            'Checking table "%s.%s.%s" ends with error: "%s"',
                            $database,
                            $schema['name'],
                            $table['name'],
                            $process->getOutput()
                        ));
                    } else {
                        $this->logger->info(sprintf(
                            'Checking table "%s.%s.%s" ends successfully. %s',
                            $database,
                            $schema['name'],
                            $table['name'],
                            $process->getOutput()
                        ));
                    }
                }
            }
        }
    }

    public function printUnusedGrants(array $grants): void
    {
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            [
                'grants' => [
                    'other' => $otherGrants,
                ],
                'futureGrants' => [
                    'other' => $otherFutureGrants,
                ],
            ] = Helper::parseGrantsToObjects($grants[$databaseRole]);

            foreach ($otherGrants as $grant) {
                $this->logger->alert(sprintf(
                    'Unused grant "%s": GRANT %s ON %s %s TO %s %s %s',
                    $grant['name'],
                    $grant['privilege'],
                    $grant['granted_on'],
                    $grant['granted_on'] !== 'ACCOUNT' ? $grant['name'] : '',
                    $grant['granted_to'],
                    Helper::quoteIdentifier($grant['grantee_name']),
                    $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
                ));
            }

            foreach ($otherFutureGrants as $grant) {
                $this->logger->alert(sprintf(
                    'Unused FUTURE grant "%s": GRANT %s ON FUTURE TABLES IN SCHEMA %s TO ROLE %s %s',
                    $grant['name'],
                    $grant['privilege'],
                    $grant['name'],
                    Helper::quoteIdentifier($grant['grantee_name']),
                    $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
                ));
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
            Helper::quoteIdentifier($role)
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

        $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);
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
                Helper::quoteIdentifier($role['name'])
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
            'name' => Helper::quoteIdentifier($database . '_SHARE'),
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
            Helper::quoteIdentifier($role)
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
                Helper::quoteIdentifier($database . '_OLD'),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($table)
            ));
            $lastUpdateTableInShareDatabase = $this->destinationConnection->fetchAll(sprintf(
                $sqlTemplate,
                Helper::quoteIdentifier($database . '_SHARE'),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($table)
            ));
        } catch (RuntimeException $e) {
            return false;
        }

        return $lastUpdateTableInOldDatabase[0]['maxTimestamp'] === $lastUpdateTableInShareDatabase[0]['maxTimestamp'];
    }

    private function grantsPrivilegesToOldDatabase(string $database, string $databaseRole, string $mainRole): void
    {
        $currentRole = $this->destinationConnection->getCurrentRole();
        $this->destinationConnection->useRole($mainRole);
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
                Helper::quoteIdentifier($database . '_OLD'),
                $databaseRole
            ),
            sprintf(
                'grant all on all schemas in database %s to role %s;',
                Helper::quoteIdentifier($database . '_OLD'),
                $databaseRole
            ),
            sprintf(
                'grant all on all tables in database %s to role %s;',
                Helper::quoteIdentifier($database . '_OLD'),
                $databaseRole
            ),
        ];

        foreach ($sqls as $sql) {
            $this->destinationConnection->query($sql);
        }
        $this->destinationConnection->useRole($currentRole);
    }
}
