<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Exception\NoWarehouseException;
use ProjectMigrationTool\Snowflake\BuildQueryHelper;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\Helper;
use Psr\Log\LoggerInterface;
use Throwable;

class MigrateStructure
{
    private const SKIP_CLONE_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];

    private array $usedUsers = [];

    public function __construct(
        private readonly Connection $sourceConnection,
        private readonly Connection $destinationConnection,
        private readonly LoggerInterface $logger,
        private readonly Config $config,
        private readonly string $mainMigrationRoleSourceAccount,
        private readonly string $mainMigrationRoleTargetAccount,
        private readonly array $databases,
    ) {
    }

    public function cloneDatabaseWithGrants(string $mainRole, array $grants): void
    {
        foreach ($this->databases as $database) {
            $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            [
                'grants' => [
                    'databases' => $databaseGrants,
                    'schemas' => $schemasGrants,
                    'tables' => $tablesGrants,
                    'views' => $viewsGrants,
                    'functions' => $functionsGrants,
                    'procedures' => $proceduresGrants,
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
                $sourceDatabase['retention_time']
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

            $this->assignSharePrivilegesToRole($database, $databaseRole);
            $this->assignForeignGrants($schemasGrants, $databaseRole);

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
                    try {
                        $this->destinationConnection->useWarehouse($ownershipOnTable['granted_by']);
                    } catch (NoWarehouseException $exception) {
                        if (!preg_match('/^(KEBOOLA|SAPI|sapi)_WORKSPACE_/', $ownershipOnTable['granted_by'])) {
                            throw $exception;
                        }
                        $this->logger->info(sprintf(
                            'Warning: Skipping table: %s, because: %s',
                            $tableName,
                            $exception->getMessage()
                        ));
                    }

                    if ($this->canCloneTable($mainRole, $database, $schemaName, $tableName)) {
                        $this->grantUsageToOldTable(
                            $database,
                            $schemaName,
                            $tableName,
                            $mainRole,
                            $ownershipOnTable['granted_by']
                        );
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

                        try {
                            $this->destinationConnection->query(sprintf(
                                'CREATE TABLE %s.%s.%s LIKE %s.%s.%s;',
                                Helper::quoteIdentifier($database),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                                Helper::quoteIdentifier($shareDbName),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                            ));

                            $this->destinationConnection->query(sprintf(
                                'INSERT INTO %s.%s.%s SELECT * FROM %s.%s.%s;',
                                Helper::quoteIdentifier($database),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                                Helper::quoteIdentifier($shareDbName),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                            ));
                        } catch (RuntimeException $e) {
                            $this->logger->info(sprintf(
                                'Warning: Skip creating table %s.%s.%s. Error: "%s".',
                                Helper::quoteIdentifier($database),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                                $e->getMessage()
                            ));
                        }
                    }

                    $tableGrants = array_filter($tableGrants, fn($v) => $v['privilege'] !== 'OWNERSHIP');
                    foreach ($tableGrants as $tableGrant) {
                        $this->destinationConnection->assignGrantToRole($tableGrant);
                    }
                }
            }

            $this->copyViews($database, $databaseRole, $viewsGrants);

            $this->copyFunctions($database, $functionsGrants);

            $this->copyProcedures($database, $proceduresGrants);
        }
    }

    public function createMainRole(array $mainRoleWithGrants, array $userPasswords): void
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
            $this->createWarehouse($warehouse);
            $this->destinationConnection->assignGrantToRole($warehouse);
        }

        if (isset($userPasswords[$user])) {
            $password = $userPasswords[$user];
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

        foreach ($projectUsers as $projectUser) {
            $this->createUser($projectUser, $userPasswords);
            $this->destinationConnection->assignGrantToRole($projectUser);
        }

        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);

        $this->destinationConnection->query(sprintf(
            'GRANT ROLE %s TO ROLE SYSADMIN;',
            $mainRole
        ));
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

    public function reApplyFailedGrants(): void
    {
        $failedGrants = $this->destinationConnection->getFailedGrants();
        if (count($failedGrants) === 0) {
            $this->logger->info('There were no failed grants, nothing to apply');
            return;
        }
        $this->logger->info('Attempting to apply ' . count($failedGrants) . ' failed grants');
        foreach ($failedGrants as $grant) {
            $this->destinationConnection->assignGrantToRole($grant);
        }
    }

    public function printUnusedGrants(array $grants): void
    {
        foreach ($grants as $databaseGrants) {
            [
                'grants' => [
                    'other' => $otherGrants,
                ],
                'futureGrants' => [
                    'other' => $otherFutureGrants,
                ],
            ] = Helper::parseGrantsToObjects($databaseGrants);

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

    private function createWarehouse(array $warehouse): string
    {
        $role = $this->sourceConnection->getCurrentRole();
        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
        $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);
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

        $this->sourceConnection->useRole($role);
        return $warehouseInfo['size'];
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

    private function canCloneTable(string $mainRole, string $database, string $schema, string $table): bool
    {

        $sqlTemplate = 'SELECT max("_timestamp") as "maxTimestamp" FROM %s.%s.%s';

        $currentRole = $this->destinationConnection->getCurrentRole();
        try {
            $this->destinationConnection->useRole($mainRole);

            $lastUpdateTableInOldDatabase = $this->destinationConnection->fetchAll(sprintf(
                $sqlTemplate,
                Helper::quoteIdentifier($database . '_OLD'),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($table)
            ));

            $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
            $lastUpdateTableInShareDatabase = $this->destinationConnection->fetchAll(sprintf(
                $sqlTemplate,
                Helper::quoteIdentifier($database . '_SHARE'),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($table)
            ));
        } catch (RuntimeException $e) {
            return false;
        } finally {
            $this->destinationConnection->useRole($currentRole);
        }

        return $lastUpdateTableInOldDatabase[0]['maxTimestamp'] === $lastUpdateTableInShareDatabase[0]['maxTimestamp'];
    }

    private function grantUsageToOldTable(
        string $database,
        string $schema,
        string $table,
        string $mainRole,
        string $role
    ): void {
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
                'grant usage on database %s to role %s;',
                Helper::quoteIdentifier($database . '_OLD'),
                Helper::quoteIdentifier($role)
            ),
            sprintf(
                'grant usage on schema %s.%s to role %s;',
                Helper::quoteIdentifier($database . '_OLD'),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($role)
            ),
            sprintf(
                'grant select on table %s.%s.%s to role %s;',
                Helper::quoteIdentifier($database . '_OLD'),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($table),
                Helper::quoteIdentifier($role)
            ),
        ];

        foreach ($sqls as $sql) {
            $this->destinationConnection->query($sql);
        }
        $this->destinationConnection->useRole($currentRole);
    }

    private function assignForeignGrants(array $grants, string $databaseRole): void
    {
        $foreignGrants = array_filter($grants, fn($v) => $v['granted_by'] !== $databaseRole);

        array_walk($foreignGrants, fn($grant) => $this->destinationConnection->assignGrantToRole($grant));
    }

    private function copyViews(string $database, string $databaseRole, array $viewsGrants): void
    {
        $this->logger->info(sprintf('Cloning views from database "%s"', $database));
        $this->sourceConnection->grantRoleToUser($this->config->getSourceSnowflakeUser(), $databaseRole);
        $this->sourceConnection->useRole($databaseRole);
        $views = $this->sourceConnection->fetchAll(sprintf(
            'SHOW VIEWS IN DATABASE %s;',
            Helper::quoteIdentifier($database),
        ));

        $views = array_filter($views, fn($v) => !in_array($v['schema_name'], self::SKIP_CLONE_SCHEMAS));

        $try = 0;
        while ($views && $try < 5) {
            if ($try > 0) {
                $this->logger->info(sprintf(
                    'Attempting to create %s failed views.',
                    count($views)
                ));
            }
            foreach ($views as $viewKey => $view) {
                $this->destinationConnection->useRole($view['owner']);

                $this->destinationConnection->query(sprintf(
                    'USE SCHEMA %s.%s;',
                    Helper::quoteIdentifier($view['database_name']),
                    Helper::quoteIdentifier($view['schema_name'])
                ));

                try {
                    $this->destinationConnection->query($view['text']);
                    unset($views[$viewKey]);
                } catch (Throwable $e) {
                    $this->logger->info(sprintf(
                        'Warning: Skip creating view %s.%s.%s. Error: "%s".',
                        Helper::quoteIdentifier($view['database_name']),
                        Helper::quoteIdentifier($view['schema_name']),
                        Helper::quoteIdentifier($view['name']),
                        $e->getMessage()
                    ));
                }
            }
            $try++;
        }

        foreach ($viewsGrants as $viewsGrant) {
            if ($viewsGrant['privilege'] === 'OWNERSHIP') {
                continue;
            }
            $this->destinationConnection->assignGrantToRole($viewsGrant);
        }
    }

    private function copyFunctions(string $database, array $functionsGrants): void
    {
        $this->logger->info(sprintf('Cloning functions from database "%s"', $database));
        $functions = $this->sourceConnection->fetchAll(sprintf(
            'SHOW FUNCTIONS IN DATABASE %s;',
            Helper::quoteIdentifier($database),
        ));

        $functions = array_filter($functions, fn($v) => $v['catalog_name'] === $database);

        $this->destinationConnection->query('USE DATABASE ' . Helper::quoteIdentifier($database) . ';');
        foreach ($functions as $function) {
            preg_match('/.*\((.*)\) RETURN/', $function['arguments'], $matches);
            $descFunction = $this->sourceConnection->fetchAll(sprintf(
                'DESC FUNCTION %s.%s.%s(%s)',
                Helper::quoteIdentifier($function['catalog_name']),
                Helper::quoteIdentifier($function['schema_name']),
                Helper::quoteIdentifier($function['name']),
                $matches[1]
            ));

            $functionParams = array_combine(
                array_map(fn($v) => $v['property'], $descFunction),
                array_map(fn($v) => $v['value'], $descFunction)
            );

            switch ($function['language']) {
                case 'SQL':
                    $functionQuery = BuildQueryHelper::buildSqlFunctionQuery($function, $functionParams);
                    break;
                case 'PYTHON':
                    $functionQuery = BuildQueryHelper::buildPythonFunctionQuery($function, $functionParams);
                    break;
                default:
                    $this->logger->warning(sprintf(
                        'Warning: Skip creating function "%s". Language "%s" is not supported.',
                        Helper::quoteIdentifier($function['name']),
                        $function['language']
                    ));
                    continue 2;
            }

            $ownership = array_filter(
                $functionsGrants,
                fn($v) => str_contains($v['granted_by'], $function['schema_name'])
            );

            $this->destinationConnection->useRole(current($ownership)['granted_by']);

            $this->destinationConnection->query(sprintf(
                'USE SCHEMA %s.%s;',
                Helper::quoteIdentifier($function['catalog_name']),
                Helper::quoteIdentifier($function['schema_name'])
            ));

            try {
                $this->destinationConnection->query($functionQuery);
            } catch (Throwable $e) {
                $this->logger->info(sprintf(
                    'Skip creating function %s.%s.%s. Error: "%s".',
                    Helper::quoteIdentifier($function['catalog_name']),
                    Helper::quoteIdentifier($function['schema_name']),
                    Helper::quoteIdentifier($function['name']),
                    $e->getMessage()
                ));
            }
        }

        foreach ($functionsGrants as $functionsGrant) {
            if ($functionsGrant['privilege'] === 'OWNERSHIP') {
                continue;
            }
            $this->destinationConnection->assignGrantToRole($functionsGrant);
        }
    }

    private function copyProcedures(string $database, array $proceduresGrants): void
    {
        $this->logger->info(sprintf('Cloning procedures from database "%s"', $database));

        $procedures = $this->sourceConnection->fetchAll(sprintf(
            'SHOW PROCEDURES IN DATABASE %s;',
            Helper::quoteIdentifier($database),
        ));

        $procedures = array_filter($procedures, fn($v) => $v['catalog_name'] === $database);

        foreach ($procedures as $procedure) {
            preg_match('/.*\((.*)\) RETURN/', $procedure['arguments'], $matches);
            $descFunction = $this->sourceConnection->fetchAll(sprintf(
                'DESC PROCEDURE %s.%s.%s(%s)',
                Helper::quoteIdentifier($procedure['catalog_name']),
                Helper::quoteIdentifier($procedure['schema_name']),
                Helper::quoteIdentifier($procedure['name']),
                $matches[1]
            ));

            $procedureParams = array_combine(
                array_map(fn($v) => $v['property'], $descFunction),
                array_map(fn($v) => $v['value'], $descFunction)
            );

            switch ($procedureParams['language']) {
                case 'SQL':
                    $procedureQuery = BuildQueryHelper::buildSqlProcedureQuery($procedure, $procedureParams);
                    break;
                case 'JAVA':
                    $procedureQuery = BuildQueryHelper::buildJavaProcedureQuery($procedure, $procedureParams);
                    break;
                case 'JAVASCRIPT':
                    $procedureQuery = BuildQueryHelper::buildJavascriptProcedureQuery($procedure, $procedureParams);
                    break;
                case 'PYTHON':
                    $procedureQuery = BuildQueryHelper::buildPythonProcedureQuery($procedure, $procedureParams);
                    break;
                default:
                    $this->logger->warning(sprintf(
                        'Warning: Skip creating procedure "%s". Language "%s" is not supported.',
                        Helper::quoteIdentifier($procedure['name']),
                        $procedure['language']
                    ));
                    continue 2;
            }

            $ownership = array_filter(
                $proceduresGrants,
                fn($v) => str_contains($v['granted_by'], $procedure['schema_name'])
            );

            $this->destinationConnection->useRole(current($ownership)['granted_by']);

            $this->destinationConnection->query(sprintf(
                'USE SCHEMA %s.%s;',
                Helper::quoteIdentifier($procedure['catalog_name']),
                Helper::quoteIdentifier($procedure['schema_name'])
            ));

            try {
                $this->destinationConnection->query($procedureQuery);
            } catch (Throwable $e) {
                $this->logger->info(sprintf(
                    'Skip creating procedure %s.%s.%s. Error: "%s".',
                    Helper::quoteIdentifier($procedure['catalog_name']),
                    Helper::quoteIdentifier($procedure['schema_name']),
                    Helper::quoteIdentifier($procedure['name']),
                    $e->getMessage()
                ));
            }
        }

        foreach ($proceduresGrants as $proceduresGrant) {
            if ($proceduresGrant['privilege'] === 'OWNERSHIP') {
                continue;
            }
            $this->destinationConnection->assignGrantToRole($proceduresGrant);
        }
    }
}
