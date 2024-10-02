<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\BuildQueryHelper;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\ValueObject\FutureGrantToRole;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\GrantToUser;
use ProjectMigrationTool\ValueObject\ProjectRoles;
use ProjectMigrationTool\ValueObject\Role;
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

    /**
     * @param ProjectRoles[] $roles
     */
    public function cloneDatabaseWithGrants(Role $mainRole, array $roles): void
    {
        foreach ($this->databases as $database) {
            $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);
            $databaseRoleName = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            $projectRoles = $roles[$databaseRoleName];

            $this->destinationConnection->useRole($mainRole->getName());

            $shareDbName = $database . '_SHARE';

            $sourceDatabases = $this->sourceConnection->fetchAll(sprintf(
                'SHOW DATABASES LIKE %s',
                QueryBuilder::quote($database)
            ));
            if (!$this->assert(
                count($sourceDatabases) === 1,
                sprintf('Database "%s" not found on source account.', $database)
            )) {
                continue;
            }
            $sourceDatabase = current($sourceDatabases);

            $this->logger->info(sprintf('Migrate database "%s".', $database));
            $this->destinationConnection->query(sprintf(
                'CREATE DATABASE %s DATA_RETENTION_TIME_IN_DAYS=%s;',
                Helper::quoteIdentifier($database),
                $sourceDatabase['retention_time']
            ));

            foreach ($projectRoles->getDatabaseGrantsFromAllRoles() as $databaseGrant) {
                if ($databaseGrant->getPrivilege() === 'OWNERSHIP') {
                    $this->destinationConnection->assignGrantToRole(
                        GrantToRole::fromArray([
                            'name' => $databaseGrant->getName(),
                            'privilege' => $databaseGrant->getPrivilege(),
                            'granted_on' => $databaseGrant->getGrantedOn(),
                            'granted_to' => $databaseGrant->getGrantedTo(),
                            'grantee_name' => $databaseGrant->getGranteeName(),
                            'grant_option' => $databaseGrant->getGrantOption(),
                            'granted_by' => $mainRole->getName(),
                        ])
                    );
                }
                $this->destinationConnection->assignGrantToRole($databaseGrant);
            }

            $this->assignSharePrivilegesToRole($database, $projectRoles->getRole($databaseRoleName)->getName());
            $this->assignForeignGrants(
                $projectRoles->getSchemaGrantsFromAllRoles(),
                $projectRoles->getRole($databaseRoleName)->getName()
            );

            $this->destinationConnection->useRole($projectRoles->getRole($databaseRoleName)->getName());

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

                /** @var GrantToRole[] $schemaGrants */
                $schemaGrants = Helper::filterSchemaGrants(
                    $database,
                    $schemaName,
                    $projectRoles->getSchemaGrantsFromAllRoles()
                );
                /** @var FutureGrantToRole[] $schemaFutureGrants */
                $schemaFutureGrants = Helper::filterSchemaGrants(
                    $database,
                    $schemaName,
                    $projectRoles->getFutureTableGrantsFromAllRoles()
                );
                $ownershipOnSchema = array_filter($schemaGrants, fn($v) => $v->getPrivilege() === 'OWNERSHIP');
                if (!$this->assert(
                    count($ownershipOnSchema) === 1,
                    sprintf('Schema %s ownership not found.', $schemaName)
                ) || !$ownershipOnSchema) {
                    continue;
                }

                $schemaOptions = array_map(fn($v) => trim($v), explode(',', $schema['options']));

                $this->destinationConnection->useRole(current($ownershipOnSchema)->getGrantedBy());
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

                $primaryKeys = $this->sourceConnection->fetchAll(sprintf(
                    'SHOW PRIMARY KEYS IN SCHEMA %s.%s;',
                    Helper::quoteIdentifier($database),
                    Helper::quoteIdentifier($schemaName)
                ));

                foreach ($tables as $table) {
                    $tableName = $table['name'];

                    $tableGrants = array_filter(
                        $projectRoles->getTableGrantsFromAllRoles(),
                        function (GrantToRole $v) use ($database, $schemaName, $tableName) {
                            $validSchema = Helper::generateFormattedQuoteCombinations(
                                $database,
                                $schemaName,
                                $tableName
                            );
                            return in_array($v->getName(), $validSchema);
                        }
                    );
                    $ownershipOnTable = array_filter(
                        $tableGrants,
                        fn(GrantToRole $v) => $v->getPrivilege() === 'OWNERSHIP'
                    );
                    if (!$this->assert(
                        count($ownershipOnTable) === 1,
                        sprintf('Table %s.%s.%s ownership not found.', $database, $schemaName, $tableName)
                    ) || !$ownershipOnTable) {
                        continue;
                    }

                    $ownershipOnTable = current($ownershipOnTable);
                    $this->logger->info(
                        sprintf(
                            'Ownership on table "%s"."%s" is To: %s, Name: %s, Grantee: %s".',
                            $schemaName,
                            $tableName,
                            $ownershipOnTable->getGrantedTo(),
                            $ownershipOnTable->getName(),
                            $ownershipOnTable->getGranteeName()
                        )
                    );

                    $this->assignSharePrivilegesToRole($database, $ownershipOnTable->getGrantedBy());
                    $this->destinationConnection->useRole($ownershipOnTable->getGrantedBy());

                    $this->logger->info(sprintf('Creating table structure "%s"."%s"', $schemaName, $tableName));
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
                    } catch (RuntimeException $e) {
                        $this->logger->info(sprintf(
                            'Warning: Skip creating table %s.%s.%s. Error: "%s".',
                            Helper::quoteIdentifier($database),
                            Helper::quoteIdentifier($schemaName),
                            Helper::quoteIdentifier($tableName),
                            $e->getMessage()
                        ));
                    }

                    $tableGrants = array_filter($tableGrants, fn(GrantToRole $v) => $v->getPrivilege() !== 'OWNERSHIP');
                    foreach ($tableGrants as $tableGrant) {
                        $this->destinationConnection->assignGrantToRole($tableGrant);
                    }

                    $tablePrimaryKeys = array_filter(
                        $primaryKeys,
                        fn($v) => $v['table_name'] === $tableName
                    );
                    if ($tablePrimaryKeys) {
                        try {
                            $this->destinationConnection->query(sprintf(
                                'ALTER TABLE %s.%s.%s ADD PRIMARY KEY (%s);',
                                Helper::quoteIdentifier($database),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                                implode(
                                    ',',
                                    array_map(fn($v) => Helper::quoteIdentifier($v['column_name']), $tablePrimaryKeys)
                                )
                            ));
                        } catch (RuntimeException $e) {
                            $this->logger->info(sprintf(
                                'Warning: Skip creating primary key on table %s.%s.%s. Error: "%s".',
                                Helper::quoteIdentifier($database),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                                $e->getMessage()
                            ));
                        }
                    }
                }
            }

            $this->copyViews($database, $databaseRoleName, $projectRoles);
            $this->copyFunctions($database, $projectRoles);
            $this->copyProcedures($database, $projectRoles);
        }
    }

    public function createMainRoleAndUser(Role $mainRoleWithGrants): void
    {
        $user = $mainRole = $mainRoleWithGrants->getName();

        $mainRoleExists = $this->destinationConnection->fetchAll(sprintf(
            'SHOW ROLES LIKE %s',
            QueryBuilder::quote($mainRole),
        ));
        if ($mainRoleExists) {
            $grantsToTargetUser = $this->destinationConnection->fetchAll(sprintf(
                'SHOW GRANTS TO USER %s',
                QueryBuilder::quoteIdentifier($this->config->getTargetSnowflakeUser()),
            ));
            $mainRoleExistsOnTargetUser = array_filter(
                $grantsToTargetUser,
                fn($v) => $v['role'] === $mainRole,
            );

            if (!$mainRoleExistsOnTargetUser) {
                throw new UserException('Main role is exists but not assign to migrate user.');
            }
            return;
        }

        $this->destinationConnection->createRole(
            GrantToRole::fromArray([
                'privilege' => 'OWNERSHIP',
                'name' => $mainRole,
                'granted_to' => 'ROLE',
                'granted_by' =>  $this->config->getTargetSnowflakeRole(),
                'granted_on' => 'ROLE',
                'grant_option' => 'true',
                'grantee_name' => $this->config->getTargetSnowflakeRole(),
            ]),
            $this->config->getTargetSnowflakeUser(),
        );

        $mainRoleGrants = [
            'GRANT CREATE DATABASE ON ACCOUNT TO ROLE %s;',
            'GRANT CREATE ROLE ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
            'GRANT CREATE USER ON ACCOUNT TO ROLE %s WITH GRANT OPTION;',
        ];

        foreach ($mainRoleGrants as $mainRoleGrant) {
            $this->destinationConnection->query(sprintf($mainRoleGrant, $mainRole));
        }

        foreach ($mainRoleWithGrants->getAssignedGrants()->getWarehouseGrants() as $warehouse) {
            $this->createWarehouse($warehouse);
            $this->destinationConnection->assignGrantToRole($warehouse);
        }

        $this->destinationConnection->query(sprintf(
            'CREATE USER IF NOT EXISTS %s PASSWORD=%s DEFAULT_ROLE=%s',
            $user,
            QueryBuilder::quote(Helper::generateRandomString()),
            $mainRole
        ));

        $this->destinationConnection->query(sprintf(
            'GRANT ROLE %s TO USER %s;',
            $mainRole,
            $user
        ));

        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
        $this->destinationConnection->query(sprintf(
            'GRANT ROLE %s TO ROLE SYSADMIN;',
            $mainRoleWithGrants->getName(),
        ));
    }

    public function createProjectRoleAndUser(Role $mainRoleWithGrants): void
    {
        $projectUsers = array_filter(
            $mainRoleWithGrants->getAssignedGrants()->getUserGrants(),
            function (GrantToRole $v) {
                if ($v->getPrivilege() !== 'OWNERSHIP') {
                    return false;
                }
                return in_array($v->getName(), $this->databases);
            }
        );

        $sourceGrants = array_map(
            fn(array $v) => GrantToRole::fromArray($v),
            $this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s',
                Helper::quoteIdentifier($this->sourceConnection->getCurrentRole()),
            ))
        );

        $sourceGrants = array_filter(
            $sourceGrants,
            fn($v) => $v->getGrantedOn() === 'WAREHOUSE' && $v->getPrivilege() === 'USAGE'
        );
        assert(count($sourceGrants) > 0);

        foreach ($projectUsers as $projectUser) {
            $this->createUser($projectUser);
            $this->destinationConnection->assignGrantToRole($projectUser);
        }
    }

    /**
     * @param ProjectRoles[] $grants
     */
    public function migrateUsersRolesAndGrants(Role $mainRole, array $grants): void
    {
        $this->logger->info('Migrating users and roles.');
        $this->destinationConnection->useRole($mainRole->getName());
        // first step - migrate users and roles (without grants)
        foreach ($this->databases as $database) {
            $databaseRoleName = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            $projectRoles = $grants[$databaseRoleName];

            $this->destinationConnection->createRole(
                GrantToRole::fromArray([
                    'name' => $projectRoles->getRole($databaseRoleName)->getName(),
                    'granted_by' => $mainRole->getName(),
                    'privilege' => 'OWNERSHIP',
                    'granted_on' => 'ROLE',
                    'granted_to' => 'ROLE',
                    'grantee_name' => $mainRole->getName(),
                    'grant_option' => 'TRUE',
                ]),
                $this->config->getTargetSnowflakeUser()
            );

            foreach ($projectRoles->getAccountGrantsFromAllRoles() as $grant) {
                $this->destinationConnection->assignGrantToRole($grant);
            }

            foreach ($projectRoles->getRoleGrantsFromAllRoles() as $grant) {
                if ($grant->getPrivilege() === 'OWNERSHIP') {
                    $this->destinationConnection->createRole($grant, $this->config->getTargetSnowflakeUser());
                }
            }

            foreach ($projectRoles->getUserGrantsFromAllRoles() as $grant) {
                $this->createUser($grant);
            }
        }

        $this->logger->info('Migrating grants of warehouses/users and roles.');
        // second step - migrate all grants of roles/users/warehouses/account
        foreach ($this->databases as $database) {
            $databaseRoleName = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            $projectRoles = $grants[$databaseRoleName];

            foreach ($projectRoles->getRoleGrantsFromAllRoles() as $grant) {
                $this->destinationConnection->assignGrantToRole($grant);
            }

            foreach ($projectRoles->getWarehouseGrantsFromAllRoles() as $grant) {
                $this->destinationConnection->assignGrantToRole($grant);
            }

            foreach ($projectRoles->getUserGrantsFromAllRoles() as $grant) {
                if ($this->config->skipCheck() && !in_array($grant->getName(), $this->usedUsers)) {
                    continue;
                }
                $this->destinationConnection->assignGrantToRole($grant);
            }
        }
    }

    public function grantRoleToUsers(): void
    {
        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
        $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);

        foreach ($this->usedUsers as $user) {
            /** @var GrantToUser[] $grants */
            $grants = array_map(
                fn(array $v) => GrantToUser::fromArray($v),
                $this->sourceConnection->fetchAll(sprintf(
                    'SHOW GRANTS TO USER %s',
                    Helper::quoteIdentifier($user)
                ))
            );

            foreach ($grants as $grant) {
                $this->destinationConnection->useRole($grant->getGrantedBy());
                $this->destinationConnection->query(sprintf(
                    'GRANT ROLE %s TO %s %s',
                    $grant->getRole(),
                    $grant->getGrantedTo(),
                    $grant->getGranteeName(),
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

    /**
     * @param ProjectRoles[] $projectRoles
     */
    public function printUnusedGrants(array $projectRoles): void
    {
        foreach ($projectRoles as $projectRole) {
            foreach ($projectRole->getOtherGrantsFromAllRoles() as $grant) {
                $this->logger->alert(sprintf(
                    'Unused grant "%s": GRANT %s ON %s %s TO %s %s %s',
                    $grant->getName(),
                    $grant->getPrivilege(),
                    $grant->getGrantedOn(),
                    $grant->getGrantedOn() !== 'ACCOUNT' ? $grant->getName() : '',
                    $grant->getGrantedTo(),
                    Helper::quoteIdentifier($grant->getGranteeName()),
                    $grant->getGrantOption() === 'true' ? 'WITH GRANT OPTION' : '',
                ));
            }

            foreach ($projectRole->getFutureOtherGrantsFromAllRoles() as $grant) {
                $this->logger->alert(sprintf(
                    'Unused FUTURE grant "%s": GRANT %s ON FUTURE TABLES IN SCHEMA %s TO ROLE %s %s',
                    $grant->getName(),
                    $grant->getPrivilege(),
                    $grant->getName(),
                    Helper::quoteIdentifier($grant->getGranteeName()),
                    $grant->getGrantOption() === 'true' ? 'WITH GRANT OPTION' : '',
                ));
            }
        }
    }

    private function createWarehouse(GrantToRole $warehouse): string
    {
        $role = $this->sourceConnection->getCurrentRole();
        $this->destinationConnection->useRole($this->mainMigrationRoleTargetAccount);
        $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);
        $warehouseInfo = $this->sourceConnection->fetchAll(sprintf(
            'SHOW WAREHOUSES LIKE %s',
            QueryBuilder::quote($warehouse->getName())
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

    private function createUser(GrantToRole $userGrant): void
    {
        $this->destinationConnection->useRole($userGrant->getGrantedBy());

        $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);
        $describeUser = $this->sourceConnection->fetchAll(sprintf(
            'SHOW USERS LIKE %s',
            QueryBuilder::quote($userGrant->getName())
        ));
        if (!$this->assert(
            count($describeUser) === 1,
            sprintf('User "%s" not found.', $userGrant->getName())
        ) || !$describeUser) {
            return;
        }

        if (!in_array($userGrant->getName(), $this->usedUsers)) {
            $this->usedUsers[] = $userGrant->getName();
        }

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

        $describeUser['password'] = sprintf(
            'PASSWORD = %s',
            QueryBuilder::quote(Helper::generateRandomString())
        );

        $this->destinationConnection->query(sprintf(
            'CREATE USER %s %s',
            $userGrant->getName(),
            implode(' ', $describeUser),
        ));
    }

    private function assignSharePrivilegesToRole(string $database, string $role): void
    {
        $this->destinationConnection->assignGrantToRole(GrantToRole::fromArray([
            'privilege' => 'IMPORTED PRIVILEGES',
            'granted_on' => 'DATABASE',
            'name' => Helper::quoteIdentifier($database . '_SHARE'),
            'granted_to' => 'ROLE',
            'grantee_name' => $role,
            'grant_option' => 'false',
            'granted_by' => $this->mainMigrationRoleTargetAccount,
        ]));
    }

    /**
     * @param GrantToRole[] $grants
     */
    private function assignForeignGrants(array $grants, string $databaseRole): void
    {
        $foreignGrants = array_filter($grants, fn($v) => $v->getGrantedBy() !== $databaseRole);

        array_walk($foreignGrants, fn($grant) => $this->destinationConnection->assignGrantToRole($grant));
    }

    private function copyViews(string $database, string $databaseRoleName, ProjectRoles $projectRoles): void
    {
        $databaseRole = $projectRoles->getRole($databaseRoleName)->getName();
        $viewsGrants = $projectRoles->getViewGrantsFromAllRoles();

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
                $ownershipRole = $projectRoles->getRole($view['owner']);
                $warehouseGrants = $ownershipRole->getAssignedGrants()->getWarehouseGrants();
                if (!$this->assert(
                    count($warehouseGrants) > 0,
                    sprintf('Warehouse grant for %s view not found.', $view['name'])
                ) || !$warehouseGrants) {
                    continue;
                }

                $this->destinationConnection->useWarehouse(current($warehouseGrants)->getName());
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
            if ($viewsGrant->getPrivilege() === 'OWNERSHIP') {
                continue;
            }
            $this->destinationConnection->assignGrantToRole($viewsGrant);
        }
    }

    private function copyFunctions(string $database, ProjectRoles $projectRoles): void
    {
        $functionsGrants = $projectRoles->getFunctionGrantsFromAllRoles();

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
                    $functionQuery = BuildQueryHelper::functionSql($function, $functionParams);
                    break;
                case 'PYTHON':
                    $functionQuery = BuildQueryHelper::functionPython($function, $functionParams);
                    break;
                case 'JAVASCRIPT':
                    $functionQuery = BuildQueryHelper::functionJavaScript($function, $functionParams);
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
                fn($v) => str_contains($v->getGrantedBy(), $function['schema_name'])
            );
            if (!$this->assert(
                count($ownership) > 0,
                sprintf('Ownership grant for %s view not found.', $function['name'])
            ) || !$ownership) {
                continue;
            }
            $ownershipRole = current($ownership);
            $warehouseGrants = $projectRoles
                ->getRole($ownershipRole->getGrantedBy())
                ->getAssignedGrants()
                ->getWarehouseGrants();
            $this->destinationConnection->useWarehouse(current($warehouseGrants)->getName());

            $this->destinationConnection->useRole($ownershipRole->getGrantedBy());

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
            if ($functionsGrant->getPrivilege() === 'OWNERSHIP') {
                continue;
            }
            $this->destinationConnection->assignGrantToRole($functionsGrant);
        }
    }

    private function copyProcedures(string $database, ProjectRoles $projectRoles): void
    {
        $proceduresGrants = $projectRoles->getProcedureGrantsFromAllRoles();

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
                    $procedureQuery = BuildQueryHelper::procedureSql($procedure, $procedureParams);
                    break;
                case 'JAVA':
                    $procedureQuery = BuildQueryHelper::procedureJava($procedure, $procedureParams);
                    break;
                case 'JAVASCRIPT':
                    $procedureQuery = BuildQueryHelper::procedureJavaScript($procedure, $procedureParams);
                    break;
                case 'PYTHON':
                    $procedureQuery = BuildQueryHelper::procedurePython($procedure, $procedureParams);
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
                fn($v) => str_contains($v->getGrantedBy(), $procedure['schema_name'])
            );
            if (!$this->assert(
                count($ownership) > 0,
                sprintf('Ownership grant for %s view not found.', $procedure['name'])
            ) || !$ownership) {
                continue;
            }
            $ownershipRole = $projectRoles->getRole(current($ownership)->getGrantedBy());
            $warehouseGrants = $ownershipRole->getAssignedGrants()->getWarehouseGrants();
            if (!$this->assert(
                count($warehouseGrants) > 0,
                sprintf('Warehouse grant for %s view not found.', $procedure['name'])
            ) || !$warehouseGrants) {
                continue;
            }

            $this->destinationConnection->useWarehouse(current($warehouseGrants)->getName());
            $this->destinationConnection->useRole(current($ownership)->getGrantedBy());

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
            if ($proceduresGrant->getPrivilege() === 'OWNERSHIP') {
                continue;
            }
            $this->destinationConnection->assignGrantToRole($proceduresGrant);
        }
    }

    private function assert(bool $assert, string $message): bool
    {
        if (!$this->config->skipCheck()) {
            assert($assert, $message);
        } elseif (!$assert) {
            $this->logger->info($message);
        }
        return $assert;
    }
}
