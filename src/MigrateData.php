<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Exception\NoWarehouseException;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\ProjectRoles;
use ProjectMigrationTool\ValueObject\Role;
use Psr\Log\LoggerInterface;

class MigrateData
{
    private const SKIP_CLONE_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];

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
    public function migrate(Role $mainRole, array $roles): void
    {
        foreach ($this->databases as $database) {
            $this->logger->info(sprintf('Use database "%s".', $database));
            $this->sourceConnection->useRole($this->mainMigrationRoleSourceAccount);
            $databaseRoleName = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $projectRoles = $roles[$databaseRoleName];

            $this->destinationConnection->useRole($projectRoles->getRole($databaseRoleName)->getName());

            $shareDbName = $database . '_SHARE';
            $oldDbName = $database . '_OLD';

            $schemas = $this->destinationConnection->fetchAll(sprintf(
                'SHOW SCHEMAS IN DATABASE %s;',
                Helper::quoteIdentifier($shareDbName)
            ));

            foreach ($schemas as $schema) {
                if (in_array($schema['name'], self::SKIP_CLONE_SCHEMAS)) {
                    continue;
                }
                $schemaName = $schema['name'];

                // Skip dev branch schemas - they follow pattern: {branchId}_{bucketId}
                if ($this->config->skipDevBranches() && Helper::isDevBranchSchema($schemaName)) {
                    $this->logger->info(sprintf(
                        'Skipping dev branch schema "%s" - dev branch objects should not be migrated.',
                        $schemaName
                    ));
                    continue;
                }

                $this->logger->info(sprintf('Use schema "%s".', $schemaName));

                $tablesInShare = $this->destinationConnection->fetchAll(sprintf(
                    'SHOW TABLES IN SCHEMA %s.%s;',
                    Helper::quoteIdentifier($shareDbName),
                    Helper::quoteIdentifier($schemaName)
                ));
                $tables = $this->destinationConnection->fetchAll(sprintf(
                    'SHOW TABLES IN SCHEMA %s.%s;',
                    Helper::quoteIdentifier($database),
                    Helper::quoteIdentifier($schemaName)
                ));

                foreach ($tablesInShare as $tableInShare) {
                    $tableName = $tableInShare['name'];
                    $table = array_filter($tables, fn($v) => $v['name'] === $tableName);
                    if (count($table) === 0) {
                        continue;
                    }
                    assert(
                        count($table) === 1,
                        sprintf('Table "%s" is not unique in database "%s"', $tableName, $database)
                    );
                    $table = current($table);

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
                    assert(count($ownershipOnTable) === 1);
                    $ownershipOnTable = current($ownershipOnTable);

                    $warehouseGrants = $projectRoles
                        ->getRole($ownershipOnTable->getGrantedBy())
                        ->getAssignedGrants()
                        ->getWarehouseGrants();
                    assert(count($warehouseGrants) > 0);

                    try {
                        $this->destinationConnection->useWarehouse($this->getWarehouseName($warehouseGrants));
                    } catch (NoWarehouseException $exception) {
                        if (
                            !$this->config->skipDevBranches()
                            || !Helper::isWorkspaceRole($ownershipOnTable->getGrantedBy())
                        ) {
                            throw $exception;
                        }
                        $this->logger->info(sprintf(
                            'Warning: Skipping table: %s, because: %s',
                            $tableName,
                            $exception->getMessage()
                        ));
                    }

                    $migrated = false;
                    if ($this->config->isSynchronizeRun()) {
                        $compareProductionAndShareDatabase = $this->compareTableMaxTimestamp(
                            $ownershipOnTable->getGrantedBy(),
                            $this->mainMigrationRoleTargetAccount,
                            $database,
                            $shareDbName,
                            $schemaName,
                            $tableName
                        );

                        $compareOldAndShareDatabase = $this->compareTableMaxTimestamp(
                            $mainRole->getName(),
                            $this->mainMigrationRoleTargetAccount,
                            $oldDbName,
                            $shareDbName,
                            $schemaName,
                            $tableName
                        );

                        if ($compareOldAndShareDatabase) {
                            $this->grantUsageToOldTable(
                                $database,
                                $schemaName,
                                $tableName,
                                $mainRole->getName(),
                                $ownershipOnTable->getGrantedBy()
                            );
                            $this->logger->info(sprintf('Cloning table "%s" from OLD database', $tableName));
                            $this->destinationConnection->query(sprintf(
                                'DROP TABLE %s.%s.%s',
                                Helper::quoteIdentifier($database),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                            ));

                            $this->destinationConnection->query(sprintf(
                                'CREATE TABLE %s.%s.%s CLONE %s.%s.%s;',
                                Helper::quoteIdentifier($database),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                                Helper::quoteIdentifier($oldDbName),
                                Helper::quoteIdentifier($schemaName),
                                Helper::quoteIdentifier($tableName),
                            ));

                            $tableGrants = array_filter(
                                $tableGrants,
                                fn(GrantToRole $v) => $v->getPrivilege() !== 'OWNERSHIP'
                            );
                            foreach ($tableGrants as $tableGrant) {
                                $this->destinationConnection->assignGrantToRole($tableGrant);
                            }
                            $migrated = true;
                        } elseif (!$compareProductionAndShareDatabase) {
                            $this->logger->info(sprintf('Migrate data for table "%s".', $tableName));
                            $this->destinationConnection->query(sprintf(
                                'TRUNCATE TABLE %s.%s.%s;',
                                Helper::quoteIdentifier($database),
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
                            $migrated = true;
                        }
                    }

                    if ($migrated) {
                        continue;
                    }

                    if ($table['rows'] === '0' && $tableInShare !== '0') {
                        $this->logger->info(sprintf('Migrate data for table "%s".', $tableName));
                        $this->destinationConnection->query(sprintf(
                            'INSERT INTO %s.%s.%s SELECT * FROM %s.%s.%s;',
                            Helper::quoteIdentifier($database),
                            Helper::quoteIdentifier($schemaName),
                            Helper::quoteIdentifier($tableName),
                            Helper::quoteIdentifier($shareDbName),
                            Helper::quoteIdentifier($schemaName),
                            Helper::quoteIdentifier($tableName),
                        ));
                    }
                }
            }
        }
    }

    private function compareTableMaxTimestamp(
        string $firstDatabaseRole,
        string $secondDatabaseRole,
        string $firstDatabase,
        string $secondDatabase,
        string $schema,
        string $table
    ): bool {

        $sqlTemplate = 'SELECT max("_timestamp") as "maxTimestamp" FROM %s.%s.%s';

        $currentRole = $this->destinationConnection->getCurrentRole();
        try {
            $this->destinationConnection->useRole($firstDatabaseRole);

            $lastUpdateInFirstDatabase = $this->destinationConnection->fetchAll(sprintf(
                $sqlTemplate,
                Helper::quoteIdentifier($firstDatabase),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($table)
            ));

            $this->destinationConnection->useRole($secondDatabaseRole);
            $lastUpdateInSecondDatabase = $this->destinationConnection->fetchAll(sprintf(
                $sqlTemplate,
                Helper::quoteIdentifier($secondDatabase),
                Helper::quoteIdentifier($schema),
                Helper::quoteIdentifier($table)
            ));
        } catch (RuntimeException $e) {
            return false;
        } finally {
            $this->destinationConnection->useRole($currentRole);
        }

        return $lastUpdateInFirstDatabase[0]['maxTimestamp'] === $lastUpdateInSecondDatabase[0]['maxTimestamp'];
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

    private function getWarehouseName(array $warehouseGrants): string
    {
        $selectedWarehouse = array_filter(
            $warehouseGrants,
            fn(GrantToRole $warehouseGrant) => str_ends_with(
                $warehouseGrant->getName(),
                $this->config->getWarehouseSize(),
            ),
        );

        // if no warehouse is selected, return the first one
        if (count($selectedWarehouse) === 0) {
            return current($warehouseGrants)->getName();
        }

        return current($selectedWarehouse)->getName();
    }
}
