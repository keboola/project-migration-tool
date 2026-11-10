<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\CannotAccessObjectException;
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

    public const SYNC_CLONE = 'clone';
    public const SYNC_COPY = 'copy';
    public const SYNC_SKIP = 'skip';

    private const SKIPPED_TABLES_LOG_LIMIT = 50;

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
        /** @var list<string> $skippedTables */
        $skippedTables = [];
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
                $tablesInOld = $this->config->isSynchronizeRun()
                    ? $this->getTablesInOldSchema($mainRole->getName(), $oldDbName, $schemaName)
                    : [];

                foreach ($tablesInShare as $tableInShare) {
                    $tableName = $tableInShare['name'];
                    $fullTableName = sprintf('%s.%s.%s', $database, $schemaName, $tableName);
                    $table = array_filter($tables, fn($v) => $v['name'] === $tableName);
                    if (count($table) === 0) {
                        continue;
                    }

                    $tableGrants = $projectRoles->getTableGrants($database, $schemaName, $tableName);
                    $ownershipOnTable = array_filter(
                        $tableGrants,
                        fn(GrantToRole $v) => $v->getPrivilege() === 'OWNERSHIP'
                    );

                    $problem = $this->findTableProblem($fullTableName, count($table), $ownershipOnTable, $projectRoles);
                    if ($problem !== null) {
                        if (!$this->config->skipCheck()) {
                            throw new UserException($problem);
                        }
                        $this->logger->warning(sprintf('Skipping table "%s": %s', $fullTableName, $problem));
                        $skippedTables[] = $fullTableName;
                        continue;
                    }
                    $table = current($table);
                    $ownershipOnTable = array_values($ownershipOnTable)[0];

                    $warehouseGrants = $projectRoles
                        ->getRole($ownershipOnTable->getGrantedBy())
                        ->getAssignedGrants()
                        ->getWarehouseGrants();

                    try {
                        $this->destinationConnection->useWarehouse($this->getWarehouseName($warehouseGrants));
                    } catch (NoWarehouseException $exception) {
                        if (!preg_match('/^(KEBOOLA|SAPI|sapi)_WORKSPACE_/', $ownershipOnTable->getGrantedBy())) {
                            throw $exception;
                        }
                        $this->logger->info(sprintf(
                            'Warning: Skipping table: %s, because: %s',
                            $tableName,
                            $exception->getMessage()
                        ));
                    }

                    $shareRows = self::toRowCount($tableInShare['rows'] ?? null);
                    $targetRows = self::toRowCount($table['rows'] ?? null);

                    $migrated = false;
                    if ($this->config->isSynchronizeRun()) {
                        $sameTimestampAsTarget = $this->compareTableMaxTimestamp(
                            $ownershipOnTable->getGrantedBy(),
                            $this->mainMigrationRoleTargetAccount,
                            $database,
                            $shareDbName,
                            $schemaName,
                            $tableName
                        );

                        $tableInOld = $tablesInOld[$tableName] ?? null;
                        $sameTimestampAsOld = $tableInOld !== null && $this->compareTableMaxTimestamp(
                            $mainRole->getName(),
                            $this->mainMigrationRoleTargetAccount,
                            $oldDbName,
                            $shareDbName,
                            $schemaName,
                            $tableName
                        );

                        $decision = self::decideSynchronizeAction(
                            $sameTimestampAsOld,
                            self::toRowCount($tableInOld['rows'] ?? null),
                            $sameTimestampAsTarget,
                            $targetRows,
                            $shareRows,
                        );
                        $this->logger->info(sprintf(
                            'Table "%s": %s, %s.',
                            $fullTableName,
                            $decision['action'],
                            $decision['reason'],
                        ));

                        if ($decision['action'] === self::SYNC_CLONE) {
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
                        } elseif ($decision['action'] === self::SYNC_COPY) {
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

                    // An unknown share row count still copies: an INSERT from an empty table changes nothing.
                    if ($targetRows === 0 && $shareRows !== 0) {
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

        $this->logSkippedTables($skippedTables);
    }

    /**
     * A row count of null means it is unknown. Then only the max timestamp is compared.
     *
     * @return array{action: self::SYNC_*, reason: string}
     */
    public static function decideSynchronizeAction(
        bool $sameTimestampAsOld,
        ?int $oldRows,
        bool $sameTimestampAsTarget,
        ?int $targetRows,
        ?int $shareRows,
    ): array {
        if ($sameTimestampAsOld) {
            if ($oldRows === null || $shareRows === null) {
                return [
                    'action' => self::SYNC_CLONE,
                    'reason' => 'same max timestamp in OLD database and share, row count is unknown',
                ];
            }
            if ($oldRows === $shareRows) {
                return [
                    'action' => self::SYNC_CLONE,
                    'reason' => 'same max timestamp and row count in OLD database and share',
                ];
            }
        }

        if (!$sameTimestampAsTarget) {
            return [
                'action' => self::SYNC_COPY,
                'reason' => 'max timestamp in target and share differs or cannot be read',
            ];
        }
        if ($targetRows === null || $shareRows === null) {
            return [
                'action' => self::SYNC_SKIP,
                'reason' => 'same max timestamp in target and share, row count is unknown',
            ];
        }
        if ($targetRows !== $shareRows) {
            return [
                'action' => self::SYNC_COPY,
                'reason' => sprintf(
                    'same max timestamp but different row count in target (%d) and share (%d)',
                    $targetRows,
                    $shareRows,
                ),
            ];
        }
        return [
            'action' => self::SYNC_SKIP,
            'reason' => 'same max timestamp and row count in target and share',
        ];
    }

    private static function toRowCount(mixed $rows): ?int
    {
        if (is_int($rows)) {
            return $rows;
        }
        if (is_string($rows) && is_numeric($rows)) {
            return (int) $rows;
        }
        return null;
    }

    /**
     * @param array<GrantToRole> $ownershipGrants
     */
    private function findTableProblem(
        string $fullTableName,
        int $tableCount,
        array $ownershipGrants,
        ProjectRoles $projectRoles,
    ): ?string {
        if ($tableCount > 1) {
            return sprintf('Table "%s" is listed %d times in the target schema.', $fullTableName, $tableCount);
        }
        if (count($ownershipGrants) === 0) {
            return sprintf('Table "%s" has no OWNERSHIP grant.', $fullTableName);
        }
        if (count($ownershipGrants) > 1) {
            return sprintf(
                'Table "%s" has %d OWNERSHIP grants, expected one.',
                $fullTableName,
                count($ownershipGrants),
            );
        }

        $ownerRoleName = array_values($ownershipGrants)[0]->getGrantedBy();
        $ownerRole = $projectRoles->getRoles()[$ownerRoleName] ?? null;
        if ($ownerRole === null) {
            return sprintf('Owner role "%s" of table "%s" is not a project role.', $ownerRoleName, $fullTableName);
        }
        if (count($ownerRole->getAssignedGrants()->getWarehouseGrants()) === 0) {
            return sprintf('Owner role "%s" of table "%s" has no warehouse grant.', $ownerRoleName, $fullTableName);
        }
        return null;
    }

    /**
     * Returns no tables when the OLD database or schema does not exist.
     *
     * @return array<string, array<string, string|null>>
     */
    private function getTablesInOldSchema(string $mainRole, string $oldDatabase, string $schema): array
    {
        $currentRole = $this->destinationConnection->getCurrentRole();
        try {
            $this->destinationConnection->useRole($mainRole);
            $tables = $this->destinationConnection->fetchAll(sprintf(
                'SHOW TABLES IN SCHEMA %s.%s;',
                Helper::quoteIdentifier($oldDatabase),
                Helper::quoteIdentifier($schema),
            ));
        } catch (RuntimeException | CannotAccessObjectException $e) {
            $this->logger->info(sprintf(
                'Schema "%s.%s" is not available, no table will be cloned from it.',
                $oldDatabase,
                $schema,
            ));
            return [];
        } finally {
            $this->destinationConnection->useRole($currentRole);
        }

        $tablesByName = [];
        foreach ($tables as $table) {
            $tablesByName[(string) $table['name']] = $table;
        }
        return $tablesByName;
    }

    /**
     * @param list<string> $skippedTables
     */
    private function logSkippedTables(array $skippedTables): void
    {
        if ($skippedTables === []) {
            return;
        }
        $message = sprintf(
            'Skipped %d table(s) because of failed checks: %s',
            count($skippedTables),
            implode(', ', array_slice($skippedTables, 0, self::SKIPPED_TABLES_LOG_LIMIT)),
        );
        if (count($skippedTables) > self::SKIPPED_TABLES_LOG_LIMIT) {
            $message .= sprintf(' and %d more', count($skippedTables) - self::SKIPPED_TABLES_LOG_LIMIT);
        }
        $this->logger->warning($message);
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
