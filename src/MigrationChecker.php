<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\MigrationCheck\StructureComparison;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\Snowflake\ShowCommandPaginator;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\Role;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;

/**
 * @phpstan-import-type Row from StructureComparison
 */
class MigrationChecker
{
    private const SKIP_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];

    private const MAX_LISTED_DIFFERENCES = 50;

    private const TABLE_FIELDS = ['name', 'schema_name', 'kind', 'rows', 'owner'];

    private const VIEW_FIELDS = ['name', 'schema_name', 'owner'];

    private const USER_FIELDS = [
        'name',
        'login_name',
        'display_name',
        'first_name',
        'last_name',
        'email',
        'default_warehouse',
        'default_namespace',
        'default_role',
        'owner',
    ];

    private const ROLE_FIELDS = ['name', 'comment', 'owner'];

    private const GRANT_OF_ROLE_FIELDS = ['role', 'granted_to', 'grantee_name', 'granted_by'];

    private const GRANT_TO_ROLE_FIELDS = [
        'privilege',
        'granted_on',
        'name',
        'granted_to',
        'grantee_name',
        'grant_option',
        'granted_by',
    ];

    private StructureComparison $comparison;

    /** @var array<string, Row> */
    private array $sourceRoles = [];

    /** @var array<string, Row> */
    private array $targetRoles = [];

    /** @var array<string, true> */
    private array $checkedRoles = [];

    /** @var array<string, true> */
    private array $usersToCheck = [];

    public function __construct(
        readonly Snowflake\Connection $sourceConnection,
        readonly Snowflake\Connection $destinationConnection,
        readonly Configuration\Config $config,
        readonly LoggerInterface $logger,
        readonly array $databases,
    ) {
    }

    public function postMigrationCheckStructure(): void
    {
        $this->comparison = new StructureComparison($this->logger);
        $this->checkedRoles = [];
        $this->usersToCheck = [];

        $this->useMigrationRoles();
        $this->sourceRoles = $this->fetchRowsByName($this->sourceConnection, 'SHOW ROLES');
        $this->targetRoles = $this->fetchRowsByName($this->destinationConnection, 'SHOW ROLES');

        foreach ($this->databases as $database) {
            $this->logger->info(sprintf('Checking database %s', $database));
            $this->useMigrationRoles();
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            $this->usersToCheck[$databaseRole] = true;

            $this->checkRoleWithChildRoles($databaseRole);
            $this->checkDatabaseObjects($database, $databaseRole);
        }

        $this->useMigrationRoles();
        $this->comparison->compareItems(
            StructureComparison::GROUP_ROLES,
            array_values(array_intersect_key($this->sourceRoles, $this->checkedRoles)),
            array_values(array_intersect_key($this->targetRoles, $this->checkedRoles)),
            fn(array $role) => (string) $role['name'],
            self::ROLE_FIELDS,
        );
        $this->comparison->compareItems(
            StructureComparison::GROUP_USERS,
            array_values(array_intersect_key(
                $this->fetchRowsByName($this->sourceConnection, 'SHOW USERS'),
                $this->usersToCheck,
            )),
            array_values(array_intersect_key(
                $this->fetchRowsByName($this->destinationConnection, 'SHOW USERS'),
                $this->usersToCheck,
            )),
            fn(array $user) => (string) $user['name'],
            self::USER_FIELDS,
        );

        $this->reportCheckResult();
    }

    public function postMigrationCheckData(Role $mainRoleWithGrants): void
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

        /** @var GrantToRole[] $warehousesGrant */
        $warehousesGrant = array_values(array_filter(
            $mainRoleWithGrants->getAssignedGrants()->getWarehouseGrants(),
            fn($v) => $v->getPrivilege() === 'USAGE'
        ));
        assert(count($warehousesGrant) > 0);
        $this->sourceConnection->grantRoleToUser(
            $this->config->getSourceSnowflakeUser(),
            $mainRoleWithGrants->getName()
        );
        $this->destinationConnection->grantRoleToUser(
            $this->config->getTargetSnowflakeUser(),
            $mainRoleWithGrants->getName()
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
                $this->sourceConnection->useRole($mainRoleWithGrants->getName());
                $this->destinationConnection->useRole($mainRoleWithGrants->getName());
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
                        $this->logger->info(sprintf(
                            'Warning: Table %s.%s.%s has no primary key. Skipping',
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
                        'warehouse' => $warehousesGrant[0]->getName(),
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
                        $this->logger->info(sprintf(
                            'Warning: Checking table "%s.%s.%s" ends with error: "%s"',
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

    private function useMigrationRoles(): void
    {
        $this->sourceConnection->useRole($this->config->getSourceSnowflakeRole());
        $this->destinationConnection->useRole($this->config->getTargetSnowflakeRole());
    }

    /**
     * Roles and users are discovered on both accounts, so an object missing on one side is still checked.
     */
    private function checkRoleWithChildRoles(string $role): void
    {
        if (isset($this->checkedRoles[$role])) {
            return;
        }
        $this->checkedRoles[$role] = true;

        $sourceGrants = $this->fetchForRole($this->sourceConnection, $this->sourceRoles, 'SHOW GRANTS TO ROLE', $role);
        $targetGrants = $this->fetchForRole(
            $this->destinationConnection,
            $this->targetRoles,
            'SHOW GRANTS TO ROLE',
            $role,
        );
        $this->comparison->compareItems(
            StructureComparison::GROUP_GRANTS_TO_ROLES,
            array_map(fn(array $grant) => $this->withoutAccountName($grant), $sourceGrants),
            array_map(fn(array $grant) => $this->withoutAccountName($grant), $targetGrants),
            fn(array $grant) => implode(' ', array_filter(
                [
                    $grant['privilege'],
                    'ON',
                    $grant['granted_on'],
                    $grant['name'],
                    'TO',
                    $grant['granted_to'],
                    $grant['grantee_name'],
                ],
                fn($value) => $value !== '',
            )),
            self::GRANT_TO_ROLE_FIELDS,
        );

        $this->comparison->compareItems(
            StructureComparison::GROUP_GRANTS_OF_ROLES,
            $this->fetchForRole($this->sourceConnection, $this->sourceRoles, 'SHOW GRANTS OF ROLE', $role),
            $this->fetchForRole($this->destinationConnection, $this->targetRoles, 'SHOW GRANTS OF ROLE', $role),
            fn(array $grant) => sprintf(
                '%s TO %s %s BY %s',
                $grant['role'],
                $grant['granted_to'],
                $grant['grantee_name'],
                $grant['granted_by'],
            ),
            self::GRANT_OF_ROLE_FIELDS,
        );

        foreach ([...$sourceGrants, ...$targetGrants] as $grant) {
            if ($grant['privilege'] !== 'OWNERSHIP') {
                continue;
            }
            if ($grant['granted_on'] === 'USER') {
                $this->usersToCheck[(string) $grant['name']] = true;
            }
            if ($grant['granted_on'] === 'ROLE') {
                $this->checkRoleWithChildRoles((string) $grant['name']);
            }
        }
    }

    private function checkDatabaseObjects(string $database, string $databaseRole): void
    {
        // The data migration lists the target tables with this role too.
        if (isset($this->targetRoles[$databaseRole])) {
            $this->destinationConnection->useRole($databaseRole);
        }

        $sourceSchemas = $this->fetchSchemas($this->sourceConnection, $database);
        $targetSchemas = $this->fetchSchemas($this->destinationConnection, $database);
        $this->comparison->compareItems(
            StructureComparison::GROUP_SCHEMAS,
            $sourceSchemas,
            $targetSchemas,
            fn(array $schema) => sprintf('%s.%s', $database, $schema['name']),
            ['name'],
        );

        $sourceSchemaNames = array_map('strval', array_column($sourceSchemas, 'name'));
        $targetSchemaNames = array_map('strval', array_column($targetSchemas, 'name'));
        $objectName = fn(array $object) => sprintf('%s.%s.%s', $database, $object['schema_name'], $object['name']);
        foreach (array_unique([...$sourceSchemaNames, ...$targetSchemaNames]) as $schema) {
            foreach ([
                StructureComparison::GROUP_TABLES => ['SHOW TABLES IN SCHEMA', self::TABLE_FIELDS],
                StructureComparison::GROUP_VIEWS => ['SHOW VIEWS IN SCHEMA', self::VIEW_FIELDS],
            ] as $group => [$showCommand, $fields]) {
                $showCommand = sprintf(
                    '%s %s.%s',
                    $showCommand,
                    Helper::quoteIdentifier($database),
                    Helper::quoteIdentifier($schema),
                );
                $this->comparison->compareItems(
                    $group,
                    in_array($schema, $sourceSchemaNames, true)
                        ? ShowCommandPaginator::fetchAll($this->sourceConnection, $showCommand)
                        : [],
                    in_array($schema, $targetSchemaNames, true)
                        ? ShowCommandPaginator::fetchAll($this->destinationConnection, $showCommand)
                        : [],
                    $objectName,
                    $fields,
                );
            }
        }
    }

    /**
     * @return array<int, Row>
     */
    private function fetchSchemas(Snowflake\Connection $connection, string $database): array
    {
        /** @var array<int, Row> $schemas */
        $schemas = $connection->fetchAll(sprintf('SHOW SCHEMAS IN DATABASE %s', Helper::quoteIdentifier($database)));
        return array_values(array_filter(
            $schemas,
            fn(array $schema) => !in_array($schema['name'], self::SKIP_SCHEMAS, true),
        ));
    }

    /**
     * @param array<string, Row> $existingRoles
     * @return array<int, Row>
     */
    private function fetchForRole(
        Snowflake\Connection $connection,
        array $existingRoles,
        string $showCommand,
        string $role,
    ): array {
        if (!isset($existingRoles[$role])) {
            return [];
        }
        /** @var array<int, Row> $rows */
        $rows = $connection->fetchAll(sprintf('%s %s', $showCommand, Helper::quoteIdentifier($role)));
        return $rows;
    }

    /**
     * @return array<string, Row>
     */
    private function fetchRowsByName(Snowflake\Connection $connection, string $showCommand): array
    {
        $rows = [];
        foreach (ShowCommandPaginator::fetchAll($connection, $showCommand) as $row) {
            $rows[(string) $row['name']] = $row;
        }
        return $rows;
    }

    /**
     * Account-level grants are named after the account, which differs between the source and the target.
     *
     * @param Row $grant
     * @return Row
     */
    private function withoutAccountName(array $grant): array
    {
        if ($grant['granted_on'] === 'ACCOUNT') {
            $grant['name'] = '';
        }
        return $grant;
    }

    private function reportCheckResult(): void
    {
        $differences = $this->comparison->getDifferences();
        if (!$differences) {
            $this->logger->info(sprintf(
                'Post-migration check passed. Compared items: %s.',
                $this->formatCounts($this->comparison->getComparedItemCounts()),
            ));
            return;
        }

        $summary = sprintf(
            'Post-migration check found %d differences (%s).',
            count($differences),
            $this->formatCounts($this->comparison->getDifferenceCounts()),
        );
        if ($this->config->isCheckReportOnly()) {
            $this->logger->warning($summary . ' The job does not fail because "checkReportOnly" is enabled.');
            return;
        }

        $lines = [$summary];
        if (count($differences) > self::MAX_LISTED_DIFFERENCES) {
            $lines[] = sprintf(
                'The first %d differences are listed below. The full list is in the job log.',
                self::MAX_LISTED_DIFFERENCES,
            );
        } else {
            $lines[] = 'The differences are listed below.';
        }
        foreach (array_slice($differences, 0, self::MAX_LISTED_DIFFERENCES) as $difference) {
            $lines[] = '- ' . $difference->toString();
        }
        throw new UserException(implode("\n", $lines));
    }

    /**
     * @param array<string, int> $counts
     */
    private function formatCounts(array $counts): string
    {
        return implode(', ', array_map(
            fn(string $group, int $count) => sprintf('%s: %d', $group, $count),
            array_keys($counts),
            $counts,
        ));
    }
}
