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
use ProjectMigrationTool\ValueObject\RoleGrants;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;
use Throwable;

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
        'default_warehouse',
        'default_namespace',
        'default_role',
        'owner',
    ];

    private const ROLE_FIELDS = ['name', 'owner'];

    private const SOURCE = 'source';

    private const TARGET = 'target';

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

    /** @var array<string, int> */
    private array $excludedMigrationUserGrants = [];

    /** @var array<string, int> */
    private array $excludedTemporaryDatabaseGrants = [];

    /** @var string[] */
    private array $temporaryDatabases = [];

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
        $this->excludedMigrationUserGrants = [self::SOURCE => 0, self::TARGET => 0];
        $this->excludedTemporaryDatabaseGrants = [self::SOURCE => 0, self::TARGET => 0];
        $this->temporaryDatabases = [];
        foreach ($this->databases as $database) {
            $this->temporaryDatabases[] = $database . '_SHARE';
            $this->temporaryDatabases[] = $database . '_OLD';
        }

        $this->useMigrationRoles();
        $this->sourceRoles = $this->fetchRowsByName($this->sourceConnection, 'SHOW ROLES');
        $this->targetRoles = $this->fetchRowsByName($this->destinationConnection, 'SHOW ROLES');

        foreach ($this->databases as $database) {
            $this->logger->info(sprintf('Checking database %s', $database));
            $this->useMigrationRoles();
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            $this->usersToCheck[$databaseRole] = true;

            $this->checkRoleWithChildRoles($databaseRole);
            if ($this->existsOnTarget($database)) {
                $this->checkDatabaseObjects($database, $databaseRole);
            }
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

        $sourceGrants = $this->fetchGrantsToRole($this->sourceConnection, $this->sourceRoles, $role);
        $targetGrants = $this->fetchGrantsToRole($this->destinationConnection, $this->targetRoles, $role);
        $this->comparison->compareItems(
            StructureComparison::GROUP_GRANTS_TO_ROLES,
            array_map(
                fn(array $grant) => $this->withoutAccountName($this->asMigratedGrant($grant)),
                $this->withoutTemporaryDatabaseGrants($sourceGrants, self::SOURCE),
            ),
            array_map(
                fn(array $grant) => $this->withoutAccountName($grant),
                $this->withoutTemporaryDatabaseGrants($targetGrants, self::TARGET),
            ),
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

        $sourceGrantsOfRole = $this->withoutGrantsToUser(
            $this->fetchForRole($this->sourceConnection, $this->sourceRoles, 'SHOW GRANTS OF ROLE', $role),
            $this->config->getSourceSnowflakeUser(),
            self::SOURCE,
        );
        $targetGrantsOfRole = $this->withoutGrantsToUser(
            $this->fetchForRole($this->destinationConnection, $this->targetRoles, 'SHOW GRANTS OF ROLE', $role),
            $this->config->getTargetSnowflakeUser(),
            self::TARGET,
        );
        $this->comparison->compareItems(
            StructureComparison::GROUP_GRANTS_OF_ROLES,
            array_map(fn(array $grant) => $this->withMigratedGrantedBy($grant), $sourceGrantsOfRole),
            $targetGrantsOfRole,
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

    /**
     * The migration grants every role it creates to its own user (target) and the database role to its own user
     * to copy views (source). These grants exist on one account only, so they are not compared.
     *
     * @param array<int, Row> $grantsOfRole
     * @return array<int, Row>
     */
    private function withoutGrantsToUser(array $grantsOfRole, string $user, string $account): array
    {
        $filtered = array_values(array_filter(
            $grantsOfRole,
            fn(array $grant) => !($grant['granted_to'] === 'USER' && $grant['grantee_name'] === $user),
        ));
        $this->excludedMigrationUserGrants[$account] += count($grantsOfRole) - count($filtered);
        return $filtered;
    }

    /**
     * The migration reads the "<DB>_SHARE" database and keeps the previous target database as "<DB>_OLD".
     * Grants on them exist until the cleanup, so they are not compared.
     *
     * @param array<int, Row> $grants
     * @return array<int, Row>
     */
    private function withoutTemporaryDatabaseGrants(array $grants, string $account): array
    {
        $filtered = array_values(array_filter(
            $grants,
            fn(array $grant) => !in_array($this->getGrantDatabaseName($grant), $this->temporaryDatabases, true),
        ));
        $this->excludedTemporaryDatabaseGrants[$account] += count($grants) - count($filtered);
        return $filtered;
    }

    /**
     * @param Row $grant
     */
    private function getGrantDatabaseName(array $grant): ?string
    {
        $name = (string) $grant['name'];
        if (preg_match('/^"([^"]*)"(.*)$/', $name, $matches)) {
            [, $database, $rest] = $matches;
        } else {
            $parts = explode('.', $name, 2);
            $database = $parts[0];
            $rest = isset($parts[1]) ? '.' . $parts[1] : '';
        }

        if ($grant['granted_on'] === 'DATABASE') {
            return $rest === '' ? $database : null;
        }
        return str_starts_with($rest, '.') ? $database : null;
    }

    /**
     * The target value the migration creates from a source grant.
     *
     * @param Row $grant
     * @return Row
     */
    private function asMigratedGrant(array $grant): array
    {
        if ($grant['granted_on'] === 'WAREHOUSE') {
            $grant['name'] = RoleGrants::mapWarehouseName(
                (string) $grant['name'],
                $this->config->getSourceSnowflakeWarehouse(),
                $this->config->getTargetSnowflakeWarehouse(),
            );
        }
        return $this->withMigratedGrantedBy($grant);
    }

    /**
     * The migration grants as the "granted_by" role, but Connection::useRole() switches to the configured role
     * instead of ACCOUNTADMIN.
     *
     * @param Row $grant
     * @return Row
     */
    private function withMigratedGrantedBy(array $grant): array
    {
        if ($grant['granted_by'] === 'ACCOUNTADMIN') {
            $grant['granted_by'] = $this->config->getTargetSnowflakeRole();
        }
        return $grant;
    }

    private function existsOnTarget(string $database): bool
    {
        /** @var array<int, Row> $databases */
        $databases = $this->destinationConnection->fetchAll(sprintf(
            'SHOW DATABASES LIKE %s',
            QueryBuilder::quote($database),
        ));
        // LIKE ignores case and treats "_" as a wildcard.
        $databases = array_values(array_filter($databases, fn(array $row) => $row['name'] === $database));
        $this->comparison->compareItems(
            StructureComparison::GROUP_DATABASES,
            [['name' => $database]],
            $databases,
            fn(array $row) => (string) $row['name'],
            ['name'],
        );
        return $databases !== [];
    }

    /**
     * SHOW lists only the objects the current role can see, so both accounts are listed with the database role.
     */
    private function checkDatabaseObjects(string $database, string $databaseRole): void
    {
        if (!isset($this->sourceRoles[$databaseRole], $this->targetRoles[$databaseRole])) {
            $this->logger->info(sprintf(
                'Schemas, tables and views of database "%s" are not checked, '
                . 'because the role "%s" does not exist on both accounts.',
                $database,
                $databaseRole,
            ));
            return;
        }

        /** @var array<int, array{Snowflake\Connection, string}> $grantedRoles */
        $grantedRoles = [];
        try {
            foreach ([
                self::SOURCE => [$this->sourceConnection, $this->config->getSourceSnowflakeUser()],
                self::TARGET => [$this->destinationConnection, $this->config->getTargetSnowflakeUser()],
            ] as $account => [$connection, $user]) {
                if ($this->grantRoleToUserIfMissing($connection, $account, $user, $databaseRole)) {
                    $grantedRoles[] = [$connection, $user];
                }
                $connection->useRole($databaseRole);
            }
            $this->compareDatabaseObjects($database);
        } finally {
            $this->useMigrationRoles();
            foreach ($grantedRoles as [$connection, $user]) {
                $connection->query(sprintf(
                    'REVOKE ROLE %s FROM USER %s',
                    Helper::quoteIdentifier($databaseRole),
                    Helper::quoteIdentifier($user),
                ));
            }
        }
    }

    /**
     * After "runCleanup" the target user has only the configured role, see Cleanup::postMigration().
     */
    private function grantRoleToUserIfMissing(
        Snowflake\Connection $connection,
        string $account,
        string $user,
        string $role,
    ): bool {
        $userRoles = array_column(
            $connection->fetchAll(sprintf('SHOW GRANTS TO USER %s', Helper::quoteIdentifier($user))),
            'role',
        );
        if (in_array($role, $userRoles, true)) {
            return false;
        }

        try {
            $connection->query(sprintf(
                'GRANT ROLE %s TO USER %s',
                Helper::quoteIdentifier($role),
                Helper::quoteIdentifier($user),
            ));
        } catch (Throwable $e) {
            throw new UserException(
                sprintf(
                    'The migration user "%s" does not have the role "%s" on the %s account '
                    . "and the check cannot grant it: %s\nRun the check before \"runCleanup\", "
                    . 'or grant the role to the user.',
                    $user,
                    $role,
                    $account,
                    $e->getMessage(),
                ),
                0,
                $e,
            );
        }
        return true;
    }

    private function compareDatabaseObjects(string $database): void
    {
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
     * The migration skips Snowflake internal USER$ grants, see Helper::filterUserDollarGrants().
     *
     * @param array<string, Row> $existingRoles
     * @return array<int, Row>
     */
    private function fetchGrantsToRole(Snowflake\Connection $connection, array $existingRoles, string $role): array
    {
        /** @var array<int, Row> $grants */
        $grants = array_values(Helper::filterUserDollarGrants(
            $this->fetchForRole($connection, $existingRoles, 'SHOW GRANTS TO ROLE', $role),
        ));
        return $grants;
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
        foreach ([
            self::SOURCE => $this->config->getSourceSnowflakeUser(),
            self::TARGET => $this->config->getTargetSnowflakeUser(),
        ] as $account => $user) {
            $this->logger->info(sprintf(
                'Excluded %d grants of roles to the migration user "%s" on the %s account.',
                $this->excludedMigrationUserGrants[$account],
                $user,
                $account,
            ));
            $this->logger->info(sprintf(
                'Excluded %d grants on the share and old databases on the %s account.',
                $this->excludedTemporaryDatabaseGrants[$account],
                $account,
            ));
        }

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
