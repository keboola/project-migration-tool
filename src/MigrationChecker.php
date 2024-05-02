<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\Role;
use Psr\Log\LoggerInterface;
use Symfony\Component\Process\Process;

class MigrationChecker
{

    public function __construct(
        readonly Snowflake\Connection $sourceConnection,
        readonly Snowflake\Connection $destinationConnection,
        readonly Configuration\Config $config,
        readonly LoggerInterface $logger,
        readonly array $databases,
    ) {
    }

    public function postMigrationCheckStructure(Role $mainRoleWithGrants): void
    {
        $warehouses = $mainRoleWithGrants->getAssignedGrants()->getWarehouseGrants();

        assert(count($warehouses) > 0);
        $useWarehouse = sprintf('USE WAREHOUSE %s', Helper::quoteIdentifier(current($warehouses)->getName()));

        $this->sourceConnection->query($useWarehouse);
        $this->destinationConnection->query($useWarehouse);

        foreach ($this->databases as $database) {
            $this->logger->info(sprintf('Checking database %s', $database));
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $this->logger->info(sprintf('Getting roles and users for database %s', $database));
            $rolesAndUsers = $this->listRolesAndUsers($databaseRole);
            $rolesAndUsers = array_merge_recursive(
                $rolesAndUsers,
                ['users' => [$databaseRole], 'roles' => [$databaseRole]]
            );
            $compares = [];
            // phpcs:disable Generic.Files.LineLength
            // Compare USERS
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

            // Compare GRANTS TO USERS
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

            // Compare ROLES
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

            // Compare GRANTS TO ROLES
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
            // Compare TABLES
            $schemas = $this->sourceConnection->fetchAll(sprintf(
                'SHOW SCHEMAS IN DATABASE %s',
                Helper::quoteIdentifier($database)
            ));
            foreach ($schemas as $schema) {
                $tables = $this->sourceConnection->fetchAll(sprintf(
                    'SHOW TABLES IN SCHEMA %s.%s',
                    Helper::quoteIdentifier($database),
                    Helper::quoteIdentifier($schema['name'])
                ));
                foreach ($tables as $table) {
                    $compares[] = [
                        'group' => sprintf('Table: %s.%s.%s', $database, $schema['name'], $table['name']),
                        'itemNameKey' => 'ID',
                        'sql' => sprintf(
                            'SELECT \'%s.%s.%s\' AS ID, count(*) AS ROW_COUNT FROM %s.%s.%s',
                            Helper::quoteIdentifier($database),
                            Helper::quoteIdentifier($schema['name']),
                            Helper::quoteIdentifier($table['name']),
                            Helper::quoteIdentifier($database),
                            Helper::quoteIdentifier($schema['name']),
                            Helper::quoteIdentifier($table['name'])
                        ),
                        'role' => $databaseRole,
                    ];
                }
            }
            // phpcs:enable Generic.Files.LineLength

            foreach ($compares as $compare) {
                $this->compareData(
                    $compare['group'],
                    $compare['itemNameKey'],
                    $compare['sql'],
                    array_key_exists('role', $compare) ? $compare['role'] : null
                );
            }
        }
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

    private function compareData(string $group, string $itemNameKey, string $sql, ?string $role = null): void
    {
        if ($role) {
            $this->sourceConnection->useRole($role);
            $this->destinationConnection->useRole($role);
        }
        $this->logger->info(sprintf('Getting source data for "%s".', $group));
        $sourceData = $this->sourceConnection->fetchAll($sql);
        $this->logger->info(sprintf('Getting target data for "%s".', $group));
        $targetData = $this->destinationConnection->fetchAll($sql);

        if (count($sourceData) !== count($targetData)) {
            $this->logger->info(sprintf(
                'Alert - %s: Source data count (%s) does not equal target data count (%s)',
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
                    $this->logger->info(sprintf(
                        'Alert - %s: Item "%s" doesn\'t exists in source account',
                        $group,
                        $k
                    ));
                    continue;
                }
                if (!isset($targetData[$k])) {
                    $this->logger->info(sprintf(
                        'Alert - %s: Item "%s" doesn\'t exists in target account',
                        $group,
                        $k
                    ));
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

        $this->logger->info(sprintf(
            'Alert - %s: "%s" is not same. Missing in %s account (%s)',
            $group,
            $name,
            $missingIn,
            implode(';', $data)
        ));
    }

    private function listRolesAndUsers(string $role): array
    {
        $grants = array_map(
            fn(array $grant) => GrantToRole::fromArray($grant),
            $this->destinationConnection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s',
                Helper::quoteIdentifier($role)
            ))
        );

        $filteredGrants = array_filter(
            $grants,
            fn($v) => $v->getPrivilege() === 'OWNERSHIP' && (in_array($v->getGrantedOn(), ['USER', 'ROLE']))
        );

        $tmp = [
            'users' => [],
            'roles' => [],
        ];
        foreach ($filteredGrants as $filteredGrant) {
            switch ($filteredGrant->getGrantedOn()) {
                case 'USER':
                    $tmp['users'][] = $filteredGrant->getName();
                    break;
                case 'ROLE':
                    $tmp['roles'][] = $filteredGrant->getName();
                    $childRoles = $this->listRolesAndUsers($filteredGrant->getName());
                    $tmp = array_merge_recursive($tmp, $childRoles);
                    break;
            }
        }

        return $tmp;
    }
}
