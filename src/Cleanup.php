<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\ValueObject\FutureGrantToRole;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\GrantToUser;
use Psr\Log\LoggerInterface;

class Cleanup
{
    public function __construct(
        readonly Config $config,
        readonly Connection $sourceConnection,
        readonly Connection $destinationConnection,
        readonly LoggerInterface $logger,
    ) {
    }

    public function sourceAccount(): void
    {
        foreach ($this->config->getDatabases() as $database) {
            $sqls = [];
            $this->sourceConnection->useRole($this->config->getSourceSnowflakeRole());
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);
            $projectUser = $this->getProjectUser($databaseRole);

            $data = $this->getDataToRemove($this->sourceConnection, $databaseRole);

            // drop roles
            $sqls[] = sprintf('DROP ROLE %s;', Helper::quoteIdentifier($databaseRole));

            $roles = array_map(fn(array $v) => GrantToRole::fromArray($v), $data['ROLE'] ?? []);
            foreach ($roles as $role) {
                $futureGrants = array_map(
                    fn(array $v) => FutureGrantToRole::fromArray($v),
                    $this->sourceConnection->fetchAll(sprintf(
                        'SHOW FUTURE GRANTS TO ROLE %s',
                        Helper::quoteIdentifier($role->getName())
                    ))
                );

                foreach ($futureGrants as $futureGrant) {
                    $sqls[] = sprintf(
                        'REVOKE %s ON FUTURE TABLES IN SCHEMA %s FROM ROLE %s;',
                        $futureGrant->getPrivilege(),
                        $futureGrant->getName(),
                        Helper::quoteIdentifier($futureGrant->getGranteeName()),
                    );
                }

                $sqls[] = sprintf(
                    'DROP ROLE %s;',
                    Helper::quoteIdentifier($role->getName())
                );
            }

            // drop users
            $sqls[] = sprintf('DROP USER %s;', Helper::quoteIdentifier($projectUser->getGranteeName()));

            $users = array_map(fn(array $v) => GrantToRole::fromArray($v), $data['USER'] ?? []);
            foreach ($users as $user) {
                $sqls[] = sprintf(
                    'DROP USER %s;',
                    Helper::quoteIdentifier($user->getName())
                );
            }

            $sqls[] = sprintf(
                'DROP DATABASE %s;',
                Helper::quoteIdentifier($database)
            );

            foreach ($sqls as $sql) {
                $this->logger->info($sql);
            }
        }
    }

    public function preMigration(string $mainRoleName): void
    {
        $sqls = [];
        $currentRole = $this->config->getTargetSnowflakeRole();
        $this->destinationConnection->grantRoleToUser($this->config->getTargetSnowflakeUser(), $mainRoleName);
        foreach ($this->config->getDatabases() as $database) {
            $this->destinationConnection->useRole($this->config->getTargetSnowflakeRole());

            $dbExists = $this->destinationConnection->fetchAll(sprintf(
                'SHOW DATABASES LIKE %s;',
                QueryBuilder::quote($database)
            ));
            if (!$dbExists) {
                continue;
            }
            $databaseRole = $this->destinationConnection->getOwnershipRoleOnDatabase($database);
            $data = $this->getDataToRemove($this->destinationConnection, $databaseRole);

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

                /** @var FutureGrantToRole[] $futureGrants */
                $futureGrants = array_map(
                    fn(array $v) => FutureGrantToRole::fromArray($v),
                    $this->destinationConnection->fetchAll(sprintf(
                        'SHOW FUTURE GRANTS TO ROLE %s',
                        $role['name']
                    ))
                );
                foreach ($futureGrants as $futureGrant) {
                    $sqls[] = sprintf(
                        'REVOKE %s ON FUTURE TABLES IN SCHEMA %s FROM ROLE %s;',
                        $futureGrant->getPrivilege(),
                        $futureGrant->getName(),
                        Helper::quoteIdentifier($futureGrant->getGranteeName()),
                    );
                }
                $sqls[] = sprintf('DROP ROLE IF EXISTS %s;', Helper::quoteIdentifier($role['name']));
            }

            $projectUser = $this->getProjectUser($databaseRole);

            if ($projectUser->getGrantedBy() !== $currentRole) {
                $currentRole = $projectUser->getGrantedBy();
                $sqls[] = sprintf(
                    'USE ROLE %s;',
                    Helper::quoteIdentifier($this->config->getTargetSnowflakeRole())
                );
                $sqls[] = sprintf(
                    'GRANT ROLE %s TO USER %s;',
                    Helper::quoteIdentifier($currentRole),
                    Helper::quoteIdentifier($this->config->getTargetSnowflakeUser())
                );
                $sqls[] = sprintf('USE ROLE %s;', Helper::quoteIdentifier($currentRole));
            }
            $sqls[] = sprintf(
                'DROP USER IF EXISTS %s;',
                Helper::quoteIdentifier($projectUser->getGranteeName())
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

        $this->destinationConnection->useRole($mainRoleName);
        foreach ($sqls as $sql) {
            if ($this->config->getSynchronizeDryPremigrationCleanupRun()) {
                $this->logger->info($sql);
            } else {
                $this->destinationConnection->query($sql);
            }
        }
        if ($this->config->getSynchronizeDryPremigrationCleanupRun() && $sqls) {
            throw new UserException('!!! PLEASE RUN SQLS ON TARGET SNOWFLAKE ACCOUNT !!!');
        }

        $this->destinationConnection->query(sprintf(
            'USE ROLE %s',
            Helper::quoteIdentifier($this->config->getTargetSnowflakeRole())
        ));
    }

    public function postMigration(): void
    {
        foreach ($this->config->getDatabases() as $database) {
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
        }

        /** @var GrantToUser[] $userRoles */
        $userRoles = array_map(
            fn(array $v) => GrantToUser::fromArray($v),
            $this->destinationConnection->fetchAll(sprintf(
                'SHOW GRANTS TO USER %s',
                Helper::quoteIdentifier($this->config->getTargetSnowflakeUser())
            ))
        );

        foreach (array_reverse($userRoles) as $userRole) {
            if ($userRole->getRole() === $this->config->getTargetSnowflakeRole()) {
                continue;
            }
            try {
                if ($userRole->getGrantedBy()) {
                    $this->destinationConnection->useRole($userRole->getGrantedBy());
                } else {
                    $this->destinationConnection->useRole($this->config->getTargetSnowflakeRole());
                }
                $this->destinationConnection->query(sprintf(
                    'REVOKE ROLE %s FROM USER %s',
                    Helper::quoteIdentifier($userRole->getRole()),
                    Helper::quoteIdentifier($userRole->getGranteeName()),
                ));
            } catch (RuntimeException $e) {
                $this->logger->info(sprintf(
                    'Warning: Query failed, please check manually: %s',
                    $e->getMessage()
                ));
            }
        }
    }

    private function getDataToRemove(Connection $connection, string $role): array
    {
        $grants = $connection->fetchAll(sprintf(
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
                    $this->getDataToRemove($connection, $roleGrant['name']),
                    $mapGrants,
                );
            }
        }

        return $mapGrants;
    }

    private function getProjectUser(string $databaseRole): GrantToUser
    {
        $grantsOfRole = $this->destinationConnection->fetchAll(sprintf('SHOW GRANTS OF ROLE %s', $databaseRole));
        $filteredGrantsOfRole = array_filter(
            $grantsOfRole,
            fn($v) => strtoupper($v['grantee_name']) === strtoupper($databaseRole)
        );
        assert(count($filteredGrantsOfRole) === 1);
        $grantOfRole = current($filteredGrantsOfRole);

        return GrantToUser::fromArray($grantOfRole);
    }
}
