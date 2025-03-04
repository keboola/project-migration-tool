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
    private const OWNERSHIP = 'OWNERSHIP';
    private const USER = 'USER';
    private const ROLE = 'ROLE';

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

            $grantedOnDatabaseRole = $this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS ON ROLE %s',
                Helper::quoteIdentifier($databaseRole)
            ));
            assert(count($grantedOnDatabaseRole) === 1);
            $mainRoleName = current($grantedOnDatabaseRole)['granted_by'];

            $data = $this->getDataToRemoveForRole($this->sourceConnection, $databaseRole, $mainRoleName);

            // drop roles
            $sqls[] = sprintf('DROP ROLE %s;', Helper::quoteIdentifier($databaseRole));

            $roles = array_map(fn(array $v) => GrantToRole::fromArray($v), $data[self::ROLE] ?? []);
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

            $users = array_map(fn(array $v) => GrantToRole::fromArray($v), $data[self::USER] ?? []);
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
        $this->logger->info(sprintf('Starting pre-migration cleanup with main role: %s', $mainRoleName));
        $sqls = [];
        $currentRole = null;

        // Check if main role is assigned to target user
        $this->logger->info('Checking main role assignment and ownership');
        $grantsToTargetUser = $this->destinationConnection->fetchAll(sprintf(
            'SHOW GRANTS TO USER %s',
            QueryBuilder::quoteIdentifier($this->config->getTargetSnowflakeUser()),
        ));
        $mainRoleExistsOnTargetUser = array_reduce(
            $grantsToTargetUser,
            fn ($found, $v) => $found || $v['role'] === $mainRoleName,
            false,
        );

        // Check if target role has ownership of main role
        $mainRole = $this->destinationConnection->fetchAll(sprintf(
            'SHOW ROLES LIKE %s',
            QueryBuilder::quote($mainRoleName),
        ));
        $hasMainRoleOwnership = array_reduce(
            $mainRole,
            fn ($found, $v) => $found || $v['owner'] === $this->config->getTargetSnowflakeRole(),
            false,
        );

        if (!$mainRoleExistsOnTargetUser) {
            if (!$hasMainRoleOwnership) {
                throw new UserException(
                    'Main role exists but is not assigned to migrate user and cannot be granted.',
                );
            }
            $this->destinationConnection->grantRoleToUser($this->config->getTargetSnowflakeUser(), $mainRoleName);
        }

        // Process each database
        foreach ($this->config->getDatabases() as $database) {
            $this->logger->info(sprintf('Processing database: %s', $database));
            $this->destinationConnection->useRole($this->config->getTargetSnowflakeRole());

            // Check if database exists
            $dbExists = $this->destinationConnection->fetchAll(sprintf(
                'SHOW DATABASES LIKE %s;',
                QueryBuilder::quote($database)
            ));

            if (!$dbExists) {
                $this->logger->info(sprintf('Database %s does not exist, checking for role with same name', $database));
                // Check if role exists with exact or lowercase name
                $roleName = null;
                foreach ([$database, strtolower($database)] as $nameVariant) {
                    $roleExists = $this->destinationConnection->fetchAll(sprintf(
                        'SHOW ROLES LIKE %s',
                        QueryBuilder::quote($nameVariant)
                    ));
                    if ($roleExists) {
                        $roleName = $nameVariant;
                        break;
                    }
                }

                if ($roleName === null) {
                    continue;
                }

                $dataToRemove = $this->getDataToRemoveForRole($this->destinationConnection, $roleName, $mainRoleName);
            } else {
                $this->logger->info(sprintf('Database %s exists, getting ownership role', $database));
                $databaseRole = $this->destinationConnection->getOwnershipRoleOnDatabase($database);
                $dataToRemove = $this->getDataToRemoveForRole(
                    $this->destinationConnection,
                    $databaseRole,
                    $mainRoleName,
                );
            }

            // First revoke all future grants from roles
            foreach ($dataToRemove[self::ROLE] as $role) {
                $this->destinationConnection->useRole($role['granted_by']);
                $futureGrants = array_map(
                    fn(array $v) => FutureGrantToRole::fromArray($v),
                    $this->destinationConnection->fetchAll(sprintf(
                        'SHOW FUTURE GRANTS TO ROLE %s',
                        Helper::quoteIdentifier($role['name'])
                    ))
                );

                if ($futureGrants) {
                    [$sqls, $currentRole] = $this->switchRole($role['granted_by'], $mainRoleName, $sqls, $currentRole);
                }
                foreach ($futureGrants as $futureGrant) {
                    $sqls[] = sprintf(
                        'REVOKE %s ON FUTURE TABLES IN SCHEMA %s FROM ROLE %s;',
                        $futureGrant->getPrivilege(),
                        $futureGrant->getName(),
                        Helper::quoteIdentifier($futureGrant->getGranteeName())
                    );
                }
            }

            // Drop users owned by roles
            if (!empty($dataToRemove[self::USER])) {
                $this->logger->info(sprintf('Dropping %d users: ', count($dataToRemove[self::USER])));
                $this->logger->info(sprintf(implode(
                    ', ',
                    array_column($dataToRemove[self::USER], 'name')
                )));
            }
            foreach ($dataToRemove[self::USER] as $user) {
                [$sqls, $currentRole] = $this->switchRole($user['granted_by'], $mainRoleName, $sqls, $currentRole);
                $sqls[] = sprintf(
                    'DROP USER IF EXISTS %s;',
                    Helper::quoteIdentifier($user['name'])
                );
            }

            // Drop roles
            if (!empty($dataToRemove[self::ROLE])) {
                $this->logger->info(sprintf('Dropping %d roles: ', count($dataToRemove[self::ROLE])));
                $this->logger->info(sprintf(implode(
                    ', ',
                    array_column($dataToRemove[self::ROLE], 'name')
                )));
            }
            foreach ($dataToRemove[self::ROLE] as $role) {
                [$sqls, $currentRole] = $this->switchRole($role['granted_by'], $mainRoleName, $sqls, $currentRole);
                $sqls[] = sprintf(
                    'DROP ROLE IF EXISTS %s;',
                    Helper::quoteIdentifier($role['name'])
                );
            }

            // Drop database role and handle database if exists
            [$sqls, $currentRole] = $this->switchRole($mainRoleName, $mainRoleName, $sqls, $currentRole);

            if ($dbExists) {
                $sqls[] = sprintf(
                    'DROP DATABASE IF EXISTS %s;',
                    Helper::quoteIdentifier($database . '_OLD')
                );
                $sqls[] = sprintf(
                    'ALTER DATABASE IF EXISTS %s RENAME TO %s;',
                    Helper::quoteIdentifier($database),
                    Helper::quoteIdentifier($database . '_OLD')
                );
            }
        }

        // Execute all SQL commands
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

        $this->destinationConnection->useRole($this->config->getTargetSnowflakeRole(), true);
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

    private function getDataToRemoveForRole(Connection $connection, string $name, string $mainRoleName): array
    {
        if ($name === $mainRoleName) {
            throw new UserException('Cannot remove main role');
        }
        $this->logger->debug(sprintf('Getting data to remove for role: %s', $name));
        $result = [
            self::ROLE => [],
            self::USER => [],
        ];

        $grants = $connection->fetchAll(sprintf(
            'SHOW GRANTS TO ROLE %s',
            Helper::quoteIdentifier($name)
        ));

        $ownershipGrants = array_filter(
            $grants,
            fn($v) => $v['privilege'] === self::OWNERSHIP
        );

        $result = $this->processOwnershipGrants($ownershipGrants, $result);
        $result = $this->addProjectRoleAndUserToRemove($name, $mainRoleName, $result);

        return $result;
    }

    private function processOwnershipGrants(array $ownershipGrants, array $result): array
    {
        foreach ($ownershipGrants as $grant) {
            if ($grant['granted_on'] === self::ROLE) {
                $this->logger->debug(sprintf('Found owned role: %s', $grant['name']));
                $result[self::ROLE][] = $grant;
            } elseif ($grant['granted_on'] === self::USER) {
                $this->logger->debug(sprintf('Found directly owned user: %s', $grant['name']));
                $result[self::USER][] = $grant;
            }
        }
        return $result;
    }

    private function addProjectRoleAndUserToRemove(string $name, string $mainRoleName, array $result): array
    {
        $result[self::ROLE][] = [
            'name' => $name,
            'granted_by' => $mainRoleName,
        ];
        $result[self::USER][] = [
            'name' => $name,
            'granted_by' => $mainRoleName,
        ];
        return $result;
    }

    private function switchRole(string $role, string $mainRoleName, array $sqls, ?string $currentRole): array
    {
        if ($currentRole === $role) {
            return [$sqls, $currentRole];
        }

        try {
            $this->logger->debug(sprintf('Switching to role: %s', $role));
            $this->destinationConnection->useRole($role);
            $sqls[] = sprintf('USE ROLE %s;', Helper::quoteIdentifier($role));
            $currentRole = $role;
        } catch (RuntimeException $e) {
            $this->logger->info(sprintf(
                'Cannot switch directly to role %s, trying through main role %s',
                $role,
                $mainRoleName
            ));
            $this->destinationConnection->useRole($mainRoleName);
            if ($currentRole !== $mainRoleName) {
                $sqls[] = sprintf('USE ROLE %s;', Helper::quoteIdentifier($mainRoleName));
                $currentRole = $mainRoleName;
            }
            $this->destinationConnection->grantRoleToUser(
                $this->config->getTargetSnowflakeUser(),
                $role
            );
            $this->logger->debug(sprintf('Granted role %s to user, switching to it', $role));
            $this->destinationConnection->useRole($role);
            $sqls[] = sprintf('USE ROLE %s;', Helper::quoteIdentifier($role));
            $currentRole = $role;
        }
        return [$sqls, $currentRole];
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
