<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\ValueObject\FutureGrantToRole;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\ProjectRoles;
use ProjectMigrationTool\ValueObject\Role;

class MetadataFetcher
{
    public function __construct(
        readonly Snowflake\Connection $sourceConnection,
        readonly Config $config,
    ) {
    }

    public function getMainRoleWithGrants(): Role
    {
        $grantsOfRoles = [];
        foreach ($this->config->getDatabases() as $database) {
            $roleName = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $grantedOnDatabaseRole = array_map(
                fn(array $grant) => GrantToRole::fromArray($grant),
                $this->sourceConnection->fetchAll(sprintf(
                    'SHOW GRANTS ON ROLE %s',
                    Helper::quoteIdentifier($roleName)
                ))
            );

            $ownershipOfRole = array_filter($grantedOnDatabaseRole, fn($v) => $v->getPrivilege() === 'OWNERSHIP');
            $ownershipOfRole = array_map(fn($v) => $v->getGranteeName(), $ownershipOfRole);

            $grantsOfRoles = array_merge($grantsOfRoles, array_unique($ownershipOfRole));
        }

        $uniqueMainRoles = array_unique($grantsOfRoles);
        assert(count($uniqueMainRoles) === 1);

        $mainRoleResult = $this->sourceConnection->fetchAll(sprintf(
            'SHOW ROLES LIKE %s',
            QueryBuilder::quote(current($uniqueMainRoles))
        ));
        $mainRole = Role::fromArray(current($mainRoleResult));

        $grants = array_map(
            fn(array $v) => GrantToRole::fromArray($v),
            Helper::filterUserDollarGrants($this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s;',
                Helper::quoteIdentifier($mainRole->getName())
            )))
        );

        $mainRole->setGrants(Helper::parseGrantsToObjects($grants, $this->config));

        return $mainRole;
    }

    public function getRolesWithGrants(): array
    {
        $tmp = [];
        foreach ($this->config->getDatabases() as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $rolesResult = $this->sourceConnection->fetchAll(
                sprintf('SHOW ROLES LIKE %s', QueryBuilder::quote($databaseRole))
            );

            $roles = [];
            foreach ($rolesResult as $roleResult) {
                $role = Role::fromArray($roleResult);
                $roles[$role->getName()] = $role;
            }

            $roles = $this->getOtherRolesToMainProjectRole($roles);

            $projectRoles = new ProjectRoles();
            foreach ($roles as $role) {
                $grants = array_map(
                    fn(array $v) => GrantToRole::fromArray($v),
                    Helper::filterUserDollarGrants($this->sourceConnection->fetchAll(sprintf(
                        'SHOW GRANTS TO ROLE %s;',
                        Helper::quoteIdentifier($role->getName())
                    )))
                );

                $futureGrants = array_map(
                    fn(array $v) => FutureGrantToRole::fromArray($v),
                    $this->sourceConnection->fetchAll(sprintf(
                        'SHOW FUTURE GRANTS TO ROLE %s;',
                        Helper::quoteIdentifier($role->getName())
                    ))
                );

                $role->setGrants(Helper::parseGrantsToObjects($grants, $this->config));
                $role->setFutureGrants(Helper::parseFutureGrantsToObjects($futureGrants));
                $projectRoles->addRole($role);
            }

            $tmp[$databaseRole] = $projectRoles;
        }
        return $tmp;
    }

    /**
     * @param Role[] $roles
     * @return Role[]
     */
    private function getOtherRolesToMainProjectRole(array $roles): array
    {
        foreach ($roles as $role) {

            /** @var GrantToRole[] $grantsToRole */
            $grantsToRole = array_map(
                fn(array $v) => GrantToRole::fromArray($v),
                Helper::filterUserDollarGrants($this->sourceConnection->fetchAll(sprintf(
                    'SHOW GRANTS TO ROLE %s;',
                    Helper::quoteIdentifier($role->getName())
                )))
            );

            $ownershipToRole = array_filter($grantsToRole, fn(GrantToRole $v) => $v->getPrivilege() === 'OWNERSHIP');
            $rolesInRole = array_filter($ownershipToRole, fn(GrantToRole $v) => $v->getGrantedOn() === 'ROLE');
            $filteredRolesInRole = array_filter($rolesInRole, fn(GrantToRole $v) => !in_array($v->getName(), $roles));

            foreach ($filteredRolesInRole as $item) {
                if (Helper::isWorkspaceRole($item->getName())) {
                    continue;
                }
                $roles[$item->getName()] = Role::fromArray([
                    'name' => $item->getName(),
                    'owner' => $item->getGranteeName(),
                ]);
            }
        }

        return $roles;
    }
}
