<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Snowflake\Helper;

class MetadataFetcher
{
    public function __construct(
        readonly Snowflake\Connection $sourceConnection,
        readonly array $databases,
    ) {
    }

    public function getMainRoleWithGrants(): array
    {
        $grantsOfRoles = [];
        foreach ($this->databases as $database) {
            $roleName = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $grantedOnDatabaseRole = $this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS ON ROLE %s',
                Helper::quoteIdentifier($roleName)
            ));

            $ownershipOfRole = array_filter($grantedOnDatabaseRole, fn($v) => $v['privilege'] === 'OWNERSHIP');

            $ownershipOfRole = array_map(fn($v) => $v['grantee_name'], $ownershipOfRole);

            $grantsOfRoles = array_merge($grantsOfRoles, array_unique($ownershipOfRole));
        }

        $uniqueMainRoles = array_unique($grantsOfRoles);

        assert(count($uniqueMainRoles) === 1);

        $mainRole = current($uniqueMainRoles);

        return [
            'name' => $mainRole,
            'assignedGrants' => $this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s;',
                Helper::quoteIdentifier($mainRole)
            )),
        ];
    }

    public function exportRolesGrants(): array
    {
        $tmp = [];
        foreach ($this->databases as $database) {
            $databaseRole = $this->sourceConnection->getOwnershipRoleOnDatabase($database);

            $roles = $this->sourceConnection->fetchAll(sprintf(
                'SHOW ROLES LIKE %s',
                QueryBuilder::quote($databaseRole)
            ));

            $roles = $this->getOtherRolesToMainProjectRole($roles);

            foreach ($roles as $role) {
                $tmp[$databaseRole][] = array_merge(
                    $role,
                    [
                        'assignedGrants' => $this->sourceConnection->fetchAll(sprintf(
                            'SHOW GRANTS TO ROLE %s;',
                            Helper::quoteIdentifier($role['name'])
                        )),
                        'assignedFutureGrants' => $this->sourceConnection->fetchAll(sprintf(
                            'SHOW FUTURE GRANTS TO ROLE %s;',
                            Helper::quoteIdentifier($role['name'])
                        )),
                    ]
                );
            }
        }
        return $tmp;
    }


    private function getOtherRolesToMainProjectRole(array $roles): array
    {
        foreach ($roles as $role) {
            $grantsToRole = $this->sourceConnection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s;',
                Helper::quoteIdentifier($role['name'])
            ));

            $ownershipToRole = array_filter($grantsToRole, fn($v) => $v['privilege'] === 'OWNERSHIP');

            $rolesInRole = array_filter($ownershipToRole, fn($v) => $v['granted_on'] === 'ROLE');
            $filteredRolesInRole = array_filter($rolesInRole, fn($v) => !in_array($v['name'], $roles));
            $roles = array_merge($roles, array_combine(
                array_map(fn($v) => $v['name'], $filteredRolesInRole),
                $filteredRolesInRole
            ));
        }

        return $roles;
    }
}
