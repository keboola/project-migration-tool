<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

use ProjectMigrationTool\Snowflake\Helper;

class ProjectRoles
{
    /** @var array<string, Role> $roles */
    private array $roles = [];

    /**
     * Grant name => (position in getTableGrantsFromAllRoles() => grant)
     *
     * Built on first use and cleared only by addRole(). Grants changed later on a role that is
     * already added (Role::setGrants()) are not seen, so set the grants before calling addRole().
     *
     * @var array<string, array<int, GrantToRole>>|null
     */
    private ?array $tableGrantsByName = null;

    public function addRole(Role $role): void
    {
        $this->roles[$role->getName()] = $role;
        $this->tableGrantsByName = null;
    }

    /**
     * Returns the same grants, in the same order, as filtering getTableGrantsFromAllRoles()
     * by Helper::generateFormattedQuoteCombinations($database, $schema, $table).
     *
     * @return list<GrantToRole>
     */
    public function getTableGrants(string $database, string $schema, string $table): array
    {
        $index = $this->getTableGrantsIndex();
        $grants = [];
        $names = array_unique(Helper::generateFormattedQuoteCombinations($database, $schema, $table));
        foreach ($names as $name) {
            $grants += $index[$name] ?? [];
        }
        ksort($grants);
        return array_values($grants);
    }

    /**
     * @return array<string, array<int, GrantToRole>>
     */
    private function getTableGrantsIndex(): array
    {
        if ($this->tableGrantsByName === null) {
            $this->tableGrantsByName = [];
            foreach ($this->getTableGrantsFromAllRoles() as $position => $grant) {
                $this->tableGrantsByName[$grant->getName()][$position] = $grant;
            }
        }
        return $this->tableGrantsByName;
    }

    /**
     * @return Role[]
     */
    public function getRoles(): array
    {
        return $this->roles;
    }

    public function getRole(string $roleName): Role
    {
        return $this->roles[$roleName];
    }

    /**
     * @return GrantToRole[]
     */
    public function getAccountGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getAccountGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getDatabaseGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getDatabaseGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getSchemaGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getSchemaGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getTableGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getTableGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getRoleGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getRoleGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getWarehouseGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getWarehouseGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getUserGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getUserGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getViewGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getViewGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getFunctionGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getFunctionGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getProcedureGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getProcedureGrants());
        }
        return $grants;
    }

    /**
     * @return GrantToRole[]
     */
    public function getOtherGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedGrants()->getOtherGrants());
        }
        return $grants;
    }

    /**
     * @return FutureGrantToRole[]
     */
    public function getFutureTableGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedFutureRoles()->getTableGrants());
        }
        return $grants;
    }

    /**
     * @return FutureGrantToRole[]
     */
    public function getFutureOtherGrantsFromAllRoles(): array
    {
        $grants = [];
        foreach ($this->roles as $role) {
            $grants = array_merge($grants, $role->getAssignedFutureRoles()->getOtherGrants());
        }
        return $grants;
    }
}
