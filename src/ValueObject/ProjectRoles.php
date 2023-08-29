<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class ProjectRoles
{
    /** @var array<string, Role> $roles */
    private array $roles = [];

    public function addRole(Role $role): void
    {
        $this->roles[$role->getName()] = $role;
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
