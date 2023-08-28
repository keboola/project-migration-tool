<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class RoleGrants
{
    private array $accountGrants = [];

    private array $databaseGrants = [];

    private array $schemaGrants = [];

    private array $tableGrants = [];

    private array $roleGrants = [];

    private array $warehouseGrants = [];

    private array $userGrants = [];

    private array $viewGrants = [];

    private array $functionGrants = [];

    private array $procedureGrants = [];

    private array $otherGrants = [];

    public function addAccountGrant(GrantToRole $grant): void
    {
        $this->accountGrants[] = $grant;
    }

    public function addDatabaseGrant(GrantToRole $grant): void
    {
        $this->databaseGrants[] = $grant;
    }

    public function addSchemaGrant(GrantToRole $grant): void
    {
        $this->schemaGrants[] = $grant;
    }

    public function addTableGrant(GrantToRole $grant): void
    {
        $this->tableGrants[] = $grant;
    }

    public function addRoleGrant(GrantToRole $grant): void
    {
        $this->roleGrants[] = $grant;
    }

    public function addWarehouseGrant(GrantToRole $grant): void
    {
        $this->warehouseGrants[] = $grant;
    }

    public function addUserGrant(GrantToRole $grant): void
    {
        $this->userGrants[] = $grant;
    }

    public function addViewGrant(GrantToRole $grant): void
    {
        $this->viewGrants[] = $grant;
    }

    public function addFunctionGrant(GrantToRole $grant): void
    {
        $this->functionGrants[] = $grant;
    }

    public function addProcedureGrant(GrantToRole $grant): void
    {
        $this->procedureGrants[] = $grant;
    }

    public function addOtherGrant(GrantToRole $grant): void
    {
        $this->otherGrants[] = $grant;
    }

    public function getAccountGrants(): array
    {
        return $this->accountGrants;
    }

    public function getDatabaseGrants(): array
    {
        return $this->databaseGrants;
    }

    public function getSchemaGrants(): array
    {
        return $this->schemaGrants;
    }

    public function getTableGrants(): array
    {
        return $this->tableGrants;
    }

    public function getRoleGrants(): array
    {
        return $this->roleGrants;
    }

    public function getWarehouseGrants(): array
    {
        return $this->warehouseGrants;
    }

    public function getUserGrants(): array
    {
        return $this->userGrants;
    }

    public function getViewGrants(): array
    {
        return $this->viewGrants;
    }

    public function getFunctionGrants(): array
    {
        return $this->functionGrants;
    }

    public function getProcedureGrants(): array
    {
        return $this->procedureGrants;
    }

    public function getOtherGrants(): array
    {
        return $this->otherGrants;
    }
}
