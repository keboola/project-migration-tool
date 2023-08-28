<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class RoleGrants
{
    /** @var GrantToRole[] $accountGrants */
    private array $accountGrants = [];

    /** @var GrantToRole[] $databaseGrants */
    private array $databaseGrants = [];

    /** @var GrantToRole[] $schemaGrants */
    private array $schemaGrants = [];

    /** @var GrantToRole[] $tableGrants */
    private array $tableGrants = [];

    /** @var GrantToRole[] $roleGrants */
    private array $roleGrants = [];

    /** @var GrantToRole[] $warehouseGrants */
    private array $warehouseGrants = [];

    /** @var GrantToRole[] $userGrants */
    private array $userGrants = [];

    /** @var GrantToRole[] $viewGrants */
    private array $viewGrants = [];

    /** @var GrantToRole[] $functionGrants */
    private array $functionGrants = [];

    /** @var GrantToRole[] $procedureGrants */
    private array $procedureGrants = [];

    /** @var GrantToRole[] $otherGrants */
    private array $otherGrants = [];

    public function addAccountGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'ACCOUNT', 'Grant is not on ACCOUNT');
        $this->accountGrants[] = $grant;
    }

    public function addDatabaseGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'DATABASE', 'Grant is not on DATABASE');
        $this->databaseGrants[] = $grant;
    }

    public function addSchemaGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'SCHEMA', 'Grant is not on SCHEMA');
        $this->schemaGrants[] = $grant;
    }

    public function addTableGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'TABLE', 'Grant is not on TABLE');
        $this->tableGrants[] = $grant;
    }

    public function addRoleGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'ROLE', 'Grant is not on ROLE');
        $this->roleGrants[] = $grant;
    }

    public function addWarehouseGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'WAREHOUSE', 'Grant is not on WAREHOUSE');
        $this->warehouseGrants[] = $grant;
    }

    public function addUserGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'USER', 'Grant is not on USER');
        $this->userGrants[] = $grant;
    }

    public function addViewGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'VIEW', 'Grant is not on VIEW');
        $this->viewGrants[] = $grant;
    }

    public function addFunctionGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'FUNCTION', 'Grant is not on FUNCTION');
        $this->functionGrants[] = $grant;
    }

    public function addProcedureGrant(GrantToRole $grant): void
    {
        assert($grant->getGrantedOn() === 'PROCEDURE', 'Grant is not on PROCEDURE');
        $this->procedureGrants[] = $grant;
    }

    public function addOtherGrant(GrantToRole $grant): void
    {
        assert(
            !in_array(
                $grant->getGrantedOn(),
                [
                    'ACCOUNT',
                    'DATABASE',
                    'SCHEMA',
                    'TABLE',
                    'ROLE',
                    'WAREHOUSE',
                    'USER',
                    'VIEW',
                    'FUNCTION',
                    'PROCEDURE',
                ],
                true
            ),
            'Grant is on known object'
        );
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

    /**
     * @return GrantToRole[]
     */
    public function getAllGrants(): array
    {
        return array_merge(
            $this->accountGrants,
            $this->databaseGrants,
            $this->schemaGrants,
            $this->tableGrants,
            $this->roleGrants,
            $this->warehouseGrants,
            $this->userGrants,
            $this->viewGrants,
            $this->functionGrants,
            $this->procedureGrants,
            $this->otherGrants
        );
    }
}
