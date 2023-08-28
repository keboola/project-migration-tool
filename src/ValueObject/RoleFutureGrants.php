<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class RoleFutureGrants
{
    private array $tableGrants = [];

    private array $otherGrants = [];

    public function addTableGrant(FutureGrantToRole $grant): void
    {
        $this->tableGrants[] = $grant;
    }

    public function addOtherGrant(FutureGrantToRole $grant): void
    {
        $this->otherGrants[] = $grant;
    }

    /**
     * @return FutureGrantToRole[]
     */
    public function getTableGrants(): array
    {
        return $this->tableGrants;
    }

    /**
     * @return FutureGrantToRole[]
     */
    public function getOtherGrants(): array
    {
        return $this->otherGrants;
    }
}
