<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class RoleFutureGrants
{
    /** @var FutureGrantToRole[] $tableGrants */
    private array $tableGrants = [];

    /** @var FutureGrantToRole[] $otherGrants */
    private array $otherGrants = [];

    public function addTableGrant(FutureGrantToRole $grant): void
    {
        assert($grant->getGrantOn() === 'TABLE', 'Grant is not on TABLE');
        $this->tableGrants[] = $grant;
    }

    public function addOtherGrant(FutureGrantToRole $grant): void
    {
        assert($grant->getGrantOn() !== 'TABLE', 'Grant is on TABLE');
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

    /**
     * @return FutureGrantToRole[]
     */
    public function getAllGrants(): array
    {
        return array_merge($this->tableGrants, $this->otherGrants);
    }
}
