<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class Role
{
    private RoleGrants $assignedGrants;
    private RoleFutureGrants $assignedFutureRoles;

    public function __construct(
        private readonly string $name,
        private readonly string $owner
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'],
            $data['owner'],
        );
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getOwner(): string
    {
        return $this->owner;
    }

    public function getAssignedGrants(): RoleGrants
    {
        return $this->assignedGrants;
    }

    public function getAssignedFutureRoles(): RoleFutureGrants
    {
        return $this->assignedFutureRoles;
    }

    public function setGrants(RoleGrants $roleGrants): void
    {
        foreach ($roleGrants->getAllGrants() as $grant) {
            assert($grant->getGranteeName() === $this->getName(), 'Grant is not assigned to this role');
        }
        $this->assignedGrants = $roleGrants;
    }

    public function setFutureGrants(RoleFutureGrants $roleFutureGrants): void
    {
        foreach ($roleFutureGrants->getAllGrants() as $grant) {
            assert($grant->getGranteeName() === $this->getName(), 'Future grant is not assigned to this role');
        }
        $this->assignedFutureRoles = $roleFutureGrants;
    }
}
