<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class GrantToUser
{
    public function __construct(
        private readonly string $role,
        private readonly string $grantedTo,
        private readonly string $granteeName,
        private readonly string $grantedBy,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            $data['role'],
            $data['granted_to'],
            $data['grantee_name'],
            $data['granted_by'],
        );
    }

    public function getRole(): string
    {
        return $this->role;
    }

    public function getGrantedTo(): string
    {
        return $this->grantedTo;
    }

    public function getGranteeName(): string
    {
        return $this->granteeName;
    }

    public function getGrantedBy(): string
    {
        return $this->grantedBy;
    }
}
