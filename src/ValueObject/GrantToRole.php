<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class GrantToRole
{
    public function __construct(
        private readonly string $name,
        private readonly string $privilege,
        private readonly string $grantedOn,
        private readonly string $grantedTo,
        private readonly string $granteeName,
        private readonly string $grantOption,
        private readonly string $grantedBy,
    ) {
    }

    public static function fromArray(array $data): self
    {
        return new self(
            $data['name'],
            $data['privilege'],
            $data['granted_on'],
            $data['granted_to'],
            $data['grantee_name'],
            $data['grant_option'],
            $data['granted_by'],
        );
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getPrivilege(): string
    {
        return $this->privilege;
    }

    public function getGrantedOn(): string
    {
        return $this->grantedOn;
    }

    public function getGrantedTo(): string
    {
        return $this->grantedTo;
    }

    public function getGranteeName(): string
    {
        return $this->granteeName;
    }

    public function getGrantOption(): string
    {
        return $this->grantOption;
    }

    public function getGrantedBy(): string
    {
        return $this->grantedBy;
    }
}
