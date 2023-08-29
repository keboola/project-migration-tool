<?php

declare(strict_types=1);

namespace ProjectMigrationTool\ValueObject;

class FutureGrantToRole
{
    public function __construct(
        private readonly string $privilege,
        private readonly string $grantOn,
        private readonly string $name,
        private readonly string $grantTo,
        private readonly string $granteeName,
        private readonly string $grantOption,
    ) {
    }

    public static function fromArray(array $data): self
    {
        $needle = preg_quote('.<TABLE>', '/');
        $name = (string) preg_replace("/$needle$/", '', $data['name']);

        return new self(
            $data['privilege'],
            $data['grant_on'],
            $name,
            $data['grant_to'],
            $data['grantee_name'],
            $data['grant_option'],
        );
    }

    public function getPrivilege(): string
    {
        return $this->privilege;
    }

    public function getGrantOn(): string
    {
        return $this->grantOn;
    }

    public function getName(): string
    {
        return $this->name;
    }

    public function getGrantTo(): string
    {
        return $this->grantTo;
    }

    public function getGranteeName(): string
    {
        return $this->granteeName;
    }

    public function getGrantOption(): string
    {
        return $this->grantOption;
    }
}
