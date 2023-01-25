<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getDefaultRole(): string
    {
        return $this->getStringValue(['parameters', 'defaultRole']);
    }

    public function getDatabases(): array
    {
        return $this->getArrayValue(['parameters', 'migrateDatabases']);
    }

    public function getUsers(): array
    {
        return $this->getArrayValue(['parameters', 'users']);
    }
}
