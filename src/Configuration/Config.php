<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getDatabases(): array
    {
        return $this->getArrayValue(['parameters', 'migrateDatabases']);
    }

    public function getPasswordOfUsers(): array
    {
        return $this->getArrayValue(['parameters', 'passwordOfUsers']);
    }
}
