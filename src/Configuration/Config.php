<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public function getMigrationRole(): string
    {
        return $this->getStringValue(['parameters', 'migrationRole']);
    }

    public function getDatabases(): array
    {
        return $this->getArrayValue(['parameters', 'migrateDatabases']);
    }

    public function getPasswordOfUsers(): array
    {
        return $this->getArrayValue(['parameters', 'passwordOfUsers']);
    }

    public function getSynchronizeRun(): bool
    {
        return is_array($this->getValue(['parameters', 'synchronize'], false));
    }

    public function getSynchronizeDryRun(): bool
    {
        /** @var bool $value */
        $value = $this->getValue(['parameters', 'synchronize', 'dryRun']);
        return $value;
    }
}
