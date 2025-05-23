<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Configuration;

use Keboola\Component\Config\BaseConfig;

class Config extends BaseConfig
{
    public const ACTION_CHECK = 'runCheckMigratedData';

    public const ACTION_MIGRATE_DATA = 'runMigrateData';

    public const ACTION_MIGRATE_STRUCTURE = 'runMigrateStructure';

    public const ACTION_CLEANUP = 'runCleanup';

    public const ACTION_CLEANUP_SOURCE_ACCOUNT = 'runCleanupSourceAccount';

    public function getWarehouseSize(): string
    {
        return $this->getStringValue(['parameters', 'warehouseSize'], 'SMALL');
    }

    public function getRunAction(): string
    {
        return $this->getStringValue(['parameters', 'action'], 'run');
    }

    public function getDatabases(): array
    {
        return $this->getArrayValue(['parameters', 'migrateDatabases']);
    }

    public function isSynchronizeRun(): bool
    {
        /** @var bool $value */
        $value = $this->getValue(['parameters', 'synchronize'], false);
        return $value;
    }

    public function skipCheck(): bool
    {
        /** @var bool $value */
        $value = $this->getValue(['parameters', 'skipCheck'], false);
        return $value;
    }

    public function getSynchronizeDryPremigrationCleanupRun(): bool
    {
        /** @var bool $value */
        $value = $this->getValue(['parameters', 'dryPremigrationCleanupRun'], true);
        return $value;
    }

    public function hasMigrateAccount(): bool
    {
        return is_array($this->getValue(['parameters', 'credentials', 'migration'], false));
    }

    public function getSourceSnowflakeHost(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'source', 'host']);
    }

    public function getMigrationSnowflakeHost(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'migration', 'host']);
    }

    public function getTargetSnowflakeHost(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'target', 'host']);
    }

    public function getSourceSnowflakeUser(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'source', 'username']);
    }

    public function getMigrationSnowflakeUser(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'migration', 'username']);
    }

    public function getTargetSnowflakeUser(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'target', 'username']);
    }

    public function getSourceSnowflakePassword(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'source', '#password'], '');
    }

    public function getMigrationSnowflakePassword(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'migration', '#password'], '');
    }

    public function getTargetSnowflakePassword(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'target', '#password'], '');
    }

    public function getSourceSnowflakePrivateKey(): ?string
    {
        /** @var string|false $value */
        $value = $this->getValue(['parameters', 'credentials', 'source', '#privateKey'], false);
        return $value ?: null;
    }

    public function getMigrationSnowflakePrivateKey(): ?string
    {
        /** @var string|false $value */
        $value = $this->getValue(['parameters', 'credentials', 'migration', '#privateKey'], false);
        return $value ?: null;
    }

    public function getTargetSnowflakePrivateKey(): ?string
    {
        /** @var string|false $value */
        $value = $this->getValue(['parameters', 'credentials', 'target', '#privateKey'], false);
        return $value ?: null;
    }

    public function getSourceSnowflakeWarehouse(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'source', 'warehouse']);
    }

    public function getMigrationSnowflakeWarehouse(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'migration', 'warehouse']);
    }

    public function getTargetSnowflakeWarehouse(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'target', 'warehouse']);
    }

    public function getSourceSnowflakeRole(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'source', 'role']);
    }

    public function getTargetSnowflakeRole(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'target', 'role']);
    }

    public function getMigrationSnowflakeRole(): string
    {
        return $this->getStringValue(['parameters', 'credentials', 'migration', 'role']);
    }
}
