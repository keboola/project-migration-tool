<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Configuration;

use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;

class ConfigTest extends TestCase
{
    /**
     * @param array<string, mixed> $parameters
     */
    private function makeConfig(array $parameters): Config
    {
        return new Config(['parameters' => $parameters], new ConfigDefinition());
    }

    public function testRefreshDefaults(): void
    {
        $config = $this->makeConfig([
            'action' => Config::ACTION_MIGRATE_STRUCTURE,
            'migrateDatabases' => ['DB1'],
        ]);

        self::assertSame(3600, $config->getReplicationRefreshTimeout());
        self::assertSame(30, $config->getReplicationRefreshPollInterval());
    }

    public function testRefreshOverrides(): void
    {
        $config = $this->makeConfig([
            'action' => Config::ACTION_MIGRATE_STRUCTURE,
            'migrateDatabases' => ['DB1'],
            'replicationRefreshTimeout' => 120,
            'replicationRefreshPollInterval' => 5,
        ]);

        self::assertSame(120, $config->getReplicationRefreshTimeout());
        self::assertSame(5, $config->getReplicationRefreshPollInterval());
    }
}
