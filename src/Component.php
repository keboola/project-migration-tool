<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\BaseComponent;
use Keboola\Component\Config\BaseConfig;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\Snowflake\Command;
use ProjectMigrationTool\Snowflake\ConnectionFactory;

class Component extends BaseComponent
{
    protected function run(): void
    {
        $databases = $this->getConfig()->getDatabases();

        $this->getLogger()->info('Connecting to databases.');
        $sourceSnflkConnection = ConnectionFactory::create('source');
        $migrateSnflkConnection = ConnectionFactory::create('migrate');
        $destinationSnflkConnection = ConnectionFactory::create('destination');

        $this->getLogger()->info('Check region of databases.');
        $sourceRegion = Command::getRegion($sourceSnflkConnection);
        $destinationRegion = Command::getRegion($destinationSnflkConnection);

        $destinationAccount = Command::getAccount($destinationSnflkConnection);

        if ($sourceRegion === $destinationRegion) {
            $this->getLogger()->info('Source and destination region is the same.');

            $sourceAccount = Command::getAccount($sourceSnflkConnection);

            Command::useRole($sourceSnflkConnection, 'ACCOUNTADMIN');
            Command::createShare($sourceSnflkConnection, $databases, $destinationAccount);
        } else {
            $sourceAccount = Command::getAccount($migrateSnflkConnection);

            // @TODO create replication and share from migration account
        }

        // create and clone databases from shares
        Command::useRole($destinationSnflkConnection, 'ACCOUNTADMIN');
        Command::createDatabasesFromShares($destinationSnflkConnection, $databases, $sourceAccount);
        Command::cloneDatabaseFromShared($destinationSnflkConnection, $databases);

        Command::exportUsersAndRoles($sourceSnflkConnection);

        Command::useRole($sourceSnflkConnection, $this->getConfig()->getDefaultRole());

        var_dump($sourceRegion, $destinationRegion);
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    protected function getConfigClass(): string
    {
        return Config::class;
    }

    protected function getConfigDefinitionClass(): string
    {
        return ConfigDefinition::class;
    }
}
