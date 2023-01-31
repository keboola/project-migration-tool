<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\BaseComponent;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\Snowflake\ConnectionFactory;

class Component extends BaseComponent
{
    protected function run(): void
    {
//        Create database connections
        $this->getLogger()->info('Connecting to databases.');
        $sourceSnflkConnection = ConnectionFactory::create('source');
        $migrateSnflkConnection = ConnectionFactory::create('migrate');
        $destinationSnflkConnection = ConnectionFactory::create('destination');

//        Switch to accountadmin role
        $sourceSnflkConnection->useRole('ACCOUNTADMIN');
        $migrateSnflkConnection->useRole('ACCOUNTADMIN');
        $destinationSnflkConnection->useRole('ACCOUNTADMIN');

        $migrate = new Migrate(
            $this->getLogger(),
            $sourceSnflkConnection,
            $migrateSnflkConnection,
            $destinationSnflkConnection,
            $this->getConfig()->getDatabases()
        );

//        Cleanup destination account
        if ($this->getConfig()->getSynchronizeRun()) {
            $migrate->cleanupAccount($this->getConfig()->getSynchronizeDryRun());
        }

//        Create DB replication
        if ($sourceSnflkConnection->getRegion() !== $destinationSnflkConnection->getRegion()) {
            $this->getLogger()->info('Creating replication.');
            $migrate->createReplication();
        }

//        Create DB sharing
        $migrate->createShare();

//        !!!! !!!!! REMOVE ME !!!!! !!!!
//        $migrate->cleanupProject();

//        Export grants from source database
        $rolesGrants = $migrate->exportRolesGrants();

//        Get main role
        $mainRoleWithGrants = $migrate->getMainRoleWithGrants();

//        Create MainRole in destination anflk account
        $migrate->createMainRole($mainRoleWithGrants, $this->getConfig()->getPasswordOfUsers());

//        create and clone databases from shares
        $migrate->createDatabasesFromShares();
        $migrate->cloneDatabaseWithGrants(
            $this->getConfig(),
            $mainRoleWithGrants['name'],
            $rolesGrants,
            $this->getConfig()->getSynchronizeRun()
        );

        $migrate->grantRoleToUsers();

//        $migrate->postMigrationCleanup();
//        $migrate->postMigrationCheck();
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
