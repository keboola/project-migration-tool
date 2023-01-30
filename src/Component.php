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
        $databases = $this->getConfig()->getDatabases();

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
            $destinationSnflkConnection
        );

//        Cleanup destination account
        if ($this->getConfig()->getSynchronizeRun()) {
            $migrate->cleanupAccount($databases, $this->getConfig()->getSynchronizeDryRun());
        }

//        Create DB replication
        if ($sourceSnflkConnection->getRegion() !== $destinationSnflkConnection->getRegion()) {
            $this->getLogger()->info('Creating replication.');
            $migrate->createReplication($databases);
        }

//        !!!! !!!!! REMOVE ME !!!!! !!!!
//        $migrate->cleanupProject();

//        Export grants from source database
        $rolesGrants = $migrate->exportUsersAndRolesGrants($databases);

//        Get main role
        $mainRoleWithGrants = $migrate->getMainRoleWithGrants($databases);

//        Create MainRole in destination anflk account
        $migrate->createMainRole(
            $mainRoleWithGrants,
            $databases,
            $this->getConfig()->getPasswordOfUsers()
        );

//        Create sharing
        $migrate->createShare($databases);

//        create and clone databases from shares
        $migrate->createDatabasesFromShares($databases);
        $migrate->cloneDatabaseFromShared(
            $this->getConfig(),
            $mainRoleWithGrants['name'],
            $databases,
            $rolesGrants,
            $this->getConfig()->getSynchronizeRun()
        );

        $migrate->grantRoleToUsers($mainRoleWithGrants['name']);
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
