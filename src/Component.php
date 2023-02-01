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

//        Switch to main migration role (e.g. ACCOUNTADMIN)
        $sourceSnflkConnection->useRole($this->getConfig()->getMigrationRole());
        $migrateSnflkConnection->useRole($this->getConfig()->getMigrationRole());
        $destinationSnflkConnection->useRole($this->getConfig()->getMigrationRole());

        $migrate = new Migrate(
            $this->getLogger(),
            $sourceSnflkConnection,
            $migrateSnflkConnection,
            $destinationSnflkConnection,
            $this->getConfig()->getDatabases(),
            $this->getConfig()->getMigrationRole()
        );

//        Cleanup destination account
        if ($this->getConfig()->getSynchronizeRun()) {
            $this->getLogger()->info('Pre-migration cleanup.');
            $migrate->cleanupAccount($this->getConfig()->getSynchronizeDryRun());
        }

//        Create DB replication
        if ($sourceSnflkConnection->getRegion() !== $destinationSnflkConnection->getRegion()) {
            $this->getLogger()->info('Creating replication.');
            $migrate->createReplication();
        }

//        Create DB sharing
        $this->getLogger()->info('Creating DB sharing.');
        $migrate->createShare();

//        Export grants from source database
        $this->getLogger()->info('Exporting grants of roles.');
        $rolesGrants = $migrate->exportRolesGrants();

//        Get main role
        $this->getLogger()->info('Getting main role with grants');
        $mainRoleWithGrants = $migrate->getMainRoleWithGrants();

//        Create MainRole in target snflk account
        $this->getLogger()->info('Creating main role in target account.');
        $migrate->createMainRole($mainRoleWithGrants, $this->getConfig()->getPasswordOfUsers());

//        create and clone databases from shares
        $this->getLogger()->info('Creating shares databases.');
        $migrate->createDatabasesFromShares();

        $this->getLogger()->info('Migrating warehouses/users/roles with grants');
        $migrate->migrateUsersRolesAndGrants($this->getConfig(), $mainRoleWithGrants['name'], $rolesGrants);

        $migrate->grantRoleToUsers();

        $this->getLogger()->info('Cloning databases with grants.');
        $migrate->cloneDatabaseWithGrants($mainRoleWithGrants['name'], $rolesGrants);

        $this->getLogger()->info('Post-migration cleanup.');
        $migrate->postMigrationCleanup();
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
