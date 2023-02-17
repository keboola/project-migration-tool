<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\BaseComponent;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;

class Component extends BaseComponent
{
    private const ACTION_CHECK_MIGRATED_DATA = 'runCheckMigratedData';

    protected function run(): void
    {
        $migrate = MigrateFactory::create($this->getLogger(), $this->getConfig());

//        Get main role
        $this->getLogger()->info('Getting main role with grants');
        $mainRoleWithGrants = $migrate->getMainRoleWithGrants();

//        Cleanup destination account
        if ($this->getConfig()->getSynchronizeRun()) {
            $this->getLogger()->info('Pre-migration cleanup.');
            $migrate->cleanupAccount($mainRoleWithGrants['name'], $this->getConfig()->getSynchronizeDryRun());
        }

//        Create DB replication
        $this->getLogger()->info('Creating replication.');
        $migrate->createReplication();

//        Create DB sharing
        $this->getLogger()->info('Creating DB sharing.');
        $migrate->createShare();

//        Export grants from source database
        $this->getLogger()->info('Exporting grants of roles.');
        $rolesGrants = $migrate->exportRolesGrants();

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

        $migrate->printUnusedGrants($rolesGrants);
    }

    protected function runCheckMigratedData(): void
    {
        $migrate = MigrateFactory::create($this->getLogger(), $this->getConfig());

        $this->getLogger()->info('Getting main role with grants');
        $mainRoleWithGrants = $migrate->getMainRoleWithGrants();

        $migrate->postMigrationCheck($mainRoleWithGrants);
    }

    public function getConfig(): Config
    {
        /** @var Config $config */
        $config = parent::getConfig();
        return $config;
    }

    public function execute(): void
    {
        $action = $this->getConfig()->getAction();
        if (!$this->isSyncAction()) {
            $this->$action();
            return;
        }

        parent::execute();
    }

    public function isSyncAction(): bool
    {
        return !in_array($this->getConfig()->getAction(), ['run', self::ACTION_CHECK_MIGRATED_DATA]);
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
