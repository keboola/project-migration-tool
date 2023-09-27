<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Exception;
use Keboola\Component\BaseComponent;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;

class Component extends BaseComponent
{
    protected function run(): void
    {
        switch ($this->getConfig()->getRunAction()) {
            case Config::ACTION_MIGRATE_STRUCTURE:
                $this->runMigrateStructure();
                break;
            case Config::ACTION_MIGRATE_DATA:
                $this->runMigrateData();
                break;
            case Config::ACTION_CLEANUP:
                $this->runCleanup();
                break;
            case Config::ACTION_CHECK:
                $this->runCheckMigratedData();
                break;
            case Config::ACTION_CLEANUP_SOURCE_ACCOUNT:
                $this->runCleanupSourceAccount();
                break;
            default:
                throw new Exception(sprintf('Action "%s" is not supported.', $this->getConfig()->getAction()));
        }
    }

    private function runMigrateStructure(): void
    {
        $migrateFactory = new MigrateFactory($this->getLogger(), $this->getConfig());

        $cleanup = $migrateFactory->createCleanup();
        $prepareMigration = $migrateFactory->createPrepareMigration();
        $metadataFetcher = $migrateFactory->createMetadataFetcher();
        $migrateStructure = $migrateFactory->createMigrateStructure();

        // Get main role
        $this->getLogger()->info('Getting main role with grants');
        $mainRoleWithGrants = $metadataFetcher->getMainRoleWithGrants();

        // Cleanup destination account
        if ($this->getConfig()->isSynchronizeRun()) {
            $this->getLogger()->info('Pre-migration cleanup.');
            $cleanup->preMigration($mainRoleWithGrants->getName());
        }

        // Export grants from source database
        $this->getLogger()->info('Exporting grants of roles.');
        $roles = $metadataFetcher->getRolesWithGrants();

        // Create DB replication
        $this->getLogger()->info('Creating replication.');
        $prepareMigration->createReplication();

        // Create DB sharing
        $this->getLogger()->info('Creating DB sharing.');
        $prepareMigration->createShare();

        // Create and clone databases from shares
        $this->getLogger()->info('Creating shares databases.');
        $prepareMigration->createDatabasesFromShares();

        // Create MainRole in target snflk account
        $this->getLogger()->info('Creating main role in target account.');
        $migrateStructure->createMainRole($mainRoleWithGrants);

        $this->getLogger()->info('Migrating warehouses/users/roles with grants');
        $migrateStructure->migrateUsersRolesAndGrants($mainRoleWithGrants, $roles);

        $migrateStructure->grantRoleToUsers();

        $this->getLogger()->info('Cloning databases with grants.');
        $migrateStructure->cloneDatabaseWithGrants($mainRoleWithGrants, $roles);

        $this->getLogger()->info('Re-applying failed grants.');
        $migrateStructure->reApplyFailedGrants();

        $migrateStructure->printUnusedGrants($roles);
    }

    private function runMigrateData(): void
    {
        $migrateFactory = new MigrateFactory($this->getLogger(), $this->getConfig());

        $metadataFetcher = $migrateFactory->createMetadataFetcher();
        $migrateData = $migrateFactory->createMigrateData();

        $mainRole = $metadataFetcher->getMainRoleWithGrants();
        $roles = $metadataFetcher->getRolesWithGrants();

        $migrateData->migrate($mainRole, $roles);
    }

    private function runCleanup(): void
    {
        $migrateFactory = new MigrateFactory($this->getLogger(), $this->getConfig());

        $cleanup = $migrateFactory->createCleanup();

        $this->getLogger()->info('Post-migration cleanup.');
        $cleanup->postMigration();
    }

    private function runCheckMigratedData(): void
    {
        $migrateFactory = new MigrateFactory($this->getLogger(), $this->getConfig());
        $metadataFetcher = $migrateFactory->createMetadataFetcher();
        $migrationChecker = $migrateFactory->createMigrationChecker();

        $this->getLogger()->info('Getting main role with grants');
        $mainRoleWithGrants = $metadataFetcher->getMainRoleWithGrants();

        $this->getLogger()->info('Checking data.');
        $migrationChecker->postMigrationCheckStructure($mainRoleWithGrants);
//        $migrationChecker->postMigrationCheckData($mainRoleWithGrants);
    }

    private function runCleanupSourceAccount(): void
    {
        $migrateFactory = new MigrateFactory($this->getLogger(), $this->getConfig());

        $cleanup = $migrateFactory->createCleanup();

        $this->getLogger()->info('Cleanup source account.');
        $cleanup->sourceAccount();
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
