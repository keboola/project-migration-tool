<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\BaseComponent;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\Snowflake\Command;
use ProjectMigrationTool\Snowflake\ConnectionFactory;
use ProjectMigrationTool\Snowflake\Helper;

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
        Command::useRole($sourceSnflkConnection, 'ACCOUNTADMIN');
        Command::useRole($migrateSnflkConnection, 'ACCOUNTADMIN');
        Command::useRole($destinationSnflkConnection, 'ACCOUNTADMIN');

//        Cleanup destination account
        if ($this->getConfig()->getSynchronizeRun()) {
            Command::cleanupAccount(
                $this->getLogger(),
                $destinationSnflkConnection,
                $databases,
                $this->getConfig()->getSynchronizeDryRun()
            );
        }

        Command::cleanupProject($destinationSnflkConnection);

//        Export grants from source database
        $rolesGrants = Command::exportUsersAndRolesGrants($sourceSnflkConnection, $databases);

//        Get main role
        $mainRoleWithGrants = Command::getMainRoleWithGrants($sourceSnflkConnection, $databases);

//        Create MainRole in destination anflk account
        Command::createMainRole(
            $this->getLogger(),
            $sourceSnflkConnection,
            $destinationSnflkConnection,
            $mainRoleWithGrants,
            $databases,
            $this->getConfig()->getPasswordOfUsers()
        );

        $this->getLogger()->info('Check region of databases.');
        $sourceRegion = Command::getRegion($sourceSnflkConnection);
        $destinationRegion = Command::getRegion($destinationSnflkConnection);

        $destinationAccount = Command::getAccount($destinationSnflkConnection);

        if ($sourceRegion === $destinationRegion) {
            $this->getLogger()->info('Source and destination region is the same.');

            $sourceAccount = Command::getAccount($sourceSnflkConnection);

            Command::createShare($sourceSnflkConnection, $databases, $destinationAccount);
        } else {
            $sourceAccount = Command::getAccount($migrateSnflkConnection);

            // @TODO create replication and share from migration account
        }

//        create and clone databases from shares
        Command::createDatabasesFromShares($destinationSnflkConnection, $databases, $sourceAccount);
        Command::cloneDatabaseFromShared(
            $this->getLogger(),
            $this->getConfig(),
            $sourceSnflkConnection,
            $destinationSnflkConnection,
            $mainRoleWithGrants['name'],
            $databases,
            $rolesGrants
        );

        Command::grantRoleToUsers($sourceSnflkConnection, $destinationSnflkConnection, $mainRoleWithGrants['name']);
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
