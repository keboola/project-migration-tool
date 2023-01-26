<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\BaseComponent;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Configuration\ConfigDefinition;
use ProjectMigrationTool\Snowflake\Command;
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
        Command::useRole($sourceSnflkConnection, 'ACCOUNTADMIN');
        Command::useRole($migrateSnflkConnection, 'ACCOUNTADMIN');
        Command::useRole($destinationSnflkConnection, 'ACCOUNTADMIN');

//        Export grants from source database
        $userAndRolesGrants = Command::exportUsersAndRolesGrants($sourceSnflkConnection, $databases);

//        Get main role
        $mainRole = Command::getMainRole($sourceSnflkConnection, $databases);

//        Cleanup destination account
        Command::cleanupProject($destinationSnflkConnection, 'migrate');

//        Create MainRole in destination anflk account
        Command::createMainRole(
            $destinationSnflkConnection,
            $mainRole,
            'migrate',
            $this->getConfig()->getUsers()
        );

//
//        [
//            'databases' => $databaseGrants,
//            'schemas' => $schemasGrants,
//            'tables' => $tablesGrants,
//            'roles' => $rolesGrants,
//            'account' => $accountGrants,
//            'warehouse' => $warehouseGrants,
//            'other' => $otherGrants
//        ] = Helper::parseGrantsToObjects($userAndRolesGrants['SAPI_9472']);
//
//        file_put_contents('data/account.json', json_encode($accountGrants));
//        file_put_contents('data/grants.json', json_encode($userAndRolesGrants));
//        file_put_contents('data/ownerships.json', json_encode($ownershipGrants));
//        file_put_contents('data/databaseGrants.json', json_encode($databaseGrants));
//        file_put_contents('data/schemasGrants.json', json_encode($schemasGrants));
//        file_put_contents('data/tablesGrants.json', json_encode($tablesGrants));
//        file_put_contents('data/rolesGrants.json', json_encode($rolesGrants));
//        file_put_contents('data/otherGrants.json', json_encode($otherGrants));
//        file_put_contents('data/warehouseGrants.json', json_encode($warehouseGrants));

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
            $destinationSnflkConnection,
            $mainRole,
            $databases,
            $userAndRolesGrants
        );

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
