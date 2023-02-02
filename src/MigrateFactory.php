<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\ConnectionFactory;
use Psr\Log\LoggerInterface;

class MigrateFactory
{
    public static function create(LoggerInterface $logger, Config $config): Migrate
    {
        //        Create database connections
        $logger->info('Connecting to databases.');
        $sourceSnflkConnection = ConnectionFactory::create('source');
        $migrateSnflkConnection = ConnectionFactory::create('migrate');
        $destinationSnflkConnection = ConnectionFactory::create('destination');

        //        Switch to main migration role (e.g. ACCOUNTADMIN)
        $sourceSnflkConnection->useRole($config->getMigrationRole());
        $migrateSnflkConnection->useRole($config->getMigrationRole());
        $destinationSnflkConnection->useRole($config->getMigrationRole());

        return new Migrate(
            $logger,
            $sourceSnflkConnection,
            $migrateSnflkConnection,
            $destinationSnflkConnection,
            $config->getDatabases(),
            $config->getMigrationRole()
        );
    }
}
