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
        $sourceSnflkConnection = ConnectionFactory::create(
            'source',
            $config->getMigrationRoleSourceAccount()
        );
        $migrateSnflkConnection = ConnectionFactory::create(
            'migrate',
            $config->getMigrationRoleSourceAccount()
        );
        $destinationSnflkConnection = ConnectionFactory::create(
            'destination',
            $config->getMigrationRoleTargetAccount()
        );

        //        Switch to main migration role (e.g. ACCOUNTADMIN)
        $sourceSnflkConnection->useRole($config->getMigrationRoleSourceAccount());
        $migrateSnflkConnection->useRole($config->getMigrationRoleSourceAccount());
        $destinationSnflkConnection->useRole($config->getMigrationRoleTargetAccount());

        return new Migrate(
            $logger,
            $sourceSnflkConnection,
            $migrateSnflkConnection,
            $destinationSnflkConnection,
            $config->getDatabases(),
            $config->getMigrationRoleSourceAccount(),
            $config->getMigrationRoleTargetAccount()
        );
    }
}
