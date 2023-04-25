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
            $config->getSourceSnowflakeHost(),
            $config->getSourceSnowflakeUser(),
            $config->getSourceSnowflakePassword(),
            $config->getSourceSnowflakeWarehouse(),
            $config->getSourceSnowflakeRole()
        );
        $migrateSnflkConnection = ConnectionFactory::create(
            $config->getMigrationSnowflakeHost(),
            $config->getMigrationSnowflakeUser(),
            $config->getMigrationSnowflakePassword(),
            $config->getMigrationSnowflakeWarehouse(),
            $config->getMigrationSnowflakeRole()
        );
        $destinationSnflkConnection = ConnectionFactory::create(
            $config->getTargetSnowflakeHost(),
            $config->getTargetSnowflakeUser(),
            $config->getTargetSnowflakePassword(),
            $config->getTargetSnowflakeWarehouse(),
            $config->getTargetSnowflakeRole()
        );

        //        Switch to main migration role (e.g. ACCOUNTADMIN)
        $sourceSnflkConnection->useRole($config->getSourceSnowflakeRole());
        $migrateSnflkConnection->useRole($config->getMigrationSnowflakeRole());
        $destinationSnflkConnection->useRole($config->getTargetSnowflakeRole());

        return new Migrate(
            $logger,
            $sourceSnflkConnection,
            $migrateSnflkConnection,
            $destinationSnflkConnection,
            $config->getDatabases(),
            $config->getSourceSnowflakeRole(),
            $config->getTargetSnowflakeRole()
        );
    }
}
