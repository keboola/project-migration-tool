<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\ConnectionFactory;
use Psr\Log\LoggerInterface;

class MigrateFactory
{
    private Connection $sourceConnection;

    private ?Connection $migrateConnection;

    private Connection $targetConnection;

    public function __construct(readonly LoggerInterface $logger, readonly Config $config)
    {
        // Create database connections
        $logger->info('Connecting to databases.');
        $this->sourceConnection = ConnectionFactory::create(
            $config->getSourceSnowflakeHost(),
            $config->getSourceSnowflakeUser(),
            $config->getSourceSnowflakePassword(),
            $config->getSourceSnowflakeWarehouse(),
            $config->getSourceSnowflakeRole(),
            $logger,
        );
        $this->sourceConnection->useRole($config->getSourceSnowflakeRole());

        if ($config->hasMigrateAccount()) {
            $this->migrateConnection = ConnectionFactory::create(
                $config->getMigrationSnowflakeHost(),
                $config->getMigrationSnowflakeUser(),
                $config->getMigrationSnowflakePassword(),
                $config->getMigrationSnowflakeWarehouse(),
                $config->getMigrationSnowflakeRole(),
                $logger,
            );
            $this->migrateConnection->useRole($config->getMigrationSnowflakeRole());
        }
        $this->targetConnection = ConnectionFactory::create(
            $config->getTargetSnowflakeHost(),
            $config->getTargetSnowflakeUser(),
            $config->getTargetSnowflakePassword(),
            $config->getTargetSnowflakeWarehouse(),
            $config->getTargetSnowflakeRole(),
            $logger,
        );
        $this->targetConnection->useRole($config->getTargetSnowflakeRole());
    }

    public function createCleanupClass(): Cleanup
    {
        return new Cleanup(
            $this->config,
            $this->targetConnection,
            $this->logger,
        );
    }

    public function createPrepareMigrationClass(): PrepareMigration
    {
        return new PrepareMigration(
            $this->config->getDatabases(),
            $this->sourceConnection,
            $this->targetConnection,
            $this->migrateConnection ?? null,
        );
    }

    public function createMetadataFetcherClass(): MetadataFetcher
    {
        return new MetadataFetcher(
            $this->sourceConnection,
            $this->config->getDatabases(),
        );
    }

    public function createMigrateStructureClass(): MigrateStructure
    {
        return new MigrateStructure(
            $this->sourceConnection,
            $this->targetConnection,
            $this->logger,
            $this->config,
            $this->config->getSourceSnowflakeRole(),
            $this->config->getTargetSnowflakeRole(),
            $this->config->getDatabases(),
        );
    }

    public function createMigrationCheckerClass(): MigrationChecker
    {
        return new MigrationChecker(
            $this->sourceConnection,
            $this->targetConnection,
            $this->config,
            $this->logger,
            $this->config->getDatabases(),
        );
    }
}
