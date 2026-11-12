<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\ConnectionFactory;
use Psr\Log\LoggerInterface;
use RuntimeException;

class MigrateFactory
{
    private ?Connection $sourceConnection;

    private ?Connection $migrateConnection;

    private Connection $targetConnection;

    public function __construct(readonly LoggerInterface $logger, readonly Config $config)
    {
        // Create database connections
        $logger->info('Connecting to databases.');
        if ($this->config->hasSourceAccount()) {
            $this->sourceConnection = ConnectionFactory::create(
                $config->getSourceSnowflakeHost(),
                $config->getSourceSnowflakeUser(),
                $config->getSourceSnowflakePassword(),
                $config->getSourceSnowflakePrivateKey(),
                $config->getSourceSnowflakeWarehouse(),
                $config->getSourceSnowflakeRole(),
                'source',
                $logger
            );
            $this->sourceConnection->useRole($config->getSourceSnowflakeRole());
        }

        if ($config->hasMigrateAccount()) {
            $this->migrateConnection = ConnectionFactory::create(
                $config->getMigrationSnowflakeHost(),
                $config->getMigrationSnowflakeUser(),
                $config->getMigrationSnowflakePassword(),
                $config->getMigrationSnowflakePrivateKey(),
                $config->getMigrationSnowflakeWarehouse(),
                $config->getMigrationSnowflakeRole(),
                'migrate',
                $logger
            );
            $this->migrateConnection->useRole($config->getMigrationSnowflakeRole());
        }
        $this->targetConnection = ConnectionFactory::create(
            $config->getTargetSnowflakeHost(),
            $config->getTargetSnowflakeUser(),
            $config->getTargetSnowflakePassword(),
            $config->getTargetSnowflakePrivateKey(),
            $config->getTargetSnowflakeWarehouse(),
            $config->getTargetSnowflakeRole(),
            'destination',
            $logger
        );
        $this->targetConnection->useRole($config->getTargetSnowflakeRole());
    }

    public function createCleanup(): Cleanup
    {
        if ($this->sourceConnection === null) {
            throw new RuntimeException('Source connection is required for cleanup operation');
        }
        return new Cleanup(
            $this->config,
            $this->sourceConnection,
            $this->targetConnection,
            $this->logger,
            $this->migrateConnection ?? null,
        );
    }

    public function createPrepareMigration(): PrepareMigration
    {
        if ($this->sourceConnection === null) {
            throw new RuntimeException('Source connection is required for migration preparation');
        }
        return new PrepareMigration(
            $this->config,
            $this->config->getDatabases(),
            $this->sourceConnection,
            $this->targetConnection,
            $this->logger,
            $this->migrateConnection ?? null,
        );
    }

    public function createMetadataFetcher(): MetadataFetcher
    {
        if ($this->sourceConnection === null) {
            throw new RuntimeException('Source connection is required for metadata fetching');
        }
        return new MetadataFetcher(
            $this->sourceConnection,
            $this->config,
        );
    }

    public function createMigrateStructure(): MigrateStructure
    {
        if ($this->sourceConnection === null) {
            throw new RuntimeException('Source connection is required for structure migration');
        }
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

    public function createMigrateData(): MigrateData
    {
        if ($this->sourceConnection === null) {
            throw new RuntimeException('Source connection is required for data migration');
        }
        return new MigrateData(
            $this->sourceConnection,
            $this->targetConnection,
            $this->logger,
            $this->config,
            $this->config->getSourceSnowflakeRole(),
            $this->config->getTargetSnowflakeRole(),
            $this->config->getDatabases(),
        );
    }

    public function createMigrateDataGatewayApp(): MigrateDataGatewayApp
    {
        return new MigrateDataGatewayApp(
            $this->targetConnection,
            $this->logger,
            $this->config,
        );
    }

    public function createMigrationChecker(): MigrationChecker
    {
        if ($this->sourceConnection === null) {
            throw new RuntimeException('Source connection is required for migration checking');
        }
        return new MigrationChecker(
            $this->sourceConnection,
            $this->targetConnection,
            $this->config,
            $this->logger,
            $this->config->getDatabases(),
        );
    }
}
