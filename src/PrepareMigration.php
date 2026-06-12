<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\Snowflake\ReplicationGroup;
use Psr\Log\LoggerInterface;

class PrepareMigration
{
    private const MIGRATION_SHARE_PREFIX = 'MIGRATION_SHARE_';

    public function __construct(
        readonly Config $config,
        readonly array $databases,
        readonly Snowflake\Connection $sourceConnection,
        readonly Snowflake\Connection $destinationConnection,
        readonly LoggerInterface $logger,
        readonly ?Snowflake\Connection $migrateConnection = null,
    ) {
    }

    public function createReplication(): void
    {
        if ($this->sourceConnection->getRegion() === $this->destinationConnection->getRegion()) {
            // Same-region migration uses sharing only; no replication group needed.
            return;
        }
        if (!$this->migrateConnection) {
            throw new UserException('Migration connection is not set');
        }

        $groupName = ReplicationGroup::buildName($this->databases);

        // 1. Create (or ensure) the replication group on the SOURCE account, allowing the migrate account.
        $this->logger->info(sprintf('Ensuring replication group "%s" on source account.', $groupName));
        $this->sourceConnection->query(ReplicationGroup::createOnSourceSql(
            $groupName,
            $this->databases,
            $this->migrateConnection->getRegion(),
            $this->migrateConnection->getAccount(),
        ));

        // 2. Reconcile membership idempotently (handles re-runs where the database set changed).
        $this->sourceConnection->query(ReplicationGroup::setAllowedDatabasesSql(
            $groupName,
            $this->databases,
        ));

        // 3. Create (or ensure) the replica replication group on the MIGRATE account.
        $this->logger->info(sprintf('Ensuring replica replication group "%s" on migrate account.', $groupName));
        $this->migrateConnection->query(ReplicationGroup::createReplicaSql(
            $groupName,
            $this->sourceConnection->getRegion(),
            $this->sourceConnection->getAccount(),
        ));

        // 4. Ensure a warehouse to drive the refresh.
        $this->ensureMigrateWarehouse();

        // 5. Trigger the refresh on the migrate account and poll until complete.
        $this->logger->info(sprintf('Refreshing replication group "%s".', $groupName));
        $this->migrateConnection->query(ReplicationGroup::refreshSql($groupName));

        $replicationGroup = new ReplicationGroup();
        $migrateConnection = $this->migrateConnection;
        $replicationGroup->waitForRefresh(
            $groupName,
            fetchProgress: fn(): array => $migrateConnection->fetchAll(
                ReplicationGroup::refreshProgressSql($groupName)
            ),
            sleeper: fn(int $seconds): int => sleep($seconds),
            clock: fn(): int => time(),
            logger: $this->logger,
            timeoutSeconds: $this->config->getReplicationRefreshTimeout(),
            pollIntervalSeconds: $this->config->getReplicationRefreshPollInterval(),
        );
    }

    private function ensureMigrateWarehouse(): void
    {
        assert($this->migrateConnection !== null);

        $this->migrateConnection->query(<<<SQL
CREATE WAREHOUSE IF NOT EXISTS "migrate"
    WITH WAREHOUSE_SIZE = 'Small'
        WAREHOUSE_TYPE = 'STANDARD'
        AUTO_SUSPEND = 300
        AUTO_RESUME = true
;
SQL);
        $this->migrateConnection->query('USE WAREHOUSE "migrate";');
    }

    public function createShare(): void
    {
        $sourceRegion = $this->sourceConnection->getRegion();
        $destinationRegion = $this->destinationConnection->getRegion();

        $connection = $this->sourceConnection;
        if ($sourceRegion !== $destinationRegion) {
            if (!$this->migrateConnection) {
                throw new UserException('Migration connection is not set');
            }
            $connection = $this->migrateConnection;
        }

        foreach ($this->databases as $database) {
            $shareName = sprintf('%s%s', self::MIGRATION_SHARE_PREFIX, strtoupper($database));

            $connection->query(sprintf(
                'CREATE SHARE IF NOT EXISTS %s;',
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON DATABASE %s TO SHARE %s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON ALL SCHEMAS IN DATABASE %s TO SHARE %s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT SELECT ON ALL TABLES IN DATABASE %s TO SHARE %s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'ALTER SHARE %s ADD ACCOUNT=%s;',
                Helper::quoteIdentifier($shareName),
                $this->destinationConnection->getAccount()
            ));
        }
    }

    public function createDatabasesFromShares(): void
    {
        $sourceRegion = $this->sourceConnection->getRegion();
        $destinationRegion = $this->destinationConnection->getRegion();

        $connection = $this->sourceConnection;
        if ($sourceRegion !== $destinationRegion) {
            if (!$this->migrateConnection) {
                throw new UserException('Migration connection is not set');
            }
            $connection = $this->migrateConnection;
        }

        foreach ($this->databases as $database) {
            $shareDbName = $database . '_SHARE';

            $this->destinationConnection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                Helper::quoteIdentifier($shareDbName)
            ));

            $this->destinationConnection->query(sprintf(
                'CREATE DATABASE %s FROM SHARE IDENTIFIER(\'%s.%s\');',
                Helper::quoteIdentifier($shareDbName),
                $connection->getAccount(),
                self::MIGRATION_SHARE_PREFIX . $database
            ));
        }
    }
}
