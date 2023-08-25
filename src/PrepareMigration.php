<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use ProjectMigrationTool\Snowflake\Helper;

class PrepareMigration
{
    private const MIGRATION_SHARE_PREFIX = 'MIGRATION_SHARE_';

    public function __construct(
        readonly array $databases,
        readonly Snowflake\Connection $sourceConnection,
        readonly Snowflake\Connection $destinationConnection,
        readonly ?Snowflake\Connection $migrateConnection = null,
    ) {
    }

    public function createReplication(): void
    {
        if ($this->sourceConnection->getRegion() === $this->destinationConnection->getRegion()) {
            return;
        }
        if (!$this->migrateConnection) {
            throw new UserException('Migration connection is not set');
        }
        foreach ($this->databases as $database) {
            //            Allow replication on source database
            $this->sourceConnection->query(sprintf(
                'ALTER DATABASE %s ENABLE REPLICATION TO ACCOUNTS %s.%s;',
                Helper::quoteIdentifier($database),
                $this->migrateConnection->getRegion(),
                $this->migrateConnection->getAccount()
            ));
            $this->sourceConnection->useRole($this->sourceConnection->getOwnershipRoleOnDatabase($database));

            //            Waiting for previous SQL query
            sleep(1);

            //            Migration database sqls
            $this->migrateConnection->query(sprintf(
                'CREATE DATABASE IF NOT EXISTS %s AS REPLICA OF %s.%s.%s;',
                Helper::quoteIdentifier($database),
                $this->sourceConnection->getRegion(),
                $this->sourceConnection->getAccount(),
                Helper::quoteIdentifier($database)
            ));

            $this->migrateConnection->query(sprintf(
                'USE DATABASE %s',
                Helper::quoteIdentifier($database)
            ));

            $this->migrateConnection->query('USE SCHEMA PUBLIC');

            //            Create and use warehouse for replicate data
            $sql = <<<SQL
CREATE WAREHOUSE IF NOT EXISTS "migrate"
    WITH WAREHOUSE_SIZE = 'Small'
        WAREHOUSE_TYPE = 'STANDARD'
        AUTO_SUSPEND = 300
        AUTO_RESUME = true
;
SQL;
            $this->migrateConnection->query($sql);

            $this->migrateConnection->query('USE WAREHOUSE "migrate";');

            //            Run replicate of data
            $this->migrateConnection->query(sprintf(
                'ALTER DATABASE %s REFRESH',
                Helper::quoteIdentifier($database)
            ));
        }
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
