<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\Connection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class Command
{

    private const MIGRATION_SHARE_PREFIX = 'MIGRATION_SHARE_';

    private const SKIP_CLONE_SCHEMAS = [
        'INFORMATION_SCHEMA',
        'PUBLIC',
    ];

    public static function useRole(Connection $connection, string $role): void
    {
        $connection->query(sprintf('USE ROLE "%s";', $role));
    }

    public static function getRegion(Connection $connection): string
    {
        $region = $connection->fetchAll('SELECT CURRENT_REGION() AS "region";');

        return $region[0]['region'];
    }

    public static function getAccount(Connection $connection): string
    {
        $account = $connection->fetchAll('SELECT CURRENT_ACCOUNT() AS "account";');

        return $account[0]['account'];
    }

    public static function createShare(Connection $connection, array $databases, string $destinationAccount): void
    {
        foreach ($databases as $database) {
            $shareName = sprintf('%s%s', self::MIGRATION_SHARE_PREFIX, strtoupper($database));

            $connection->query(sprintf(
                'DROP SHARE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'CREATE SHARE %s;',
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON DATABASE %s TO SHARE %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT USAGE ON ALL SCHEMAS IN DATABASE %s TO SHARE %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'GRANT SELECT ON ALL TABLES IN DATABASE %s TO SHARE %s;',
                QueryBuilder::quoteIdentifier($database),
                QueryBuilder::quoteIdentifier($shareName)
            ));

            $connection->query(sprintf(
                'ALTER SHARE %s ADD ACCOUNT=%s;',
                QueryBuilder::quoteIdentifier($shareName),
                $destinationAccount //RL74503.KEBOOLA_BYODB_MIGRATION_APP_3
            ));
        }
    }

    public static function createDatabasesFromShares(
        Connection $connection,
        array $databases,
        string $sourceAccount
    ): void {
        foreach ($databases as $database) {
            $shareDbName = $database . '_SHARE';

            $connection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($shareDbName)
            ));

            $connection->query(sprintf(
                'CREATE DATABASE %s FROM SHARE IDENTIFIER(\'%s.%s\');',
                QueryBuilder::quoteIdentifier($shareDbName),
                $sourceAccount,
                self::MIGRATION_SHARE_PREFIX.$database
            ));
        }
    }

    public static function cloneDatabaseFromShared(Connection $connection, array $databases): void
    {
        foreach ($databases as $database) {
            $shareDbName = $database . '_SHARE';

            $connection->query(sprintf(
                'DROP DATABASE IF EXISTS %s;',
                QueryBuilder::quoteIdentifier($database)
            ));

            $connection->query(sprintf(
                'CREATE DATABASE %s;',
                QueryBuilder::quoteIdentifier($database)
            ));

            $schemas = $connection->fetchAll(sprintf(
                'SHOW SCHEMAS IN DATABASE %s;',
                QueryBuilder::quoteIdentifier($shareDbName)
            ));

            foreach ($schemas as $k => $schema) {
                if (in_array($schema['name'], self::SKIP_CLONE_SCHEMAS)) {
                    continue;
                }
                $schemaName = $schema['name'];

                $connection->query(sprintf(
                    'CREATE SCHEMA %s.%s;',
                    QueryBuilder::quoteIdentifier($database),
                    QueryBuilder::quoteIdentifier($schemaName)
                ));

                $tables = $connection->fetchAll(sprintf(
                    'SHOW TABLES IN SCHEMA %s.%s;',
                    QueryBuilder::quoteIdentifier($shareDbName),
                    QueryBuilder::quoteIdentifier($schemaName)
                ));

                foreach ($tables as $table) {
                    $tableName = $table['name'];

                    $connection->query(sprintf(
                        'CREATE TABLE %s.%s.%s AS SELECT * FROM %s.%s.%s;',
                        QueryBuilder::quoteIdentifier($database),
                        QueryBuilder::quoteIdentifier($schemaName),
                        QueryBuilder::quoteIdentifier($tableName),
                        QueryBuilder::quoteIdentifier($shareDbName),
                        QueryBuilder::quoteIdentifier($schemaName),
                        QueryBuilder::quoteIdentifier($tableName),
                    ));
                }

                var_dump($schemaName);
                if ($k > 9) {
                    exit();
                }
            }
        }
    }

    public static function exportUsersAndRoles(Connection $sourceSnflkConnection): array
    {
        return [];
    }
}
