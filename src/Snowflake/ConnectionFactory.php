<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\Connection;

class ConnectionFactory
{
    public static function create(string $typeConnection): Connection
    {
        $options = [
            'host' => getenv(sprintf('SNOWFLAKE_%s_ACCOUNT_HOST', strtoupper($typeConnection))),
            'user' => getenv(sprintf('SNOWFLAKE_%s_ACCOUNT_USERNAME', strtoupper($typeConnection))),
            'password' => getenv(sprintf('SNOWFLAKE_%s_ACCOUNT_PASSWORD', strtoupper($typeConnection))),
            'warehouse' => getenv(sprintf('SNOWFLAKE_%s_ACCOUNT_WAREHOUSE', strtoupper($typeConnection))),
        ];

        return new Connection($options);
    }
}
