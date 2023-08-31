<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\SnowflakeDbAdapterException;
use Psr\Log\LoggerInterface;

class ConnectionFactory
{
    public static function create(
        string $host,
        string $user,
        string $password,
        string $warehouse,
        string $role,
        string $connectionName,
        ?LoggerInterface $logger = null,
    ): Connection {
        $options = [
            'host' => $host,
            'user' => $user,
            'password' => $password,
            'warehouse' => $warehouse,
            'clientSessionKeepAlive' => true,
        ];
        try {
            return new Connection($options, $role, $logger);
        } catch (SnowflakeDbAdapterException $e) {
            if (str_contains($e->getMessage(), 'Incorrect username or password')) {
                throw new UserException(sprintf(
                    'Incorrect username or password for %s database.',
                    $connectionName
                ));
            } else {
                throw $e;
            }
        }
    }
}
