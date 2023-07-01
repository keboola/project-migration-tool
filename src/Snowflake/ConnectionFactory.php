<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Psr\Log\LoggerInterface;

class ConnectionFactory
{
    public static function create(
        string $host,
        string $user,
        string $password,
        string $warehouse,
        string $role,
        ?LoggerInterface $logger = null
    ): Connection {
        $options = [
            'host' => $host,
            'user' => $user,
            'password' => $password,
            'warehouse' => $warehouse,
            'clientSessionKeepAlive' => true,
        ];
        return new Connection($options, $role, $logger);
    }
}
