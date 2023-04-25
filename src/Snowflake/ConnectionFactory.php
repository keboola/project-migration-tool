<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

class ConnectionFactory
{
    public static function create(
        string $host,
        string $user,
        string $password,
        string $warehouse,
        string $role
    ): Connection {
        $options = [
            'host' => $host,
            'user' => $user,
            'password' => $password,
            'warehouse' => $warehouse,
        ];

        return new Connection($options, $role);
    }
}
