<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Storage;

use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class ClientOptionsFactory
{
    public static function createForToken(string $projectToken): ClientOptions
    {
        // Authenticate as the project's Storage token: STORAGE_TOKEN sends it in the
        // X-StorageApi-Token header, and the generated run-* run ID tags the Storage jobs
        // this migration triggers so they remain traceable.
        return new ClientOptions(
            token: $projectToken,
            runId: uniqid('run-'),
            authType: AuthType::STORAGE_TOKEN,
        );
    }
}
