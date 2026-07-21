<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Storage;

use Keboola\StorageApiBranch\Factory\AuthType;
use Keboola\StorageApiBranch\Factory\ClientOptions;

class ClientOptionsFactory
{
    public static function createForToken(string $projectToken): ClientOptions
    {
        // StorageClientPlainFactory does not derive a token, auth type or run ID from an HTTP request
        // (unlike StorageClientRequestFactory), so set them explicitly here. The run-* prefix mirrors
        // the run ID the request factory used to generate, keeping Storage job tracing consistent.
        return new ClientOptions(
            token: $projectToken,
            runId: uniqid('run-'),
            authType: AuthType::STORAGE_TOKEN,
        );
    }
}
