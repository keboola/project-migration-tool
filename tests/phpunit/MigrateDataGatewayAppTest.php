<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use Keboola\StorageApiBranch\Factory\AuthType;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\MigrateDataGatewayApp;

class MigrateDataGatewayAppTest extends TestCase
{
    public function testCreateClientOptionsForTokenSetsToken(): void
    {
        $options = MigrateDataGatewayApp::createClientOptionsForToken('my-token-123');

        self::assertSame('my-token-123', $options->getToken());
    }

    public function testCreateClientOptionsForTokenUsesStorageTokenAuthType(): void
    {
        $options = MigrateDataGatewayApp::createClientOptionsForToken('my-token-123');

        self::assertSame(AuthType::STORAGE_TOKEN, $options->getAuthType());
    }

    public function testCreateClientOptionsForTokenGeneratesRunId(): void
    {
        $options = MigrateDataGatewayApp::createClientOptionsForToken('my-token-123');

        self::assertStringStartsWith('run-', (string) $options->getRunId());
    }
}
