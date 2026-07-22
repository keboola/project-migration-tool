<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Storage;

use Keboola\StorageApiBranch\Factory\AuthType;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Storage\ClientOptionsFactory;

class ClientOptionsFactoryTest extends TestCase
{
    public function testCreateForTokenSetsToken(): void
    {
        $options = ClientOptionsFactory::createForToken('my-token-123');

        self::assertSame('my-token-123', $options->getToken());
    }

    public function testCreateForTokenUsesStorageTokenAuthType(): void
    {
        $options = ClientOptionsFactory::createForToken('my-token-123');

        self::assertSame(AuthType::STORAGE_TOKEN, $options->getAuthType());
    }

    public function testCreateForTokenGeneratesRunId(): void
    {
        $options = ClientOptionsFactory::createForToken('my-token-123');

        self::assertStringStartsWith('run-', (string) $options->getRunId());
    }
}
