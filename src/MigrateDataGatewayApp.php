<?php

declare(strict_types=1);

namespace ProjectMigrationTool;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\QueryBuilder;
use Keboola\StorageApi\Components;
use Keboola\StorageApi\Options\Components\Configuration;
use Keboola\StorageApi\Options\Components\ListComponentConfigurationsOptions;
use Keboola\StorageApi\WorkspaceLoginType;
use Keboola\StorageApiBranch\Factory\ClientOptions;
use Keboola\StorageApiBranch\Factory\StorageClientPlainFactory;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\Storage\ClientOptionsFactory;
use ProjectMigrationTool\ValueObject\GrantToRole;
use Psr\Log\LoggerInterface;

class MigrateDataGatewayApp
{
    private StorageClientPlainFactory $storageClientFactory;

    private const COMPONENT_ID = 'keboola.app-data-gateway';

    public function __construct(
        private readonly Connection $destinationConnection,
        private readonly LoggerInterface $logger,
        private readonly Config $config,
    ) {
        $clientOptions = new ClientOptions(url: $config->getProjectsUrlStack());
        $this->storageClientFactory = new StorageClientPlainFactory($clientOptions);
    }

    private function generatePublicKey(): string
    {
        $config = [
            'private_key_bits' => 2048,
            'private_key_type' => OPENSSL_KEYTYPE_RSA,
        ];

        $resource = openssl_pkey_new($config);
        if ($resource === false) {
            throw new UserException('Failed to generate RSA key pair');
        }

        $keyDetails = openssl_pkey_get_details($resource);
        if ($keyDetails === false) {
            throw new UserException('Failed to get key details');
        }

        return $keyDetails['key'];
    }

    public function migrate(array $projectsToken): void
    {
        foreach ($projectsToken as $projectToken) {
            $basicClient = $this->storageClientFactory
                ->createClientWrapper(ClientOptionsFactory::createForToken($projectToken))
                ->getBasicClient();
            $verifyToken = $basicClient->verifyToken();

            $this->logger->info(sprintf(
                'Migrating project "%s" [%s]',
                $verifyToken['owner']['name'],
                $verifyToken['owner']['id'],
            ));
            $components = new Components($basicClient);

            $listComponentConfigurationsOptions = new ListComponentConfigurationsOptions();
            $listComponentConfigurationsOptions->setComponentId(self::COMPONENT_ID);
            $configurations = $components->listComponentConfigurations($listComponentConfigurationsOptions);

            if (count($configurations) === 0) {
                continue;
            }

            $workspaces = [];
            foreach ($configurations as $configuration) {
                $database = $configuration['configuration']['parameters']['db']['database'] ?? null;
                $schema = $configuration['configuration']['parameters']['db']['schema'] ?? null;

                if ($database === null || $schema === null) {
                    $this->logger->warning(sprintf(
                        'Skipping configuration "%s" - missing database or schema',
                        $configuration['name'] ?? $configuration['id'],
                    ));
                    continue;
                }

                $this->logger->info(sprintf(
                    'Migrate configuration "%s"',
                    $configuration['name'],
                ));

                if (array_key_exists($schema, $workspaces)) {
                    $this->logger->info(sprintf(
                        'Configuration "%s" uses workspace from another configuration. Updating just config',
                        $configuration['name'],
                    ));
                    $newWorkspace = $workspaces[$schema];
                } else {
                    $this->logger->info(sprintf(
                        'Creating workspace for Configuration "%s"',
                        $configuration['name'],
                    ));

                    $databaseName = preg_replace('/_\d+$/', '', $database);
                    $newWorkspace = $this->createNewWorkspace($components, $configuration);
                    $this->copyWorkspaceData(
                        $databaseName,
                        $schema,
                        $newWorkspace['connection']['schema'],
                    );

                    $workspaces[$schema] = $newWorkspace;
                }

                $this->updateConfiguration($components, $configuration, $newWorkspace);
            }
        }
    }

    private function getWarehouseName(array $warehouseGrants): string
    {
        $selectedWarehouse = array_filter(
            $warehouseGrants,
            fn(GrantToRole $warehouseGrant) => str_ends_with(
                $warehouseGrant->getName(),
                $this->config->getWarehouseSize(),
            ),
        );

        // if no warehouse is selected, return the first one
        if (count($selectedWarehouse) === 0) {
            return current($warehouseGrants)->getName();
        }

        return current($selectedWarehouse)->getName();
    }

    private function createNewWorkspace(Components $components, array $configuration): array
    {
        return $components->createConfigurationWorkspace(
            self::COMPONENT_ID,
            $configuration['id'],
            [
                'publicKey' => $this->generatePublicKey(),
                'useCase' => 'reader',
                'backend' => 'snowflake',
                'loginType' => WorkspaceLoginType::SNOWFLAKE_PERSON_KEYPAIR,
            ],
        );
    }

    private function copyWorkspaceData(string $database, string $oldSchema, string $newSchema): void
    {
        $databaseRole = $this->destinationConnection->getOwnershipRoleOnDatabase($database);
        $this->destinationConnection->useRole($databaseRole);

        $grantsToRole = array_map(
            fn(array $grant) => GrantToRole::fromArray($grant),
            $this->destinationConnection->fetchAll(sprintf(
                'SHOW GRANTS TO ROLE %s',
                QueryBuilder::quoteIdentifier($databaseRole),
            ))
        );
        $warehouseGrants = array_filter(
            $grantsToRole,
            fn(GrantToRole $grant) => $grant->getGrantedOn() === 'WAREHOUSE',
        );

        $this->destinationConnection->useWarehouse($this->getWarehouseName($warehouseGrants));

        $tables = $this->destinationConnection->fetchAll(sprintf(
            'SHOW TABLES IN SCHEMA %s.%s;',
            Helper::quoteIdentifier($database),
            Helper::quoteIdentifier($oldSchema)
        ));

        foreach ($tables as $table) {
            $tableName = $table['name'];
            $this->logger->info(sprintf('Cloning table "%s".', $tableName));
            $this->destinationConnection->query(sprintf(
                'CREATE TABLE %s.%s.%s CLONE %s.%s.%s;',
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($newSchema),
                Helper::quoteIdentifier($tableName),
                Helper::quoteIdentifier($database),
                Helper::quoteIdentifier($oldSchema),
                Helper::quoteIdentifier($tableName),
            ));
        }

        // Validate that all tables were cloned successfully before dropping old schema
        $clonedTables = $this->destinationConnection->fetchAll(sprintf(
            'SHOW TABLES IN SCHEMA %s.%s;',
            Helper::quoteIdentifier($database),
            Helper::quoteIdentifier($newSchema)
        ));

        if (count($clonedTables) !== count($tables)) {
            throw new UserException(sprintf(
                'Failed to clone all tables. Expected %d tables, but found %d in new schema',
                count($tables),
                count($clonedTables)
            ));
        }

        $this->destinationConnection->query(sprintf(
            'DROP SCHEMA %s.%s;',
            Helper::quoteIdentifier($database),
            Helper::quoteIdentifier($oldSchema),
        ));
    }

    private function updateConfiguration(Components $components, array $configuration, array $workspace): void
    {
        $configuration['configuration']['parameters']['db'] = array_merge(
            $configuration['configuration']['parameters']['db'],
            $workspace['connection'],
            [
                'workspaceId' => $workspace['id'],
            ],
        );

        $configurationOption = new Configuration();
        $configurationOption->setComponentId(self::COMPONENT_ID);
        $configurationOption->setConfigurationId($configuration['id']);
        $configurationOption->setConfiguration($configuration['configuration']);
        $components->updateConfiguration($configurationOption);
    }
}
