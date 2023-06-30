<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection as AdapterConnection;
use Psr\Log\LoggerInterface;
use Throwable;

class Connection extends AdapterConnection
{
    private ?string $region = null;

    private ?string $account = null;

    private ?string $actualRole = null;

    private string $defaultRole;

    private ?LoggerInterface $logger;

    private array $roleWarehouses = [];

    private array $failedGrants = [];

    public function __construct(array $options, string $defaultRole, ?LoggerInterface $logger = null)
    {
        $this->defaultRole = $defaultRole;
        $this->logger = $logger;
        parent::__construct($options);
    }

    public function query(string $sql, array $bind = []): void
    {
        if ($this->getCurrentRole() === 'ACCOUNTADMIN' && str_starts_with($sql, 'DROP')) {
            throw new UserException('Run DROP query on ACCOUNTADMIN role is not allowed.');
        }
        parent::query($sql, $bind);
    }

    public function useRole(string $roleName): void
    {
        if ($roleName === 'ACCOUNTADMIN') {
            $roleName = $this->defaultRole;
        }
        if ($roleName === $this->actualRole) {
            return;
        }
        $this->query(sprintf('USE ROLE %s;', Helper::quoteIdentifier($roleName)));

        $this->actualRole = $roleName;
    }

    public function useWarehouse(string $role): void
    {
        if (empty($this->roleWarehouses[$role])) {
            throw new UserException(sprintf(
                'The role "%s" cannot use any warehouses',
                $role
            ));
        }

        $this->query(sprintf(
            'USE WAREHOUSE %s;',
            Helper::quoteIdentifier(current($this->roleWarehouses[$role]))
        ));
    }

    public function getRegion(): string
    {
        if (!$this->region) {
            $region = $this->fetchAll('SELECT CURRENT_REGION() AS "region";');
            $this->region = $region[0]['region'];
        }

        return $this->region;
    }

    public function getAccount(): string
    {
        if (!$this->account) {
            $account = $this->fetchAll('SELECT CURRENT_ACCOUNT() AS "account";');

            $this->account = $account[0]['account'];
        }

        return $this->account;
    }

    public function getCurrentRole(): string
    {
        $role = $this->fetchAll('SELECT CURRENT_ROLE() AS "role";');

        return $role[0]['role'];
    }

    public function createRole(array $role, string $userToGrantRoleTo): void
    {
        assert($role['privilege'] === 'OWNERSHIP');

        if (isset($role['granted_by'])) {
            $this->useRole($role['granted_by']);
        }

        $this->query(sprintf('CREATE ROLE IF NOT EXISTS %s', Helper::quoteIdentifier($role['name'])));

        $this->grantRoleToUser($userToGrantRoleTo, $role['name']);
    }

    public function grantRoleToUser(string $user, string $role): void
    {
        $this->query(sprintf(
            'GRANT ROLE %s TO USER %s',
            Helper::quoteIdentifier($role),
            Helper::quoteIdentifier($user)
        ));
    }

    public function assignGrantToRole(array $grant): void
    {
        if ($grant['granted_on'] === 'SCHEMA' &&
            ($grant['privilege'] === 'CREATE BUDGET' ||  $grant['privilege'] === 'CREATE ANOMALY_DETECTION')) {
            // CREATE BUDGET is not supported in Snowflake
            // CREATE ANOMALY_DETECTION likewise
            return;
        }

        $this->useRole($grant['granted_by']);

        $isWarehouseGrant = false;
        if ($grant['privilege'] === 'USAGE' && $grant['granted_on'] === 'WAREHOUSE') {
            $this->roleWarehouses[$grant['grantee_name']][] = $grant['name'];
            $isWarehouseGrant = true;
        }

        if ($grant['privilege'] === 'USAGE' && $grant['granted_on'] === 'ROLE') {
            $query = sprintf(
                'GRANT %s %s TO %s %s %s',
                $grant['granted_on'],
                $grant['name'],
                $grant['granted_to'],
                Helper::quoteIdentifier($grant['grantee_name']),
                $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
            );
        } else {
            $query = sprintf(
                'GRANT %s ON %s %s TO %s %s %s',
                $grant['privilege'],
                $grant['granted_on'],
                $grant['granted_on'] !== 'ACCOUNT' ? $grant['name'] : '',
                $grant['granted_to'],
                Helper::quoteIdentifier($grant['grantee_name']),
                $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
            );
        }

        try {
            $this->query($query);
        } catch (Throwable $e) {
            if (!$isWarehouseGrant || $this->logger === null) {
                if (in_array($grant['granted_on'], ['DATABASE', 'SCHEMA'])) {
                    $this->failedGrants[] = $grant;
                } else {
                    throw $e;
                }
            }
            $this->logger?->warning(sprintf(
                'Failed query "%s" with role "%s"',
                $query,
                $grant['granted_by']
            ));
        }
    }

    public function assignFutureGrantToRole(array $schemaFutureGrant): void
    {
        switch ($schemaFutureGrant['grant_on']) {
            case 'TABLE':
                $this->query(sprintf(
                    'GRANT %s ON FUTURE TABLES IN SCHEMA %s TO ROLE %s %s',
                    $schemaFutureGrant['privilege'],
                    $schemaFutureGrant['name'],
                    Helper::quoteIdentifier($schemaFutureGrant['grantee_name']),
                    $schemaFutureGrant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
                ));
                break;
            default:
                throw new UserException('Unknown future grant on ' . $schemaFutureGrant['grant_on']);
        }
    }

    public function getOwnershipRoleOnDatabase(string $database): string
    {
        $grantsOnDatabase = $this->fetchAll(sprintf(
            'SHOW GRANTS ON DATABASE %s;',
            Helper::quoteIdentifier($database)
        ));

        $ownershipOnDatabase = array_filter($grantsOnDatabase, fn($v) => $v['privilege'] === 'OWNERSHIP');
        assert(count($ownershipOnDatabase) === 1);

        return current($ownershipOnDatabase)['grantee_name'];
    }

    public function getFailedGrants(): array
    {
        return $this->failedGrants;
    }
}
