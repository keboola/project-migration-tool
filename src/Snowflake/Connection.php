<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection as AdapterConnection;
use Keboola\SnowflakeDbAdapter\QueryBuilder;

class Connection extends AdapterConnection
{
    private ?string $region = null;

    private ?string $account = null;

    private ?string $actualRole = null;

    private string $defaultRole;

    private array $roleWarehouses = [];

    public function __construct(array $options, string $defaultRole)
    {
        $this->defaultRole = $defaultRole;
        parent::__construct($options);
    }

    public function useRole(string $roleName): void
    {
        if ($roleName === 'ACCOUNTADMIN') {
            $roleName = $this->defaultRole;
        }
        if ($roleName === $this->actualRole) {
            return;
        }
        $this->query(sprintf('USE ROLE %s;', QueryBuilder::quoteIdentifier($roleName)));

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
            QueryBuilder::quoteIdentifier(current($this->roleWarehouses[$role]))
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

    public function createRole(array $role): void
    {
        assert($role['privilege'] === 'OWNERSHIP');

        if (isset($role['granted_by'])) {
            $this->useRole($role['granted_by']);
        }

        $this->query(sprintf('CREATE ROLE IF NOT EXISTS %s', $role['name']));

        $this->grantRoleToUser((string) getenv('SNOWFLAKE_DESTINATION_ACCOUNT_USERNAME'), $role['name']);
    }

    public function grantRoleToUser(string $user, string $role): void
    {
        $this->query(sprintf(
            'GRANT ROLE %s TO USER %s',
            QueryBuilder::quoteIdentifier($role),
            QueryBuilder::quoteIdentifier($user)
        ));
    }

    public function assignGrantToRole(array $grant): void
    {
        $this->useRole($grant['granted_by']);

        if ($grant['privilege'] === 'USAGE' && $grant['granted_on'] === 'WAREHOUSE') {
            $this->roleWarehouses[$grant['grantee_name']][] = $grant['name'];
        }

        if ($grant['privilege'] === 'USAGE' && $grant['granted_on'] === 'ROLE') {
            $this->query(sprintf(
                'GRANT %s %s TO %s %s %s',
                $grant['granted_on'],
                $grant['name'],
                $grant['granted_to'],
                $grant['grantee_name'],
                $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
            ));
        } else {
            $this->query(sprintf(
                'GRANT %s ON %s %s TO %s %s %s',
                $grant['privilege'],
                $grant['granted_on'],
                $grant['granted_on'] !== 'ACCOUNT' ? $grant['name'] : '',
                $grant['granted_to'],
                $grant['grantee_name'],
                $grant['grant_option'] === 'true' ? 'WITH GRANT OPTION' : '',
            ));
        }
    }

    public function getOwnershipRoleOnDatabase(string $database): string
    {
        $grantsOnDatabase = $this->fetchAll(sprintf(
            'SHOW GRANTS ON DATABASE %s;',
            QueryBuilder::quoteIdentifier($database)
        ));

        $ownershipOnDatabase = array_filter($grantsOnDatabase, fn($v) => $v['privilege'] === 'OWNERSHIP');
        assert(count($ownershipOnDatabase) === 1);

        return current($ownershipOnDatabase)['grantee_name'];
    }
}
