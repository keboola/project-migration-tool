<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Connection as AdapterConnection;
use ProjectMigrationTool\Exception\NoWarehouseException;
use ProjectMigrationTool\ValueObject\FutureGrantToRole;
use ProjectMigrationTool\ValueObject\GrantToRole;
use Psr\Log\LoggerInterface;
use Throwable;

class Connection extends AdapterConnection
{
    private ?string $region = null;

    private ?string $account = null;

    private ?string $actualRole = null;

    private string $defaultRole;

    private ?LoggerInterface $logger;

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

    public function useWarehouse(string $warehouse): void
    {
        $this->query(sprintf(
            'USE WAREHOUSE %s;',
            Helper::quoteIdentifier($warehouse)
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

    public function createRole(GrantToRole $grant, string $userToGrantRoleTo): void
    {
        assert($grant->getPrivilege() === 'OWNERSHIP');

        if (!empty($grant->getGrantedBy())) {
            $this->useRole($grant->getGrantedBy());
        }

        $this->query(sprintf('CREATE ROLE IF NOT EXISTS %s', Helper::quoteIdentifier($grant->getName())));

        $this->grantRoleToUser($userToGrantRoleTo, $grant->getName());
    }

    public function grantRoleToUser(string $user, string $role): void
    {
        $this->query(sprintf(
            'GRANT ROLE %s TO USER %s',
            Helper::quoteIdentifier($role),
            Helper::quoteIdentifier($user)
        ));
    }

    public function assignGrantToRole(GrantToRole $grant): void
    {
        if ($grant->getGrantedOn() === 'SCHEMA' &&
            (in_array(
                $grant->getPrivilege(),
                ['CREATE SNOWFLAKE.CORE.BUDGET', 'CREATE BUDGET', 'CREATE ANOMALY_DETECTION', 'CREATE FORECAST']
            ))
        ) {
            // CREATE SNOWFLAKE.CORE.BUDGET
            // CREATE BUDGET is not supported in Snowflake
            // CREATE ANOMALY_DETECTION likewise
            // CREATE FORECAST likewise
            return;
        }

        $this->useRole($grant->getGrantedBy());

        $isWarehouseGrant = false;
        if ($grant->getPrivilege() === 'USAGE' && $grant->getGrantedOn() === 'WAREHOUSE') {
            $isWarehouseGrant = true;
        }

        if ($grant->getPrivilege() === 'USAGE' && $grant->getGrantedOn() === 'ROLE') {
            $query = sprintf(
                'GRANT %s %s TO %s %s %s',
                $grant->getGrantedOn(),
                $grant->getName(),
                $grant->getGrantedTo(),
                Helper::quoteIdentifier($grant->getGranteeName()),
                $grant->getGrantOption() === 'true' ? 'WITH GRANT OPTION' : '',
            );
        } else {
            $query = sprintf(
                'GRANT %s ON %s %s TO %s %s %s',
                $grant->getPrivilege(),
                $grant->getGrantedOn(),
                $grant->getGrantedOn() !== 'ACCOUNT' ? $grant->getName() : '',
                $grant->getGrantedTo(),
                Helper::quoteIdentifier($grant->getGranteeName()),
                $grant->getGrantOption() === 'true' ? 'WITH GRANT OPTION' : '',
            );
        }

        try {
            $this->query($query);
        } catch (Throwable $e) {
            if (!$isWarehouseGrant || $this->logger === null) {
                if (in_array($grant->getGrantedOn(), ['DATABASE', 'SCHEMA'])) {
                    $this->failedGrants[] = $grant;
                } else {
                    throw $e;
                }
            }
            $this->logger?->info(sprintf(
                'Warning: Failed grant query "%s" with role "%s"',
                $query,
                $grant->getGrantedBy()
            ));
        }
    }

    public function assignFutureGrantToRole(FutureGrantToRole $schemaFutureGrant): void
    {
        switch ($schemaFutureGrant->getGrantOn()) {
            case 'TABLE':
                $this->query(sprintf(
                    'GRANT %s ON FUTURE TABLES IN SCHEMA %s TO ROLE %s %s',
                    $schemaFutureGrant->getPrivilege(),
                    $schemaFutureGrant->getName(),
                    Helper::quoteIdentifier($schemaFutureGrant->getGranteeName()),
                    $schemaFutureGrant->getGrantOption() === 'true' ? 'WITH GRANT OPTION' : '',
                ));
                break;
            default:
                throw new UserException('Unknown future grant on ' . $schemaFutureGrant->getGrantOn());
        }
    }

    public function getOwnershipRoleOnDatabase(string $database): string
    {
        /** @var GrantToRole[] $grantsOnDatabase */
        $grantsOnDatabase = array_map(
            fn(array $grant) => GrantToRole::fromArray($grant),
            $this->fetchAll(sprintf(
                'SHOW GRANTS ON DATABASE %s;',
                Helper::quoteIdentifier($database)
            ))
        );

        $ownershipOnDatabase = array_filter($grantsOnDatabase, fn($v) => $v->getPrivilege() === 'OWNERSHIP');
        assert(count($ownershipOnDatabase) === 1);

        return current($ownershipOnDatabase)->getGranteeName();
    }

    public function getFailedGrants(): array
    {
        return $this->failedGrants;
    }
}
