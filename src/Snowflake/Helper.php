<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\ValueObject\FutureGrantToRole;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\RoleFutureGrants;
use ProjectMigrationTool\ValueObject\RoleGrants;

class Helper
{
    /**
     * Filters out Snowflake internal USER$ system grants from an array of raw grant data.
     * These grants (e.g., USER$KEBOOLA_WORKSPACE_942394159) are internal Snowflake system grants
     * for user-based access control that started appearing around November 2025.
     * They should not be managed by end users and cause structure migration validation to fail.
     *
     * @param array<array<string, mixed>> $grants Raw grant data from SHOW GRANTS queries
     * @return array<array<string, mixed>> Filtered grants without USER$ entries
     */
    public static function filterUserDollarGrants(array $grants): array
    {
        return array_filter($grants, function (array $grant): bool {
            if (isset($grant['name']) && is_string($grant['name']) && str_starts_with($grant['name'], 'USER$')) {
                return false;
            }
            if (isset($grant['role']) && is_string($grant['role']) && str_starts_with($grant['role'], 'USER$')) {
                return false;
            }
            return true;
        });
    }

    /**
     * @param GrantToRole[] $grants
     */
    public static function parseGrantsToObjects(array $grants, Config $config): RoleGrants
    {
        $roleGrants = new RoleGrants();

        foreach ($grants as $grant) {
            switch ($grant->getGrantedOn()) {
                case 'DATABASE':
                    $roleGrants->addDatabaseGrant($grant);
                    break;
                case 'SCHEMA':
                    $roleGrants->addSchemaGrant($grant);
                    break;
                case 'TABLE':
                    $roleGrants->addTableGrant($grant);
                    break;
                case 'ROLE':
                    $roleGrants->addRoleGrant($grant);
                    break;
                case 'ACCOUNT':
                    $roleGrants->addAccountGrant($grant);
                    break;
                case 'WAREHOUSE':
                    $roleGrants->addWarehouseGrant(
                        $grant,
                        $config->getSourceSnowflakeWarehouse(),
                        $config->getTargetSnowflakeWarehouse(),
                    );
                    break;
                case 'USER':
                    $roleGrants->addUserGrant($grant);
                    break;
                case 'VIEW':
                    $roleGrants->addViewGrant($grant);
                    break;
                case 'FUNCTION':
                    $roleGrants->addFunctionGrant($grant);
                    break;
                case 'PROCEDURE':
                    $roleGrants->addProcedureGrant($grant);
                    break;
                default:
                    $roleGrants->addOtherGrant($grant);
            }
        }

        return $roleGrants;
    }

    /**
     * @param FutureGrantToRole[] $futureGrants
     */
    public static function parseFutureGrantsToObjects(array $futureGrants): RoleFutureGrants
    {
        $roleFutureGrants = new RoleFutureGrants();

        foreach ($futureGrants as $futureGrant) {
            switch ($futureGrant->getGrantOn()) {
                case 'TABLE':
                    $roleFutureGrants->addTableGrant($futureGrant);
                    break;
                default:
                    $roleFutureGrants->addOtherGrant($futureGrant);
            }
        }

        return $roleFutureGrants;
    }

    public static function generateRandomString(int $length = 10): string
    {
        $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $randomString = '';
        for ($i = 0; $i < $length; $i++) {
            $randomString .= $characters[random_int(0, strlen($characters) - 1)];
        }
        return $randomString;
    }

    /**
     * @param GrantToRole[]|FutureGrantToRole[] $schemasGrants
     * @return GrantToRole[]|FutureGrantToRole[]
     */
    public static function filterSchemaGrants(string $database, string $schemaName, array $schemasGrants): array
    {
        return array_filter(
            $schemasGrants,
            function (GrantToRole|FutureGrantToRole $v) use ($database, $schemaName) {
                $validSchema = Helper::generateFormattedQuoteCombinations($database, $schemaName);
                return in_array($v->getName(), $validSchema);
            }
        );
    }

    public static function generateFormattedQuoteCombinations(string ...$inputArray): array
    {
        $ret = [];
        for ($b = 0; $b < pow(count($inputArray), 2); $b++) {
            $binNum = str_pad(decbin($b), count($inputArray), '0', STR_PAD_LEFT);
            if (strlen($binNum) === count($inputArray)) {
                $variables = [];
                foreach ($inputArray as $k => $item) {
                    $variables[] = $binNum[$k] === '1' ? Helper::quoteIdentifier($item) : $item;
                }
                $ret[] = sprintf(
                    implode('.', array_pad([], count($inputArray), '%s')),
                    ...$variables
                );
            }
        }
        return $ret;
    }

    public static function quoteIdentifier(string $str): string
    {
        if (preg_match('/^".+"$/', $str)) {
            return $str;
        }
        return QueryBuilder::quoteIdentifier($str);
    }
}
