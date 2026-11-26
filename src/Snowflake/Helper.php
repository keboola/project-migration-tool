<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\QueryBuilder;
use ProjectMigrationTool\Configuration\Config;
use ProjectMigrationTool\ValueObject\FutureGrantToRole;
use ProjectMigrationTool\ValueObject\GrantToRole;
use ProjectMigrationTool\ValueObject\GrantToUser;
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
     * This filter is intended for use with SHOW GRANTS results from sourceConnection only,
     * filtering based on the 'name' field.
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
            return true;
        });
    }

    /**
     * LIKE in SHOW commands ignores case and treats "_" and "%" as wildcards,
     * so it can return objects with a different name.
     *
     * @param array<array<string, string>> $rows
     * @return list<array<string, string>>
     */
    public static function filterRowsByExactName(array $rows, string $name): array
    {
        return array_values(array_filter($rows, fn(array $row): bool => ($row['name'] ?? null) === $name));
    }

    /**
     * A role is revoked only after every remaining role it granted, because revoking a role
     * runs as its grantor. Otherwise, and for cycles, the reverse of the given order is kept.
     *
     * @param GrantToUser[] $grants
     * @return GrantToUser[]
     */
    public static function orderUserGrantsForRevoke(array $grants): array
    {
        $remaining = array_values(array_reverse($grants));
        $ordered = [];
        while ($remaining !== []) {
            $next = 0;
            foreach ($remaining as $index => $grant) {
                if (!self::isGrantorOfAny($grant->getRole(), $remaining, $index)) {
                    $next = $index;
                    break;
                }
            }
            $ordered[] = $remaining[$next];
            array_splice($remaining, $next, 1);
        }
        return $ordered;
    }

    /**
     * @param GrantToUser[] $grants
     */
    private static function isGrantorOfAny(string $role, array $grants, int $skipIndex): bool
    {
        foreach ($grants as $index => $grant) {
            if ($index !== $skipIndex && $grant->getGrantedBy() === $role) {
                return true;
            }
        }
        return false;
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

    public static function generateFormattedQuoteCombinations(string ...$inputArray): array
    {
        $ret = [];
        for ($b = 0; $b < 2 ** count($inputArray); $b++) {
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
