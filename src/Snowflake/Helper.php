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

    /**
     * Checks if a schema name belongs to a dev branch.
     * Dev branch schemas follow the pattern: {branchId}_{bucketId}
     * where branchId is a numeric value and bucketId starts with "in." or "out."
     *
     * Examples of dev branch schemas:
     * - 14107_in.c-my-bucket
     * - 14107_out.c-my-bucket
     *
     * @param string $schemaName The schema name to check
     * @return bool True if the schema belongs to a dev branch
     */
    public static function isDevBranchSchema(string $schemaName): bool
    {
        // Dev branch schemas have pattern: {numericBranchId}_{bucketId}
        // where bucketId starts with "in." or "out."
        return (bool) preg_match('/^\d+_(in\.|out\.)/', $schemaName);
    }

    /**
     * Checks if a role name belongs to a workspace (including dev branch workspaces).
     * Workspace roles follow these patterns:
     * - Default branch: KEBOOLA_WORKSPACE_{workspaceId} or SAPI_WORKSPACE_{workspaceId}
     * - Dev branch: KEBOOLA_{branchId}_WORKSPACE_{workspaceId}
     *
     * @param string $roleName The role name to check
     * @return bool True if the role belongs to a workspace
     */
    public static function isWorkspaceRole(string $roleName): bool
    {
        // Match both default branch and dev branch workspace patterns:
        // - KEBOOLA_WORKSPACE_123456 (default branch)
        // - KEBOOLA_14107_WORKSPACE_123456 (dev branch with branchId 14107)
        // - SAPI_WORKSPACE_123456 (legacy default branch)
        // - sapi_WORKSPACE_123456 (legacy lowercase)
        return (bool) preg_match('/^(KEBOOLA|SAPI|sapi)_(\d+_)?WORKSPACE_/', $roleName);
    }
}
