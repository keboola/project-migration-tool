<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\QueryBuilder;

class Helper
{
    public static function parseGrantsToObjects(array $ownershipOnObjectsInDatabase): array
    {
        $tmp = [
            'grants' => [
                'account' => [],
                'databases' => [],
                'schemas' => [],
                'tables' => [],
                'roles' => [],
                'warehouse' => [],
                'user' => [],
                'other' => [],
            ],
            'futureGrants' => [
                'tables' => [],
                'other' => [],
            ],
        ];
        foreach ($ownershipOnObjectsInDatabase as $role) {
            foreach ($role['assignedGrants'] as $assignedGrant) {
                switch ($assignedGrant['granted_on']) {
                    case 'DATABASE':
                        $tmp['grants']['databases'][] = $assignedGrant;
                        break;
                    case 'SCHEMA':
                        $tmp['grants']['schemas'][] = $assignedGrant;
                        break;
                    case 'TABLE':
                        $tmp['grants']['tables'][] = $assignedGrant;
                        break;
                    case 'ROLE':
                        $tmp['grants']['roles'][] = $assignedGrant;
                        break;
                    case 'ACCOUNT':
                        $tmp['grants']['account'][] = $assignedGrant;
                        break;
                    case 'WAREHOUSE':
                        $tmp['grants']['warehouse'][] = $assignedGrant;
                        break;
                    case 'USER':
                        $tmp['grants']['user'][] = $assignedGrant;
                        break;
                    default:
                        $tmp['grants']['other'][] = $assignedGrant;
                }
            }
            foreach ($role['assignedFutureGrants'] as $assignedFutureGrant) {
                switch ($assignedFutureGrant['grant_on']) {
                    case 'TABLE':
                        $assignedFutureGrant['name'] = preg_replace('/.<TABLE>$/', '', $assignedFutureGrant['name']);
                        $tmp['futureGrants']['tables'][] = $assignedFutureGrant;
                        break;
                    default:
                        $tmp['futureGrants']['other'][] = $assignedFutureGrant;
                }
            }
        }

        return $tmp;
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

    public static function filterSchemaGrants(string $database, string $schemaName, array $schemasGrants): array
    {
        return array_filter(
            $schemasGrants,
            function (array $v) use ($database, $schemaName) {
                $validSchema = [
                    sprintf('%s.%s', $database, $schemaName),
                    sprintf('%s.%s', $database, QueryBuilder::quoteIdentifier($schemaName)),
                    sprintf('%s.%s', QueryBuilder::quoteIdentifier($database), $schemaName),
                    sprintf(
                        '%s.%s',
                        QueryBuilder::quoteIdentifier($database),
                        QueryBuilder::quoteIdentifier($schemaName)
                    ),
                ];
                return in_array($v['name'], $validSchema);
            }
        );
    }
}
