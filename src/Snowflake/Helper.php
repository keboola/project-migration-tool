<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

class Helper
{
    public static function parseGrantsToObjects(array $ownershipOnObjectsInDatabase): array
    {
        $tmp = [
            'account' =>[],
            'databases' => [],
            'schemas' => [],
            'tables' => [],
            'roles' => [],
            'warehouse' => [],
            'other' => [],
        ];
        foreach ($ownershipOnObjectsInDatabase['roles'] as $role) {
            foreach ($role['assignedGrants'] as $assignedGrant) {
                switch ($assignedGrant['granted_on']) {
                    case 'DATABASE':
                        $tmp['databases'][] = $assignedGrant;
                        break;
                    case 'SCHEMA':
                        $tmp['schemas'][] = $assignedGrant;
                        break;
                    case 'TABLE':
                        $tmp['tables'][] = $assignedGrant;
                        break;
                    case 'ROLE':
                        $tmp['roles'][] = $assignedGrant;
                        break;
                    case 'ACCOUNT':
                        $tmp['account'][] = $assignedGrant;
                        break;
                    case 'WAREHOUSE':
                        $tmp['warehouse'][] = $assignedGrant;
                        break;
                    default:
                        $tmp['other'][] = $assignedGrant;
                }
            }
        }

        return $tmp;
    }
}
