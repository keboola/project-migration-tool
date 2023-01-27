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
            'user' => [],
            'other' => [],
        ];
        foreach ($ownershipOnObjectsInDatabase as $role) {
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
                    case 'USER':
                        $tmp['user'][] = $assignedGrant;
                        break;
                    default:
                        $tmp['other'][] = $assignedGrant;
                }
            }
        }

        return $tmp;
    }

    public static function generateRandomString(int $length = 10): string
    {
        $characters = '0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ';
        $charactersLength = strlen($characters);
        $randomString = '';
        for ($i = 0; $i < $length; $i++) {
            $randomString .= $characters[rand(0, $charactersLength - 1)];
        }
        return $randomString;
    }
}
