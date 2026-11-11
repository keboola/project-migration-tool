<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests;

use RuntimeException;

/**
 * Answers the SHOW commands used by the post-migration check from in-memory data.
 */
class FakeSnowflakeAccount
{
    /** @var array<int, array<string, string>> */
    public array $roles = [];

    /** @var string[] */
    public array $databases = ['DB1', 'DB1_SHARE'];

    /**
     * Roles granted to the migration users; by default the state before cleanup.
     *
     * @var array<string, string[]>
     */
    public array $rolesOfUser = [
        'SOURCE_MIGRATE' => ['SOURCE_MAIN_MIGRATE', 'KEBOOLA_1'],
        'TARGET_MIGRATE' => ['TARGET_MAIN_MIGRATE', 'KEBOOLA_STORAGE', 'KEBOOLA_1', 'KEBOOLA_1_RO'],
    ];

    /** @var array<int, array<string, string>> */
    public array $users = [];

    /** @var array<string, array<int, array<string, string>>> */
    public array $grantsToRole = [];

    /** @var array<string, array<int, array<string, string>>> */
    public array $grantsOfRole = [];

    /** @var array<string, array<int, array<string, string>>> */
    public array $schemas = [];

    /** @var array<string, array<int, array<string, string>>> */
    public array $tables = [];

    /** @var array<string, array<int, array<string, string>>> */
    public array $views = [];

    public static function keboolaProject(string $accountName): self
    {
        $account = new self();
        $account->roles = [
            self::role('KEBOOLA_1', 'KEBOOLA_STORAGE'),
            self::role('KEBOOLA_1_RO', 'KEBOOLA_1'),
            self::role('OTHER_PROJECT_ROLE', 'KEBOOLA_STORAGE'),
        ];
        $account->users = [
            self::user('KEBOOLA_1', 'KEBOOLA_1', 'John', 'Doe'),
            self::user('OTHER_PROJECT_USER', 'KEBOOLA_STORAGE', 'Jane', 'Roe'),
        ];
        $account->grantsToRole = [
            'KEBOOLA_1' => [
                self::grantToRole('OWNERSHIP', 'ROLE', 'KEBOOLA_1_RO', 'KEBOOLA_1'),
                self::grantToRole('OWNERSHIP', 'USER', 'KEBOOLA_1', 'KEBOOLA_1'),
                self::grantToRole('OWNERSHIP', 'DATABASE', 'DB1', 'KEBOOLA_1'),
                self::grantToRole('CREATE DATABASE', 'ACCOUNT', $accountName, 'KEBOOLA_1'),
            ],
            'KEBOOLA_1_RO' => [
                self::grantToRole('SELECT', 'TABLE', 'DB1.S1.T1', 'KEBOOLA_1_RO'),
            ],
            'OTHER_PROJECT_ROLE' => [],
        ];
        $account->grantsOfRole = [
            'KEBOOLA_1' => [self::grantOfRole('KEBOOLA_1', 'USER', 'KEBOOLA_1')],
            'KEBOOLA_1_RO' => [self::grantOfRole('KEBOOLA_1_RO', 'ROLE', 'KEBOOLA_1')],
            'OTHER_PROJECT_ROLE' => [],
        ];
        $account->schemas = [
            'DB1' => [['name' => 'INFORMATION_SCHEMA'], ['name' => 'PUBLIC'], ['name' => 'S1']],
        ];
        $account->tables = [
            'DB1.S1' => [self::table('S1', 'T1', '10', 'KEBOOLA_1')],
        ];
        $account->views = [
            'DB1.S1' => [['name' => 'V1', 'schema_name' => 'S1', 'owner' => 'KEBOOLA_1']],
        ];
        return $account;
    }

    /**
     * @return array<string, string>
     */
    public static function role(string $name, string $owner): array
    {
        return ['created_on' => '2024-01-01', 'name' => $name, 'comment' => '', 'owner' => $owner];
    }

    /**
     * @return array<string, string>
     */
    public static function user(string $name, string $owner, string $firstName, string $lastName): array
    {
        return [
            'name' => $name,
            'login_name' => $name,
            'display_name' => $name . ' User',
            'first_name' => $firstName,
            'last_name' => $lastName,
            'email' => '',
            'default_warehouse' => 'WH',
            'default_namespace' => '',
            'default_role' => $name,
            'owner' => $owner,
        ];
    }

    /**
     * @return array<string, string>
     */
    public static function grantToRole(
        string $privilege,
        string $grantedOn,
        string $name,
        string $granteeName,
        string $grantOption = 'false',
        string $grantedBy = 'KEBOOLA_STORAGE',
    ): array {
        return [
            'created_on' => '2024-01-01',
            'privilege' => $privilege,
            'granted_on' => $grantedOn,
            'name' => $name,
            'granted_to' => 'ROLE',
            'grantee_name' => $granteeName,
            'grant_option' => $grantOption,
            'granted_by' => $grantedBy,
        ];
    }

    /**
     * @return array<string, string>
     */
    public static function grantOfRole(
        string $role,
        string $grantedTo,
        string $granteeName,
        string $grantedBy = 'KEBOOLA_STORAGE',
    ): array {
        return [
            'created_on' => '2024-01-01',
            'role' => $role,
            'granted_to' => $grantedTo,
            'grantee_name' => $granteeName,
            'granted_by' => 'KEBOOLA_STORAGE',
        ];
    }

    /**
     * @return array<string, string>
     */
    public static function table(string $schema, string $name, string $rows, string $owner): array
    {
        return [
            'name' => $name,
            'database_name' => 'DB1',
            'schema_name' => $schema,
            'kind' => 'TABLE',
            'rows' => $rows,
            'owner' => $owner,
        ];
    }

    /**
     * @return array<int, array<string, string>>
     */
    public function fetchAll(string $sql): array
    {
        if (preg_match('/^SHOW ROLES LIMIT \d+$/', $sql)) {
            return $this->roles;
        }
        if (preg_match('/^SHOW USERS LIMIT \d+$/', $sql)) {
            return $this->users;
        }
        if (preg_match('/^SHOW GRANTS TO ROLE "(.+)"$/', $sql, $matches)) {
            return $this->grantsToRole[$matches[1]] ?? throw new RuntimeException('Role does not exist: ' . $sql);
        }
        if (preg_match('/^SHOW GRANTS OF ROLE "(.+)"$/', $sql, $matches)) {
            return $this->grantsOfRole[$matches[1]] ?? throw new RuntimeException('Role does not exist: ' . $sql);
        }
        if (preg_match('/^SHOW GRANTS TO USER "(.+)"$/', $sql, $matches)) {
            return array_map(
                fn(string $role) => [
                    'role' => $role,
                    'granted_to' => 'USER',
                    'grantee_name' => $matches[1],
                    'granted_by' => '',
                ],
                $this->rolesOfUser[$matches[1]] ?? throw new RuntimeException('User does not exist: ' . $sql),
            );
        }
        if (preg_match('/^SHOW DATABASES LIKE \'(.+)\'$/', $sql, $matches)) {
            $pattern = '/^' . str_replace(['_', '%'], ['.', '.*'], preg_quote($matches[1], '/')) . '$/i';
            return array_values(array_map(
                fn(string $database) => ['name' => $database],
                array_filter($this->databases, fn(string $database) => preg_match($pattern, $database) === 1),
            ));
        }
        if (preg_match('/^SHOW SCHEMAS IN DATABASE "(.+)"$/', $sql, $matches)) {
            if (!in_array($matches[1], $this->databases, true)) {
                throw new RuntimeException('Database does not exist: ' . $sql);
            }
            return $this->schemas[$matches[1]] ?? throw new RuntimeException('Database does not exist: ' . $sql);
        }
        if (preg_match('/^SHOW TABLES IN SCHEMA "(.+)"\."(.+)" LIMIT \d+$/', $sql, $matches)) {
            return $this->tables[$matches[1] . '.' . $matches[2]] ?? [];
        }
        if (preg_match('/^SHOW VIEWS IN SCHEMA "(.+)"\."(.+)" LIMIT \d+$/', $sql, $matches)) {
            return $this->views[$matches[1] . '.' . $matches[2]] ?? [];
        }
        throw new RuntimeException('Unexpected query: ' . $sql);
    }
}
