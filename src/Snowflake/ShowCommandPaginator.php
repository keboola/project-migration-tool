<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\QueryBuilder;

class ShowCommandPaginator
{
    public const MAX_PAGE_SIZE = 10000;

    /**
     * Snowflake SHOW commands return at most 10,000 rows, so the next page starts after the last name.
     *
     * @return array<int, array<string, string|int|float|bool|null>>
     */
    public static function fetchAll(
        Connection $connection,
        string $showCommand,
        int $pageSize = self::MAX_PAGE_SIZE,
    ): array {
        $rows = [];
        $lastName = null;
        while (true) {
            $sql = sprintf('%s LIMIT %d', $showCommand, $pageSize);
            if ($lastName !== null) {
                $sql .= sprintf(' FROM %s', QueryBuilder::quote($lastName));
            }
            /** @var array<int, array<string, string|int|float|bool|null>> $page */
            $page = $connection->fetchAll($sql);

            $newRows = array_values(array_filter($page, fn(array $row) => $row['name'] !== $lastName));
            array_push($rows, ...$newRows);

            if (count($page) < $pageSize || $newRows === []) {
                return $rows;
            }
            $lastName = (string) end($newRows)['name'];
        }
    }
}
