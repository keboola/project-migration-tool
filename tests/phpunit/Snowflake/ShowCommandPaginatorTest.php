<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\Connection;
use ProjectMigrationTool\Snowflake\ShowCommandPaginator;

class ShowCommandPaginatorTest extends TestCase
{
    public function testFetchesAllPages(): void
    {
        $expectedCalls = [
            ['sql' => 'SHOW ROLES LIMIT 2', 'rows' => [['name' => 'A'], ['name' => 'B']]],
            ['sql' => 'SHOW ROLES LIMIT 2 FROM \'B\'', 'rows' => [['name' => 'C'], ['name' => 'D']]],
            ['sql' => 'SHOW ROLES LIMIT 2 FROM \'D\'', 'rows' => [['name' => 'E']]],
        ];

        $connection = $this->createMock(Connection::class);
        $connection->expects(self::exactly(count($expectedCalls)))
            ->method('fetchAll')
            ->willReturnCallback(function (string $sql) use (&$expectedCalls): array {
                $expected = array_shift($expectedCalls);
                self::assertNotNull($expected);
                self::assertSame($expected['sql'], $sql);
                return $expected['rows'];
            });

        $rows = ShowCommandPaginator::fetchAll($connection, 'SHOW ROLES', 2);

        self::assertSame(['A', 'B', 'C', 'D', 'E'], array_column($rows, 'name'));
    }

    public function testStopsAfterFirstShortPage(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->expects(self::once())
            ->method('fetchAll')
            ->with('SHOW TABLES IN SCHEMA "DB"."S" LIMIT 10000')
            ->willReturn([['name' => 'T1']]);

        $rows = ShowCommandPaginator::fetchAll($connection, 'SHOW TABLES IN SCHEMA "DB"."S"');

        self::assertSame(['T1'], array_column($rows, 'name'));
    }

    public function testSkipsRowRepeatedFromPreviousPage(): void
    {
        $expectedCalls = [
            ['sql' => 'SHOW USERS LIMIT 2', 'rows' => [['name' => 'A'], ['name' => 'B']]],
            ['sql' => 'SHOW USERS LIMIT 2 FROM \'B\'', 'rows' => [['name' => 'B'], ['name' => 'C']]],
            ['sql' => 'SHOW USERS LIMIT 2 FROM \'C\'', 'rows' => [['name' => 'C']]],
        ];

        $connection = $this->createMock(Connection::class);
        $connection->expects(self::exactly(count($expectedCalls)))
            ->method('fetchAll')
            ->willReturnCallback(function (string $sql) use (&$expectedCalls): array {
                $expected = array_shift($expectedCalls);
                self::assertNotNull($expected);
                self::assertSame($expected['sql'], $sql);
                return $expected['rows'];
            });

        $rows = ShowCommandPaginator::fetchAll($connection, 'SHOW USERS', 2);

        self::assertSame(['A', 'B', 'C'], array_column($rows, 'name'));
    }
}
