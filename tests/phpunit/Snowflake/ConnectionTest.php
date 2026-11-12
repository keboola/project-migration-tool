<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use Generator;
use Keboola\Component\UserException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\Connection;

class ConnectionTest extends TestCase
{
    /**
     * @dataProvider nonDropStatementProvider
     */
    public function testNonDropStatementDoesNotQueryRole(string $sql): void
    {
        $connection = $this->createConnection();
        $connection->expects(self::never())->method('getCurrentRole');

        $connection->assertQueryIsAllowed($sql);
    }

    public static function nonDropStatementProvider(): Generator
    {
        yield 'select' => ['SELECT 1;'];
        yield 'use role' => ['USE ROLE "ACCOUNTADMIN";'];
        yield 'alter table drop column' => ['ALTER TABLE x DROP COLUMN y'];
        yield 'create table with drop in name' => ['CREATE TABLE DROPBOX (id INT)'];
        yield 'revoke' => ['REVOKE ROLE "DROP" FROM USER "U"'];
    }

    /**
     * @dataProvider dropStatementProvider
     */
    public function testDropOnAccountAdminIsRejected(string $sql): void
    {
        $connection = $this->createConnection();
        $connection->expects(self::once())->method('getCurrentRole')->willReturn('ACCOUNTADMIN');

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Run DROP query on ACCOUNTADMIN role is not allowed.');

        $connection->assertQueryIsAllowed($sql);
    }

    /**
     * @dataProvider dropStatementProvider
     */
    public function testDropOnOtherRoleIsAllowed(string $sql): void
    {
        $connection = $this->createConnection();
        $connection->expects(self::once())->method('getCurrentRole')->willReturn('MIGRATION_ROLE');

        $connection->assertQueryIsAllowed($sql);
    }

    public static function dropStatementProvider(): Generator
    {
        yield 'uppercase' => ['DROP TABLE x'];
        yield 'lowercase with leading spaces' => ['  drop table x'];
        yield 'mixed case with leading newline' => ["\n\tDrOp DATABASE IF EXISTS \"DB\";"];
        yield 'no word boundary' => ['DROPX'];
    }

    public function testQueryDoesNotSendRejectedDrop(): void
    {
        $connection = $this->createConnection();
        $connection->expects(self::once())->method('getCurrentRole')->willReturn('ACCOUNTADMIN');

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Run DROP query on ACCOUNTADMIN role is not allowed.');

        $connection->query('  drop table x');
    }

    /**
     * @return Connection&MockObject
     */
    private function createConnection(): Connection
    {
        return $this->getMockBuilder(Connection::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['getCurrentRole'])
            ->getMock();
    }
}
