<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use Generator;
use Keboola\Component\UserException;
use Keboola\SnowflakeDbAdapter\Exception\RuntimeException;
use PHPUnit\Framework\MockObject\MockObject;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\Connection;
use ReflectionProperty;

class ConnectionTest extends TestCase
{
    /** @var list<string> */
    private array $queries = [];

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

    public function testCurrentRoleSetByUseRoleIsNotQueried(): void
    {
        $connection = $this->createConnectionWithServerRole(null, ['query']);
        $connection->expects(self::once())->method('query')->with('USE ROLE "OWNER";');

        $connection->useRole('OWNER');

        self::assertSame('OWNER', $connection->getCurrentRole());
    }

    public function testCurrentRoleIsQueriedWhenNoRoleWasSet(): void
    {
        $connection = $this->createConnectionWithServerRole('SERVER_ROLE');

        self::assertSame('SERVER_ROLE', $connection->getCurrentRole());
    }

    public function testCurrentRoleIsQueriedWhenRoleWasSetAsQuotedName(): void
    {
        $connection = $this->createConnectionWithServerRole('OWNER', ['query']);
        $connection->expects(self::once())->method('query')->with('USE ROLE "OWNER";');

        $connection->useRole('"OWNER"');

        self::assertSame('OWNER', $connection->getCurrentRole());
    }

    public function testCurrentRoleIsQueriedAfterFailedUseRole(): void
    {
        $connection = $this->createConnectionWithServerRole('SERVER_ROLE');
        $this->setTrackedRole($connection, 'OWNER');

        try {
            $connection->useRole('MISSING');
            self::fail('USE ROLE must fail without a database connection.');
        } catch (RuntimeException) {
        }

        self::assertSame('SERVER_ROLE', $connection->getCurrentRole());
    }

    /**
     * @dataProvider statementChangingRoleProvider
     */
    public function testCurrentRoleIsQueriedAfterStatementThatCanChangeIt(string $sql): void
    {
        $connection = $this->createConnectionWithServerRole('SERVER_ROLE');
        $this->setTrackedRole($connection, 'OWNER');

        try {
            $connection->query($sql);
            self::fail('Query must fail without a database connection.');
        } catch (RuntimeException) {
        }

        self::assertSame('SERVER_ROLE', $connection->getCurrentRole());
    }

    public static function statementChangingRoleProvider(): Generator
    {
        yield 'use role' => ['USE ROLE "OTHER";'];
        yield 'use role lowercase with leading spaces' => ['  use role "OTHER";'];
        yield 'use secondary roles' => ['USE SECONDARY ROLES ALL;'];
        yield 'revoke role' => ['REVOKE ROLE "OWNER" FROM USER "U"'];
        yield 'drop role' => ['DROP ROLE IF EXISTS "OWNER"'];
    }

    public function testDropOnTrackedAccountAdminRoleIsRejectedWithoutQuery(): void
    {
        $connection = $this->createConnectionWithServerRole(null);
        $this->setTrackedRole($connection, 'ACCOUNTADMIN');

        $this->expectException(UserException::class);
        $this->expectExceptionMessage('Run DROP query on ACCOUNTADMIN role is not allowed.');

        $connection->assertQueryIsAllowed('DROP TABLE x');
    }

    public function testSameWarehouseWithSameRoleIsUsedOnce(): void
    {
        $connection = $this->createConnectionLoggingQueries();

        $connection->useRole('OWNER');
        $connection->useWarehouse('WH');
        $connection->useWarehouse('WH');

        self::assertSame(['USE ROLE "OWNER";', 'USE WAREHOUSE "WH";'], $this->queries);
    }

    public function testWarehouseIsUsedAgainWhenAnythingChanged(): void
    {
        $connection = $this->createConnectionLoggingQueries();

        $connection->useWarehouse('WH');
        $connection->useWarehouse('WH');
        $connection->useRole('OWNER');
        $connection->useWarehouse('WH');
        $connection->useWarehouse('OTHER_WH');
        $connection->useWarehouse('OTHER_WH', true);
        $connection->useRole('OTHER_OWNER');
        $connection->useWarehouse('OTHER_WH');

        self::assertSame(
            [
                'USE WAREHOUSE "WH";',
                'USE WAREHOUSE "WH";',
                'USE ROLE "OWNER";',
                'USE WAREHOUSE "WH";',
                'USE WAREHOUSE "OTHER_WH";',
                'USE WAREHOUSE "OTHER_WH";',
                'USE ROLE "OTHER_OWNER";',
                'USE WAREHOUSE "OTHER_WH";',
            ],
            $this->queries,
        );
    }

    /**
     * @dataProvider statementChangingWarehouseProvider
     */
    public function testWarehouseIsUsedAgainAfterStatementThatCanChangeIt(string $sql): void
    {
        $connection = $this->createConnectionWithServerRole(null);
        $this->setTrackedRole($connection, 'OWNER');
        (new ReflectionProperty(Connection::class, 'actualWarehouse'))->setValue($connection, 'WH');
        (new ReflectionProperty(Connection::class, 'warehouseRole'))->setValue($connection, 'OWNER');

        try {
            $connection->query($sql);
            self::fail('Query must fail without a database connection.');
        } catch (RuntimeException) {
        }

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('USE WAREHOUSE "WH";');
        $connection->useWarehouse('WH');
    }

    public static function statementChangingWarehouseProvider(): Generator
    {
        yield 'failed use warehouse' => ['USE WAREHOUSE "WH";'];
        yield 'use warehouse lowercase' => ['use warehouse "OTHER_WH";'];
        yield 'alter warehouse' => ['ALTER WAREHOUSE "WH" RENAME TO "WH2"'];
        yield 'drop warehouse' => ['DROP WAREHOUSE IF EXISTS "WH"'];
        yield 'create warehouse' => ['CREATE WAREHOUSE "NEW_WH"'];
        yield 'create warehouse if not exists' => ['CREATE WAREHOUSE IF NOT EXISTS "NEW_WH"'];
        yield 'create or replace warehouse' => ['create or replace warehouse "NEW_WH"'];
    }

    /**
     * @dataProvider statementKeepingWarehouseProvider
     */
    public function testWarehouseIsNotUsedAgainAfterUnrelatedStatement(string $sql): void
    {
        $connection = $this->createConnectionWithServerRole(null);
        $this->setTrackedRole($connection, 'OWNER');
        (new ReflectionProperty(Connection::class, 'actualWarehouse'))->setValue($connection, 'WH');
        (new ReflectionProperty(Connection::class, 'warehouseRole'))->setValue($connection, 'OWNER');

        try {
            $connection->query($sql);
            self::fail('Query must fail without a database connection.');
        } catch (RuntimeException) {
        }

        $connection->useWarehouse('WH');
        self::assertSame('WH', (new ReflectionProperty(Connection::class, 'actualWarehouse'))->getValue($connection));
    }

    public static function statementKeepingWarehouseProvider(): Generator
    {
        yield 'create table' => ['CREATE TABLE "DB"."S"."T" (id INT)'];
        yield 'create role' => ['CREATE ROLE IF NOT EXISTS "R"'];
    }

    /**
     * @return Connection&MockObject
     */
    private function createConnectionLoggingQueries(): Connection
    {
        $connection = $this->createConnectionWithServerRole(null, ['query']);
        $connection->expects(self::atLeastOnce())
            ->method('query')
            ->willReturnCallback(function (string $sql): void {
                $this->queries[] = $sql;
            });
        return $connection;
    }

    /**
     * @param list<string> $mockedMethods
     * @return Connection&MockObject
     */
    private function createConnectionWithServerRole(?string $serverRole, array $mockedMethods = []): Connection
    {
        $connection = $this->getMockBuilder(Connection::class)
            ->disableOriginalConstructor()
            ->onlyMethods(['fetchAll', ...$mockedMethods])
            ->getMock();
        (new ReflectionProperty(Connection::class, 'defaultRole'))->setValue($connection, 'DEFAULT_ROLE');
        if ($serverRole === null) {
            $connection->expects(self::never())->method('fetchAll');
        } else {
            $connection->expects(self::once())
                ->method('fetchAll')
                ->with('SELECT CURRENT_ROLE() AS "role";')
                ->willReturn([['role' => $serverRole]]);
        }
        return $connection;
    }

    private function setTrackedRole(Connection $connection, string $role): void
    {
        (new ReflectionProperty(Connection::class, 'actualRole'))->setValue($connection, $role);
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
