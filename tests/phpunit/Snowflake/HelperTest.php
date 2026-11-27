<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use Generator;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\Helper;
use ProjectMigrationTool\ValueObject\GrantToUser;

class HelperTest extends TestCase
{
    /**
     * @param array<string, string> $grantedByOfRole role => granted_by, in SHOW GRANTS TO USER order
     * @param string[] $expectedRoles
     * @dataProvider userGrantsProvider
     */
    public function testOrderUserGrantsForRevoke(array $grantedByOfRole, array $expectedRoles): void
    {
        $grants = [];
        foreach ($grantedByOfRole as $role => $grantedBy) {
            $grants[] = GrantToUser::fromArray([
                'role' => (string) $role,
                'granted_to' => 'USER',
                'grantee_name' => 'MIGRATE',
                'granted_by' => $grantedBy,
            ]);
        }

        $orderedRoles = array_map(
            fn(GrantToUser $grant) => $grant->getRole(),
            Helper::orderUserGrantsForRevoke($grants),
        );

        self::assertSame($expectedRoles, $orderedRoles);
    }

    public static function userGrantsProvider(): Generator
    {
        yield 'grantor sorts after the roles it granted' => [
            [
                'MIGRATE_TARGET' => '',
                'MIRO_1' => 'MIRO_STORAGE',
                'MIRO_1_RO' => 'MIRO_1',
                'MIRO_STORAGE' => 'MIGRATE_TARGET',
                'MIRO_WORKSPACE_1' => 'MIRO_1',
            ],
            ['MIRO_WORKSPACE_1', 'MIRO_1_RO', 'MIRO_1', 'MIRO_STORAGE', 'MIGRATE_TARGET'],
        ];
        yield 'grantor already sorts after the roles it granted' => [
            [
                'KEBOOLA_STORAGE' => 'MIGRATE_TARGET',
                'MIGRATE_TARGET' => '',
                'SAPI_123' => 'KEBOOLA_STORAGE',
                'SAPI_123_RO' => 'SAPI_123',
            ],
            ['SAPI_123_RO', 'SAPI_123', 'KEBOOLA_STORAGE', 'MIGRATE_TARGET'],
        ];
        yield 'three-level chain' => [
            [
                'A_LOW' => 'B_MID',
                'B_MID' => 'C_TOP',
                'C_TOP' => 'MIGRATE_TARGET',
            ],
            ['A_LOW', 'B_MID', 'C_TOP'],
        ];
        yield 'grantor not in the list keeps the reverse order' => [
            [
                'A_ROLE' => 'ACCOUNTADMIN',
                'B_ROLE' => 'OTHER_ROLE',
                'C_ROLE' => '',
            ],
            ['C_ROLE', 'B_ROLE', 'A_ROLE'],
        ];
        yield 'cycle falls back to the remaining order' => [
            [
                'A_ROLE' => 'B_ROLE',
                'B_ROLE' => 'A_ROLE',
                'C_ROLE' => 'A_ROLE',
            ],
            ['C_ROLE', 'B_ROLE', 'A_ROLE'],
        ];
        yield 'empty list' => [[], []];
        yield 'leaf roles of a cycle go first' => [
            [
                'A_ROLE' => 'B_ROLE',
                'B_ROLE' => 'A_ROLE',
                'LEAF_1' => 'A_ROLE',
                'LEAF_2' => 'B_ROLE',
            ],
            ['LEAF_2', 'LEAF_1', 'B_ROLE', 'A_ROLE'],
        ];
    }

    public function testOrderUserGrantsForRevokeWithRoleGrantedTwice(): void
    {
        $grants = array_map(
            fn(array $grant) => GrantToUser::fromArray([
                'role' => $grant[0],
                'granted_to' => 'USER',
                'grantee_name' => 'MIGRATE',
                'granted_by' => $grant[1],
            ]),
            [
                ['A_ROLE', 'A_ROLE'],
                ['B_ROLE', 'A_ROLE'],
                ['A_ROLE', 'MIGRATE_TARGET'],
            ],
        );

        $ordered = array_map(
            fn(GrantToUser $grant) => sprintf('%s BY %s', $grant->getRole(), $grant->getGrantedBy()),
            Helper::orderUserGrantsForRevoke($grants),
        );

        self::assertSame(['B_ROLE BY A_ROLE', 'A_ROLE BY A_ROLE', 'A_ROLE BY MIGRATE_TARGET'], $ordered);
    }

    /**
     * @param string[] $parts
     * @dataProvider partsProvider
     */
    public function testGenerateFormattedQuoteCombinationsReturnsAllCombinations(array $parts): void
    {
        $combinations = Helper::generateFormattedQuoteCombinations(...$parts);

        $expectedCount = 2 ** count($parts);
        self::assertCount($expectedCount, $combinations);
        self::assertCount($expectedCount, array_unique($combinations));
    }

    public static function partsProvider(): Generator
    {
        yield '1 part' => [['DB']];
        yield '2 parts' => [['DB', 'S']];
        yield '3 parts' => [['DB', 'S', 'T']];
        yield '4 parts' => [['DB', 'S', 'T', 'C']];
        yield '5 parts' => [['A', 'B', 'C', 'D', 'E']];
    }

    public function testGenerateFormattedQuoteCombinationsForOnePart(): void
    {
        self::assertSame(['DB', '"DB"'], Helper::generateFormattedQuoteCombinations('DB'));
    }

    public function testGenerateFormattedQuoteCombinationsForTwoPartsKeepsOrder(): void
    {
        self::assertSame(
            [
                'DB.my schema',
                'DB."my schema"',
                '"DB".my schema',
                '"DB"."my schema"',
            ],
            Helper::generateFormattedQuoteCombinations('DB', 'my schema'),
        );
    }

    public function testGenerateFormattedQuoteCombinationsForThreePartsKeepsOrder(): void
    {
        self::assertSame(
            [
                'DB.S.T"X',
                'DB.S."T""X"',
                'DB."S".T"X',
                'DB."S"."T""X"',
                '"DB".S.T"X',
                '"DB".S."T""X"',
                '"DB"."S".T"X',
                '"DB"."S"."T""X"',
            ],
            Helper::generateFormattedQuoteCombinations('DB', 'S', 'T"X'),
        );
    }

    /**
     * @param array<int, array<string, string>> $rows
     * @param array<int, array<string, string>> $expectedRows
     * @dataProvider showLikeRowsProvider
     */
    public function testFilterRowsByExactName(array $rows, array $expectedRows): void
    {
        self::assertSame($expectedRows, Helper::filterRowsByExactName($rows, 'SAPI_1234'));
    }

    public static function showLikeRowsProvider(): Generator
    {
        yield 'only exact row' => [
            [['name' => 'SAPI_1234', 'owner' => 'A']],
            [['name' => 'SAPI_1234', 'owner' => 'A']],
        ];
        yield 'different case is removed' => [
            [['name' => 'sapi_1234', 'owner' => 'A'], ['name' => 'SAPI_1234', 'owner' => 'B']],
            [['name' => 'SAPI_1234', 'owner' => 'B']],
        ];
        yield 'underscore wildcard match is removed' => [
            [['name' => 'SAPIX1234', 'owner' => 'A'], ['name' => 'SAPI_1234', 'owner' => 'B']],
            [['name' => 'SAPI_1234', 'owner' => 'B']],
        ];
        yield 'no exact match' => [
            [['name' => 'SAPIX1234', 'owner' => 'A'], ['name' => 'Sapi_1234', 'owner' => 'B']],
            [],
        ];
        yield 'no rows' => [[], []];
    }

    public function testGenerateFormattedQuoteCombinationsKeepsQuotedPart(): void
    {
        self::assertSame(
            ['"DB".S', '"DB"."S"', '"DB".S', '"DB"."S"'],
            Helper::generateFormattedQuoteCombinations('"DB"', 'S'),
        );
    }
}
