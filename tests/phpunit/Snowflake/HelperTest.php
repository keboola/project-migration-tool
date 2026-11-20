<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Tests\Snowflake;

use Generator;
use PHPUnit\Framework\TestCase;
use ProjectMigrationTool\Snowflake\Helper;

class HelperTest extends TestCase
{
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
