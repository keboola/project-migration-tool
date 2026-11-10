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

    public function testGenerateFormattedQuoteCombinationsKeepsQuotedPart(): void
    {
        self::assertSame(
            ['"DB".S', '"DB"."S"', '"DB".S', '"DB"."S"'],
            Helper::generateFormattedQuoteCombinations('"DB"', 'S'),
        );
    }
}
