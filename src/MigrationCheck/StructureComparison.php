<?php

declare(strict_types=1);

namespace ProjectMigrationTool\MigrationCheck;

use Closure;
use Psr\Log\LoggerInterface;

/**
 * @phpstan-type Row array<string, scalar|null>
 */
class StructureComparison
{
    public const GROUP_SCHEMAS = 'Schemas';

    public const GROUP_TABLES = 'Tables';

    public const GROUP_VIEWS = 'Views';

    public const GROUP_USERS = 'Users';

    public const GROUP_GRANTS_OF_ROLES = 'Grants of roles';

    public const GROUP_ROLES = 'Roles';

    public const GROUP_GRANTS_TO_ROLES = 'Grants to roles';

    /** @var Difference[] */
    private array $differences = [];

    /** @var array<string, int> */
    private array $comparedItemCounts = [
        self::GROUP_SCHEMAS => 0,
        self::GROUP_TABLES => 0,
        self::GROUP_VIEWS => 0,
        self::GROUP_USERS => 0,
        self::GROUP_GRANTS_OF_ROLES => 0,
        self::GROUP_ROLES => 0,
        self::GROUP_GRANTS_TO_ROLES => 0,
    ];

    public function __construct(private readonly LoggerInterface $logger)
    {
    }

    /**
     * @param array<int, Row> $sourceRows
     * @param array<int, Row> $targetRows
     * @param Closure(Row): string $itemName
     * @param string[] $fields
     */
    public function compareItems(
        string $group,
        array $sourceRows,
        array $targetRows,
        Closure $itemName,
        array $fields,
    ): void {
        $sourceItems = $this->indexByItemName($sourceRows, $itemName, $fields);
        $targetItems = $this->indexByItemName($targetRows, $itemName, $fields);
        $this->comparedItemCounts[$group] = ($this->comparedItemCounts[$group] ?? 0)
            + count($sourceItems + $targetItems);

        foreach ($sourceItems as $item => $sourceValues) {
            $item = (string) $item;
            if (!isset($targetItems[$item])) {
                $this->addDifference($group, $item, Difference::KIND_MISSING_IN_TARGET, 'missing in target account');
                continue;
            }
            $changes = [];
            foreach ($sourceValues as $field => $sourceValue) {
                $targetValue = $targetItems[$item][$field];
                if ($sourceValue !== $targetValue) {
                    $changes[] = sprintf(
                        '%s: %s -> %s',
                        $field,
                        $sourceValue ?? 'NULL',
                        $targetValue ?? 'NULL',
                    );
                }
            }
            if ($changes) {
                $this->addDifference($group, $item, Difference::KIND_FIELD_DIFFERS, implode(', ', $changes));
            }
        }

        foreach (array_keys(array_diff_key($targetItems, $sourceItems)) as $item) {
            $this->addDifference(
                $group,
                (string) $item,
                Difference::KIND_MISSING_IN_SOURCE,
                'missing in source account',
            );
        }
    }

    /**
     * @return Difference[]
     */
    public function getDifferences(): array
    {
        return $this->differences;
    }

    /**
     * @return array<string, int>
     */
    public function getComparedItemCounts(): array
    {
        return $this->comparedItemCounts;
    }

    /**
     * @return array<string, int>
     */
    public function getDifferenceCounts(): array
    {
        $counts = array_fill_keys(array_keys($this->comparedItemCounts), 0);
        foreach ($this->differences as $difference) {
            $counts[$difference->getGroup()]++;
        }
        return array_filter($counts);
    }

    /**
     * @param array<int, Row> $rows
     * @param Closure(Row): string $itemName
     * @param string[] $fields
     * @return array<string, array<string, string|null>>
     */
    private function indexByItemName(array $rows, Closure $itemName, array $fields): array
    {
        $items = [];
        foreach ($rows as $row) {
            // One item can appear twice, e.g. the same privilege granted by two different roles.
            $baseName = $itemName($row);
            $name = $baseName;
            for ($i = 2; isset($items[$name]); $i++) {
                $name = sprintf('%s #%d', $baseName, $i);
            }
            $values = [];
            foreach ($fields as $field) {
                $values[$field] = isset($row[$field]) ? (string) $row[$field] : null;
            }
            $items[$name] = $values;
        }
        return $items;
    }

    private function addDifference(string $group, string $item, string $kind, string $detail): void
    {
        $difference = new Difference($group, $item, $kind, $detail);
        $this->differences[] = $difference;
        $this->logger->warning('Post-migration check difference: ' . $difference->toString());
    }
}
