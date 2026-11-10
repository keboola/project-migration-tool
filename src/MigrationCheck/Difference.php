<?php

declare(strict_types=1);

namespace ProjectMigrationTool\MigrationCheck;

class Difference
{
    public const KIND_MISSING_IN_SOURCE = 'missing-in-source';

    public const KIND_MISSING_IN_TARGET = 'missing-in-target';

    public const KIND_FIELD_DIFFERS = 'field-differs';

    public function __construct(
        private readonly string $group,
        private readonly string $item,
        private readonly string $kind,
        private readonly string $detail,
    ) {
    }

    public function getGroup(): string
    {
        return $this->group;
    }

    public function getItem(): string
    {
        return $this->item;
    }

    public function getKind(): string
    {
        return $this->kind;
    }

    public function getDetail(): string
    {
        return $this->detail;
    }

    public function toString(): string
    {
        return sprintf('%s "%s": %s', $this->group, $this->item, $this->detail);
    }
}
