<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();

        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->children()
                ->arrayNode('migrateDatabases')
                    ->scalarPrototype()->end()
                ->end()
                ->arrayNode('passwordOfUsers')
                    ->ignoreExtraKeys(false)
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
