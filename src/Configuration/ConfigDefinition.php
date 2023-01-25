<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;

class ConfigDefinition extends BaseConfigDefinition
{
    private const DEFAULT_SNFLK_ROLE = 'KEBOOLA_STORAGE';

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
                ->arrayNode('users')
                    ->ignoreExtraKeys(false)
                ->end()
                ->scalarNode('defaultRole')
                    ->isRequired()
                    ->cannotBeEmpty()
                    ->defaultValue(self::DEFAULT_SNFLK_ROLE)
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
