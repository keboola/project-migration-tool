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
                ->enumNode('action')
                    ->values([Config::ACTION_RUN, Config::ACTION_CHECK])
                    ->defaultValue(Config::ACTION_RUN)
                ->end()
                ->arrayNode('credentials')
                    ->children()
                        ->arrayNode('source')
                            ->children()
                                ->scalarNode('host')->isRequired()->end()
                                ->scalarNode('username')->isRequired()->end()
                                ->scalarNode('#password')->isRequired()->end()
                                ->scalarNode('warehouse')->isRequired()->end()
                                ->scalarNode('role')->isRequired()->end()
                            ->end()
                        ->end()
                        ->arrayNode('migration')
                            ->children()
                                ->scalarNode('host')->isRequired()->end()
                                ->scalarNode('username')->isRequired()->end()
                                ->scalarNode('#password')->isRequired()->end()
                                ->scalarNode('warehouse')->isRequired()->end()
                                ->scalarNode('role')->isRequired()->end()
                            ->end()
                        ->end()
                        ->arrayNode('target')
                            ->children()
                                ->scalarNode('host')->isRequired()->end()
                                ->scalarNode('username')->isRequired()->end()
                                ->scalarNode('#password')->isRequired()->end()
                                ->scalarNode('warehouse')->isRequired()->end()
                                ->scalarNode('role')->isRequired()->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('migrateDatabases')
                    ->scalarPrototype()->end()
                ->end()
                ->arrayNode('synchronize')
                    ->children()
                        ->booleanNode('dryPremigrationCleanupRun')->defaultTrue()->end()
                    ->end()
                ->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
