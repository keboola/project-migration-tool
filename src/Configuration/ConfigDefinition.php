<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Configuration;

use Keboola\Component\Config\BaseConfigDefinition;
use Symfony\Component\Config\Definition\Builder\ArrayNodeDefinition;
use Symfony\Component\Config\Definition\Exception\InvalidConfigurationException;

class ConfigDefinition extends BaseConfigDefinition
{
    protected function getParametersDefinition(): ArrayNodeDefinition
    {
        $parametersNode = parent::getParametersDefinition();

        // @formatter:off
        /** @noinspection NullPointerExceptionInspection */
        $parametersNode
            ->ignoreExtraKeys()
            ->children()
                ->enumNode('action')
                    ->values([
                        Config::ACTION_MIGRATE_STRUCTURE,
                        Config::ACTION_MIGRATE_DATA,
                        Config::ACTION_CHECK,
                        Config::ACTION_CLEANUP,
                        Config::ACTION_CLEANUP_SOURCE_ACCOUNT,
                    ])
                    ->defaultValue(Config::ACTION_MIGRATE_STRUCTURE)
                ->end()
                ->arrayNode('credentials')
                    ->ignoreExtraKeys()
                    ->children()
                        ->arrayNode('source')
                            ->validate()->always(function ($v) {
                                if (!empty($v['#privateKey']) && !empty($v['#password'])) {
                                    throw new InvalidConfigurationException(
                                        'You can use either privateKey or password, not both.',
                                    );
                                }
                                return $v;
                            })->end()
                            ->children()
                                ->scalarNode('host')->isRequired()->end()
                                ->scalarNode('username')->isRequired()->end()
                                ->scalarNode('#password')->end()
                                ->scalarNode('#privateKey')->end()
                                ->scalarNode('warehouse')->isRequired()->end()
                                ->scalarNode('role')->isRequired()->end()
                            ->end()
                        ->end()
                        ->arrayNode('migration')
                            ->validate()->always(function ($v) {
                                if (!empty($v['#privateKey']) && !empty($v['#password'])) {
                                    throw new InvalidConfigurationException(
                                        'You can use either privateKey or password, not both.',
                                    );
                                }
                                return $v;
                            })->end()
                            ->children()
                                ->scalarNode('host')->isRequired()->end()
                                ->scalarNode('username')->isRequired()->end()
                                ->scalarNode('#password')->end()
                                ->scalarNode('#privateKey')->end()
                                ->scalarNode('warehouse')->isRequired()->end()
                                ->scalarNode('role')->isRequired()->end()
                            ->end()
                        ->end()
                        ->arrayNode('target')
                            ->validate()->always(function ($v) {
                                if (!empty($v['#privateKey']) && !empty($v['#password'])) {
                                    throw new InvalidConfigurationException(
                                        'You can use either privateKey or password, not both.',
                                    );
                                }
                                return $v;
                            })->end()
                            ->children()
                                ->scalarNode('host')->isRequired()->end()
                                ->scalarNode('username')->isRequired()->end()
                                ->scalarNode('#password')->end()
                                ->scalarNode('#privateKey')->end()
                                ->scalarNode('warehouse')->isRequired()->end()
                                ->scalarNode('role')->isRequired()->end()
                            ->end()
                        ->end()
                    ->end()
                ->end()
                ->arrayNode('migrateDatabases')
                    ->scalarPrototype()->end()
                ->end()
                ->enumNode('warehouseSize')->values(['SMALL', 'MEDIUM', 'LARGE'])->defaultValue('SMALL')->end()
                ->booleanNode('skipCheck')->defaultFalse()->end()
                ->booleanNode('synchronize')->defaultFalse()->end()
                ->booleanNode('dryPremigrationCleanupRun')->defaultTrue()->end()
                ->booleanNode('skipDevBranches')->defaultFalse()->end()
            ->end()
        ;
        // @formatter:on
        return $parametersNode;
    }
}
