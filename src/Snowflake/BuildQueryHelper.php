<?php

declare(strict_types=1);

namespace ProjectMigrationTool\Snowflake;

use Keboola\SnowflakeDbAdapter\QueryBuilder;

class BuildQueryHelper
{

    public static function buildSqlFunctionQuery(array $function, array $functionParams): string
    {
        $sql = <<<SQL
CREATE %s FUNCTION %s%s
returns %s 
AS 
$$
%s
$$
;
SQL;

        return sprintf(
            $sql,
            $function['is_secure'] === 'Y' ? 'SECURE' : '',
            Helper::quoteIdentifier($function['name']),
            $functionParams['signature'],
            $functionParams['returns'],
            trim($functionParams['body'])
        );
    }

    public static function buildPythonFunctionQuery(array $function, array $functionParams): string
    {
        $python = <<<SQL
CREATE FUNCTION %s%s
RETURNS %s 
LANGUAGE PYTHON
RUNTIME_VERSION=%s
HANDLER=%s
%s
%s
AS 
$$
%s
$$
;
SQL;

        return sprintf(
            $python,
            Helper::quoteIdentifier($function['name']),
            $functionParams['signature'],
            $functionParams['returns'],
            $functionParams['runtime_version'],
            QueryBuilder::quote($functionParams['handler']),
            $functionParams['imports'] !== '[]' ?
                'IMPORTS = (' . substr($functionParams['imports'], 1, -1) . ')' :
                '',
            $functionParams['packages'] !== '[]' ?
                'PACKAGES = (' . substr($functionParams['packages'], 1, -1) . ')' :
                '',
            trim($functionParams['body'])
        );
    }

    public static function buildSqlProcedureQuery(array $procedure, array $procedureParams): string
    {
        $sql = <<<SQL
CREATE %s PROCEDURE %s%s
returns %s 
language sql
execute as %s
AS 
%s
;
SQL;

        return sprintf(
            $sql,
            $procedure['is_secure'] === 'Y' ? 'SECURE' : '',
            Helper::quoteIdentifier($procedure['name']),
            $procedureParams['signature'],
            $procedureParams['returns'],
            $procedureParams['execute as'],
            trim($procedureParams['body'])
        );
    }

    public static function buildJavaProcedureQuery(array $procedure, array $procedureParams): string
    {
        $sql = <<<SQL
CREATE %s PROCEDURE %s%s
returns %s 
language java
runtime_version = '%s'
handler = '%s'
execute as %s
%s
%s
AS 
$$
%s
$$
;
SQL;

        return sprintf(
            $sql,
            $procedure['is_secure'] === 'Y' ? 'SECURE' : '',
            Helper::quoteIdentifier($procedure['name']),
            $procedureParams['signature'],
            $procedureParams['returns'],
            $procedureParams['runtime_version'],
            $procedureParams['handler'],
            $procedureParams['execute as'],
            $procedureParams['packages'] !== '[]' ?
                'PACKAGES = (' . substr($procedureParams['packages'], 1, -1) . ')' :
                '',
            $procedureParams['imports'] !== '[]' ?
                'IMPORTS = (' . substr($procedureParams['imports'], 1, -1) . ')' :
                '',
            trim($procedureParams['body'])
        );
    }

    public static function buildJavascriptProcedureQuery(array $procedure, array $procedureParams): string
    {
        $sql = <<<SQL
CREATE %s PROCEDURE %s%s
returns %s 
language javascript
execute as %s
AS 
$$
%s
$$
;
SQL;

        return sprintf(
            $sql,
            $procedure['is_secure'] === 'Y' ? 'SECURE' : '',
            Helper::quoteIdentifier($procedure['name']),
            $procedureParams['signature'],
            $procedureParams['returns'],
            $procedureParams['execute as'],
            trim($procedureParams['body'])
        );
    }

    public static function buildPythonProcedureQuery(array $procedure, array $procedureParams): string
    {
        $sql = <<<SQL
CREATE %s PROCEDURE %s%s
returns %s 
language python
runtime_version = '%s'
handler = '%s'
%s
%s
execute as %s
AS 
$$
%s
$$
;
SQL;

        return sprintf(
            $sql,
            $procedure['is_secure'] === 'Y' ? 'SECURE' : '',
            Helper::quoteIdentifier($procedure['name']),
            $procedureParams['signature'],
            $procedureParams['returns'],
            $procedureParams['runtime_version'],
            $procedureParams['handler'],
            $procedureParams['packages'] !== '[]' ?
                'PACKAGES = (' . substr($procedureParams['packages'], 1, -1) . ')' :
                '',
            $procedureParams['imports'] !== '[]' ?
                'IMPORTS = (' . substr($procedureParams['imports'], 1, -1) . ')' :
                '',
            $procedureParams['execute as'],
            trim($procedureParams['body'])
        );
    }
}
