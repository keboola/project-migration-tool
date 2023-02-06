# Project migration with snowflake backend

> Fill in description

# Configuration

Application needs Snowflake creadentials.

Following queries will create users with required permissions:

## On Source Snowflake account

```sql
CREATE ROLE SOURCE_MAIN_MIGRATE;
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='SOURCE_MAIN_MIGRATE';
GRANT ROLE "SOURCE_MAIN_MIGRATE" TO USER "MIGRATE";
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SOURCE_MAIN_MIGRATE;
```

## On Target Snowflake account

```sql
CREATE ROLE TARGET_MAIN_MIGRATE;
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='TARGET_MAIN_MIGRATE';

GRANT MONITOR USAGE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE; // abych viděl vytvořené databáze a mohl jí při synchronizaci přejmenovat
GRANT IMPORT SHARE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE; // pro naimportování SHARU
GRANT CREATE ROLE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE WITH GRANT OPTION;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE;
GRANT CREATE USER ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE WITH GRANT OPTION;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE WITH GRANT OPTION;
```

## Development
 
Clone this repository and init the workspace with following command:

```
git clone https://github.com/keboola/project-migration-tool
cd project-migration-tool
docker-compose build
docker-compose run --rm dev composer install --no-scripts
```

Run the test suite using this command:

```
docker-compose run --rm dev composer tests
```
 
# Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 
