# Project migration with snowflake backend

> This application is used for transferring the Keboola Connection project at the database level. It transfers all tables, their permissions and database users.

# Configuration

Application needs Snowflake creadentials.

Following queries will create users with required permissions:

## On Source Snowflake account

### If you migrate to the same region
```sql
CREATE ROLE SOURCE_MAIN_MIGRATE;
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='SOURCE_MAIN_MIGRATE';
GRANT ROLE "SOURCE_MAIN_MIGRATE" TO USER "MIGRATE";
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE SOURCE_MAIN_MIGRATE;
GRANT CREATE SHARE ON ACCOUNT TO ROLE SOURCE_MAIN_MIGRATE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE_SMALL TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE_MEDIUM TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE_LARGE TO ROLE SOURCE_MAIN_MIGRATE;
```

### If you migrate to a different region

ACCOUNTADMIN is required for replication to a different region. 

```sql
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='SOURCE_MAIN_MIGRATE';
GRANT ROLE "ACCOUNTADMIN" TO USER "MIGRATE";
```

## On Migrate Snowflake account (only if you migrate to a different region)
    
```sql
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='SOURCE_MAIN_MIGRATE';
GRANT ROLE "ACCOUNTADMIN" TO USER "MIGRATE";
```

## On Target Snowflake account

```sql
CREATE ROLE TARGET_MAIN_MIGRATE;
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='TARGET_MAIN_MIGRATE';

GRANT ROLE TARGET_MAIN_MIGRATE TO USER MIGRATE;
GRANT MONITOR USAGE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE; // abych viděl vytvořené databáze a mohl jí při synchronizaci přejmenovat
GRANT IMPORT SHARE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE; // pro naimportování SHARU
GRANT CREATE ROLE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE WITH GRANT OPTION;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE;
GRANT CREATE USER ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE WITH GRANT OPTION;
GRANT CREATE DATABASE ON ACCOUNT TO ROLE TARGET_MAIN_MIGRATE WITH GRANT OPTION;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE TARGET_MAIN_MIGRATE;
```

If you already have a Keboola project on your Snowflake account, please also run the following SQL:

```sql
GRANT ROLE KEBOOLA_STORAGE TO USER "MIGRATE";
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
 
## Integration

For information about deployment and integration with KBC, please refer to the [deployment section of developers documentation](https://developers.keboola.com/extend/component/deployment/) 

## License

MIT licensed, see [LICENSE](./LICENSE) file.