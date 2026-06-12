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

### Cross-region replication

For cross-region migrations the tool creates a single Snowflake replication group on the
source account, replicates it to the migrate account, and refreshes it. The refresh is
awaited by polling `REPLICATION_GROUP_REFRESH_PROGRESS`; progress is logged on each poll.

The group name is derived deterministically from the set of migrated databases
(`MIGRATION_RG_<hash>`). The same migration reuses the same group across repeated runs
(idempotent), while migrations with different database sets get distinct names so parallel
migrations never collide on a shared group.

The replication group is dropped during the cleanup phase (`runCleanup`). Because dropping a
secondary replication group leaves its member databases behind as writable standalone
databases, the cleanup also drops those replica databases on the migrate account so their
names are freed and they stop consuming storage. Same-region migrations do not use a
replication group, so creation and teardown are no-ops.

**Deployment prerequisite:** the configured source and migrate roles must hold the
privilege to create and drop replication groups (`CREATE REPLICATION GROUP` on the source
account, `CREATE REPLICATION GROUP ... AS REPLICA` on the migrate account, and `OWNERSHIP`
to drop them). The tool runs these statements under the configured roles, not `ACCOUNTADMIN`.

Tunable parameters (optional):

- `replicationRefreshTimeout` (seconds, default `3600`) — fail the run if the refresh
  does not complete within this time.
- `replicationRefreshPollInterval` (seconds, default `30`) — wait between progress polls.

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