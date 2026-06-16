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

**Deployment prerequisites**

The tool runs the replication-group statements under the configured roles (not
`ACCOUNTADMIN`), so those roles must hold the `CREATE REPLICATION GROUP` privilege. Grant it
(as `ACCOUNTADMIN`) on **both** accounts — on the source account to the configured source
role, and on the migrate account to the configured migrate role:

```sql
-- On the SOURCE account (creates the primary replication group):
GRANT CREATE REPLICATION GROUP ON ACCOUNT TO ROLE <source_role>;

-- On the MIGRATE account (creates the secondary/replica replication group):
GRANT CREATE REPLICATION GROUP ON ACCOUNT TO ROLE <migrate_role>;
```

Without these grants the run fails with `Insufficient privileges to operate on account ...
must have CREATE REPLICATION GROUP granted on ACCOUNT`.

Replication groups authorize accounts by their **organization account identifier**
(`<organization_name>.<account_name>`), not by the legacy `<region>.<account_locator>` form.
The tool reads these via `CURRENT_ORGANIZATION_NAME()` / `CURRENT_ACCOUNT_NAME()`. Both the
source and migrate accounts must therefore belong to the **same Snowflake organization** and
be enabled for replication; otherwise the secondary creation fails with `This account is not
authorized to create a secondary replica of this primary replication group`.

If any database in `migrateDatabases` already has **standalone** database replication enabled
(from the previous per-database approach, via `ALTER DATABASE ... ENABLE REPLICATION TO
ACCOUNTS`), it must be disabled before it can be added to a replication group. Otherwise the
run fails with `Database replication for '<db>' must be disabled before it can be added to a
replication group`. Disable it on the **source** account for each affected database:

```sql
SELECT SYSTEM$DISABLE_DATABASE_REPLICATION('<database_name>');
```

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