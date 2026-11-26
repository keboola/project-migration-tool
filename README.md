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
GRANT MANAGE SHARE TARGET ON ACCOUNT TO ROLE SOURCE_MAIN_MIGRATE;
GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE_SMALL TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE_MEDIUM TO ROLE SOURCE_MAIN_MIGRATE;
GRANT USAGE ON WAREHOUSE MIGRATE_LARGE TO ROLE SOURCE_MAIN_MIGRATE;
```

### If you migrate to a different region

ACCOUNTADMIN is not required. The tool runs under the configured source role, so create the role as for
the same region and add the replication grants from [Deployment prerequisites](#cross-region-replication)
below:

```sql
-- all grants from "If you migrate to the same region", then:
GRANT CREATE REPLICATION GROUP ON ACCOUNT TO ROLE SOURCE_MAIN_MIGRATE;
-- for each database in migrateDatabases:
GRANT MONITOR ON DATABASE <database_name> TO ROLE SOURCE_MAIN_MIGRATE;
```

The simple alternative is to use `ACCOUNTADMIN` as the source role:

```sql
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='ACCOUNTADMIN';
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

The cleanup also revokes roles from the target migration user (`credentials.target.username`), but only
the roles of the databases in `migrateDatabases`: the owner role of each database and the roles granted
to the user by these roles. Roles of other projects stay, so other projects can still be migrated with the
same user on the same target account. The shared main role (for example `KEBOOLA_STORAGE`) is revoked only
when no other project role depends on it. The kept roles are listed in the job log.

**Deployment prerequisites**

The tool runs all statements under the configured roles, so `ACCOUNTADMIN` is not required.
A cross-region run on real accounts passed with these grants (run them as `ACCOUNTADMIN`):

```sql
-- On the SOURCE account (creates the primary replication group),
-- in addition to the grants for the same region:
GRANT CREATE REPLICATION GROUP ON ACCOUNT TO ROLE <source_role>;
GRANT CREATE SHARE ON ACCOUNT TO ROLE <source_role>;
GRANT MANAGE GRANTS ON ACCOUNT TO ROLE <source_role>;
-- for each database in migrateDatabases:
GRANT MONITOR ON DATABASE <database_name> TO ROLE <source_role>;

-- On the MIGRATE account (creates the replica replication group and the shares):
GRANT CREATE REPLICATION GROUP ON ACCOUNT TO ROLE <migrate_role>;
GRANT CREATE SHARE ON ACCOUNT TO ROLE <migrate_role>;
GRANT MANAGE SHARE TARGET ON ACCOUNT TO ROLE <migrate_role>;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE <migrate_role>;
-- or, instead of CREATE WAREHOUSE, an existing warehouse with the lowercase name "migrate":
-- GRANT USAGE ON WAREHOUSE "migrate" TO ROLE <migrate_role>;
```

The simple alternative is to use `ACCOUNTADMIN` as the source and migrate role.

Errors when a grant is missing:

- no `CREATE REPLICATION GROUP`: `Insufficient privileges to operate on account ... must have
  CREATE REPLICATION GROUP granted on ACCOUNT`
- no privilege on a migrated database: `Insufficient privileges to operate on database '<db>'.
  Your primary role ... must have USAGE or any other privilege granted on DATABASE <db>`.
  `MONITOR` on the database is enough.

Errors that the tool retries:

- `Replication group '<org>.<account>.MIGRATION_RG_...' does not exist or is not authorized` when the
  replica group is created on the migrate account. A new primary group can take a few minutes to become
  visible on another account. The tool makes up to 20 attempts, 15 seconds apart (about 5 minutes). If it still fails,
  check that the migrate account is enabled for replication with the source account.
- `Replication group 'MIGRATION_RG_...' is currently a primary replication group in a replication
  relationship and cannot be dropped` when the cleanup drops the group on the source account. The source
  account can still see the replica that was just dropped. The tool tries again in the same way.

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

Create a role with the grants for the migrate account from
[Deployment prerequisites](#cross-region-replication):

```sql
CREATE ROLE MIGRATE_MAIN_MIGRATE;
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='MIGRATE_MAIN_MIGRATE';
GRANT ROLE "MIGRATE_MAIN_MIGRATE" TO USER "MIGRATE";
GRANT CREATE REPLICATION GROUP ON ACCOUNT TO ROLE MIGRATE_MAIN_MIGRATE;
GRANT CREATE SHARE ON ACCOUNT TO ROLE MIGRATE_MAIN_MIGRATE;
GRANT MANAGE SHARE TARGET ON ACCOUNT TO ROLE MIGRATE_MAIN_MIGRATE;
GRANT CREATE WAREHOUSE ON ACCOUNT TO ROLE MIGRATE_MAIN_MIGRATE;
```

The simple alternative is `ACCOUNTADMIN`:

```sql
CREATE USER "MIGRATE" PASSWORD='MIGRATE_PASSWORD' DEFAULT_ROLE='ACCOUNTADMIN';
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

## Post-migration check

The `runCheckMigratedData` action compares the source and the target account. It uses
`SHOW` commands, so it shows the current state and can run right after the migration.
You do not need to wait for `SNOWFLAKE.ACCOUNT_USAGE` to catch up, and no warehouse is used.

**When to run it:** after `runMigrateData` and before `runCleanup`. Keep the source project frozen
(maintenance mode) from the data migration until the check ends; otherwise new writes change the
row counts and the check fails.

The schemas, tables and views of each database are listed with the database owner role on both accounts.
Before `runCleanup` the migration users already have this role. After `runCleanup` the target user no
longer has the roles of the migrated databases, so the check grants the database role to the user and
revokes it at the end. If the configured role cannot grant it, the job fails with an error that says so.

For each database in `migrateDatabases` the check compares:

- the database itself: if it does not exist on the target account, this is reported and its schemas,
  tables and views are not checked
- schemas (except `INFORMATION_SCHEMA` and `PUBLIC`)
- tables: `name`, `schema_name`, `kind`, `rows`, `owner`
- views: `name`, `schema_name`, `owner`
- the database owner role and all roles it owns (also nested), discovered on both accounts:
  roles (`name`, `owner`), grants to these roles and grants of these roles
- users owned by these roles: `name`, `login_name`, `display_name`, `default_warehouse`,
  `default_namespace`, `default_role`, `owner`

Fields the migration does not copy (the role `comment`, the user `first_name`, `last_name` and `email`)
are not compared. Grants are compared the way the migration creates them:

- Snowflake internal `USER$...` grants are skipped on both accounts.
- Warehouse names in source grants are renamed from `credentials.source.warehouse` to
  `credentials.target.warehouse`, as the migration does. The users' `default_warehouse` is not renamed,
  because the migration copies it as it is.
- `granted_by = ACCOUNTADMIN` on the source is expected as the configured target role on the target.
- Warehouse grants may be recorded on the target as granted by the configured target role instead of
  the source grantor. The check accepts this when `granted_by` is the only difference. Snowflake keeps
  one grant per grantor, so the same warehouse grant made by several source grantors becomes one grant on
  the target. The check accepts this too, also when one of the source grantors is `ACCOUNTADMIN`, and logs
  which source grantors became one target grant. A warehouse grant is reported only when the target has
  no grant by the configured target role for it.
  As a result, revoking the warehouse from a project role on the target does not cascade to the grants
  that role made. Because the target role is recorded as the grantor and owns the warehouse, neither
  the main role nor the project role can revoke `USAGE ON WAREHOUSE` from a workspace or child role
  on the target; only `DROP ROLE` removes it.
- Account-level grants are compared without the account name, because it differs between accounts.
- Grants on the `<DB>_SHARE` and `<DB>_OLD` databases are skipped on both accounts.
- Grants of roles to the configured migration user (`credentials.source.username` on the source account,
  `credentials.target.username` on the target account) are skipped, because the migration itself
  creates them on one account only.

The job log shows how many grants were skipped on each account.
The check does not compare the data in the tables, only the row counts.

Each difference is logged as a warning. If there are any differences, the job fails and the
error message lists a count per group and the first 50 differences. The full list is in the job log.

Optional parameter:

- `checkReportOnly` (default `false`) — log the differences but do not fail the job.

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