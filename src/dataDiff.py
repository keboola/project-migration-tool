import data_diff
import sys
import argparse

parser = argparse.ArgumentParser()
parser.add_argument('--sourceAccount')
parser.add_argument('--sourceUser')
parser.add_argument('--sourcePassword')
parser.add_argument('--targetAccount')
parser.add_argument('--targetUser')
parser.add_argument('--role')
parser.add_argument('--targetPassword')
parser.add_argument('--warehouse')
parser.add_argument('--database')
parser.add_argument('--schema')
parser.add_argument('--table')
parser.add_argument('--extraColumns')
parser.add_argument('--primaryKeys')
args = parser.parse_args()

# This is a result from print(df.columns) in the previous step, basically list of table columns except "_timestamp" and PKs
primaryKeys = tuple(args.primaryKeys.split(','))
extraColumns = tuple(args.extraColumns.split(','))

table_source_dict = {
    "driver": "snowflake",
    "user": args.sourceUser,
    "password": args.sourcePassword,
    "account": args.sourceAccount,
    "role": args.role,
    "warehouse": args.warehouse,
    "database": args.database,
    "schema": args.schema
}
table_target_dict = {
    "driver": "snowflake",
    "user": args.targetUser,
    "password": args.targetPassword,
    "account": args.targetAccount,
    "role": args.role,
    "warehouse": args.warehouse,
    "database": args.database,
    "schema": args.schema
}

table_source = data_diff.connect_to_table(
    table_source_dict,
    args.table,
    update_column="_timestamp",
    key_columns=primaryKeys,
    extra_columns=extraColumns
)

table_destination=data_diff.connect_to_table(
    table_target_dict,
    args.table,
    update_column="_timestamp",
    key_columns=primaryKeys,
    extra_columns=extraColumns
)

try:
    diff_result = list(data_diff.diff_tables(table_source, table_destination, threaded=True, max_threadpool_size=6))
except Exception as e:
    print(e)
    sys.exit(1)

print(diff_result)
sys.exit(0)