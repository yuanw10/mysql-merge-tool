import argparse
from mysql_merge_tool import database_comparison
from mysql_merge_tool import dump_comparison


def main():
    """
    Output sql commands merging two databases to a file
    """

    merge_sql = "-- Following queries are supposed to be executed on target database to match " \
                "updates on source database:\n"

    args = parse_arguments()

    if args.type == 'conn':
        source_db_config = {
            "host": args.source_host,
            "user": args.source_user,
            "password": args.source_password,
            "database": args.source_database
        }
        target_db_config = {
            "host": args.target_host,
            "user": args.target_user,
            "password": args.target_password,
            "database": args.target_database
        }

        merge_sql += database_comparison.generate_merge_sql(source_db_config, target_db_config)

    elif args.type == 'dump':
        try:
            with open(args.source_file, "r") as f:
                source_dump = f.read()
            with open(args.target_file, "r") as f:
                target_dump = f.read()
        except FileNotFoundError:
            print("Cannot find dump files. Please check file paths.")
            exit(1)
        merge_sql += dump_comparison.generate_merge_sql(source_dump, target_dump)

    # output merge sql to file
    with open("merge_sql.sql", "w") as f:
        f.write(merge_sql)
    print("Merging sql completed.")


def parse_arguments():
    """
    Parse command line arguments entered by users:
    user can choose 'dump' or 'conn' as the input (mysql dump files or database connections)
    and specify file paths or database configurations
    :return: parsed command line arguments
    """
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(dest='type')

    # sub parser: add paths to sql dump files
    parser_dump = subparsers.add_parser('dump')
    # paths to source and target sql dump files
    parser_dump.add_argument('-s', '--source-file', help='source SQL dump file path', required=True)
    parser_dump.add_argument('-t', '--target-file', help='target SQL dump file path', required=True)

    # sub parser: add configurations of databases
    parser_conn = subparsers.add_parser('conn')
    # source database configuration
    parser_conn.add_argument('-sh', '--source-host', help='source database host', required=True)
    parser_conn.add_argument('-su', '--source-user', help='source database user', required=True)
    parser_conn.add_argument('-sp', '--source-password', help='source database password', default="")
    parser_conn.add_argument('-sd', '--source-database', help='source database sub-database', required=True)
    # target database configuration
    parser_conn.add_argument('-th', '--target-host', help='target database host', required=True)
    parser_conn.add_argument('-tu', '--target-user', help='target database user', required=True)
    parser_conn.add_argument('-tp', '--target-password', help='target database password', default="")
    parser_conn.add_argument('-td', '--target-database', help='target database sub-database', required=True)

    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
