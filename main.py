import subprocess
import pymysql
import source_target
from deepdiff import DeepDiff


def connect_database(db_config):
    """
    Connect to database
    :param db_config: configurations of the database {host, user, password, database}
    :return: db cursor, db connection
    """
    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    return cursor, conn


def get_all_tables(cursor):
    """
    Fetch all tables in a database
    :param cursor: cursor to a database
    :return: a set of all tables stored in the database
    """
    cursor.execute("SHOW TABLES")
    all_tables = set(t[0] for t in cursor.fetchall())
    return all_tables


def get_tables_def(cursor, tables):
    """
    Get definitions of tables using "DESCRIBE table"
    :param cursor: cursor to a database
    :param tables: tables that need to be described
    :return: dictionary that saves definitions of tables
    """
    tables_def = dict()
    # iterate all tables
    for t in tables:
        # describe table t
        cursor.execute(f"DESCRIBE {t}")
        columns = cursor.fetchall()
        # key - value : table name - definition tuple
        tables_def[t] = {r[0]: r[1:-1] for r in columns}
    return tables_def


def generate_create_tables_queries(src_tables, tgt_tables, src_cursor):
    """
    Generate queries creating tables when merging source into target
    Queries are supposed to be executed on target db to match updates on source db
    :param src_tables: source tables set
    :param tgt_tables: target tables set
    :param src_cursor: cursor to source database
    :return: queries creating new tables added to source db
    """
    queries = []
    new_tables = src_tables - tgt_tables
    for t in new_tables:
        src_cursor.execute(f"SHOW CREATE TABLE {t}")
        command = src_cursor.fetchall()[0][1]
        queries.append(f"{command};\n")
    return "".join(queries)


def generate_drop_tables_queries(src_tables, tgt_tables):
    """
    Generate queries dropping tables when merging source into target
    Queries are supposed to be executed on target db to match updates on source db
    :param src_tables: source tables set
    :param tgt_tables: target tables set
    :return: queries dropping tables deleted from source
    """
    queries = []
    dropped_tables = tgt_tables - src_tables
    for t in dropped_tables:
        command = f"DROP TABLE {t}"
        queries.append(f"{command};\n")
    return "".join(queries)


# TODO：
def generate_modify_tables_queries(src_tables, tgt_tables, src_tables_def, tgt_tables_def):
    queries = []
    # diff = DeepDiff(tgt_tables_def, src_tables_def)
    # diff.affected_root_keys
    return "".join(queries)


def main():
    # check if database configurations valid
    src_config_valid = set(source_target.source_db_config.keys()) == {"host", "user", "password", "database"}
    tgt_config_valid = set(source_target.target_db_config.keys()) == {"host", "user", "password", "database"}
    if not (src_config_valid and tgt_config_valid):
        print("Invalid database configurations. Please check source_target.py")
        exit(1)

    # connect to source and target databases
    try:
        src_cursor, src_conn = connect_database(source_target.source_db_config)
        tgt_cursor, tgt_conn = connect_database(source_target.target_db_config)
    except Exception as e:
        print("Connections failed: " + e)
        exit(1)

    # get sets of all table in source and target databases
    src_tables = get_all_tables(src_cursor)
    tgt_tables = get_all_tables(tgt_cursor)

    # get definitions of all tables in source and target databases
    src_tables_def = get_tables_def(src_cursor, src_tables)
    tgt_tables_def = get_tables_def(tgt_cursor, tgt_tables)

    # generate queries merging source database to target database
    print("Generating queries merging source database to target database....")
    merge_queries = "-- Following queries are supposed to be executed on target database to match " \
                    "updates on source database:\n"
    merge_queries += "\n-- Create new tables:\n"
    merge_queries += generate_create_tables_queries(src_tables, tgt_tables, src_cursor)
    merge_queries += "\n-- Drop deleted tables:\n"
    merge_queries += generate_drop_tables_queries(src_tables, tgt_tables)
    merge_queries += "\n-- Modify structures of updated tables:\n"
    # TODO
    merge_queries += generate_modify_tables_queries(src_tables, tgt_tables, src_tables_def, tgt_tables_def)

    # close db connections
    src_cursor.close()
    src_conn.close()
    tgt_cursor.close()
    tgt_conn.close()

    # write queries to file
    with open("merge_queries.sql", "w") as f:
        f.writelines(merge_queries)
    print("Merging queries completed.")


def install_dependencies():
    subprocess.run(['pip', 'install', '-r', 'requirements.txt'])


if __name__ == '__main__':
    install_dependencies()
    main()
