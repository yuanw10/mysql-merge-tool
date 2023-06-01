import subprocess
import pymysql
import source_target
from deepdiff import DeepDiff
import re


def connect_database(db_config):
    """
    Connect to database
    :param db_config: configurations of the database {host, user, password, database}
    :return: db cursor, db connection
    """
    # check if database configurations valid
    config_valid = set(db_config.keys()) == {"host", "user", "password", "database"}

    if not config_valid:
        raise ValueError("Invalid database configurations.")

    conn = pymysql.connect(**db_config)
    cursor = conn.cursor()
    return cursor, conn


def generate_create_tables_queries(src_cursor, tgt_cursor):
    """
    Generate queries creating tables when merging source db into target db
    Queries are supposed to be executed on target db to match updates on source db
    :param src_cursor: cursor to source database
    :param tgt_cursor: cursor to target database
    :return: queries creating new tables added to source db
    """
    queries = []
    new_tables = _get_all_tables(src_cursor) - _get_all_tables(tgt_cursor)
    for t in new_tables:
        src_cursor.execute(f"SHOW CREATE TABLE {t}")
        command = src_cursor.fetchall()[0][1]
        queries.append(f"{command};\n")
    return "".join(queries)


def generate_drop_tables_queries(src_cursor, tgt_cursor):
    """
    Generate queries dropping tables when merging source into target
    Queries are supposed to be executed on target db to match updates on source db
    :param src_cursor: cursor to source database
    :param tgt_cursor: cursor to target database
    :return: queries dropping tables deleted from source
    """
    queries = []
    dropped_tables = _get_all_tables(tgt_cursor) - _get_all_tables(src_cursor)
    for t in dropped_tables:
        command = f"DROP TABLE {t}"
        queries.append(f"{command};\n")
    return "".join(queries)


def generate_modify_tables_queries(src_cursor, tgt_cursor):
    """
    Generate "ALTER TABLE" queries to modify updated tables when merging source to target
    :param src_cursor: cursor to source database
    :param tgt_cursor: cursor to target database
    :return: sql commands modifying specified tables
    """
    queries = []
    modified_tables = _get_modified_tables(src_cursor, tgt_cursor)
    for table, updates in modified_tables.items():
        cols_def_dict, _ = _get_table_definition(src_cursor, table)
        query = _generate_modify_table_query(table, updates, cols_def_dict)
        queries.append(query)
    return "".join(queries)


def _generate_modify_table_query(table, updates, cols_def_dict):
    """
    Generate "ALTER TABLE" queries to modify one updated table when merging source to target
    :param table: table that needs to be modified
    :param updates: dictionary containing what modifications to make on this table
    :param cols_def_dict: dictionary showing columns definition
    :return: sql commands modifying specified table
    """
    query = []
    for col in updates.get("added"):
        col_def = cols_def_dict.get(col)
        command = f"ALTER TABLE {table} ADD {col_def}"
        query.append(f"{command};\n")
    for col in updates.get("changed"):
        col_def = cols_def_dict.get(col)
        command = f"ALTER TABLE {table} MODIFY {col_def}"
        query.append(f"{command};\n")
    for col in updates.get("removed"):
        command = f"ALTER TABLE {table} DROP {col}"
        query.append(f"{command};\n")
    return "".join(query)


def _get_all_tables(cursor):
    """
    Fetch all tables in a database
    :param cursor: cursor to a database
    :return: a set of all tables stored in the database
    """
    cursor.execute("SHOW TABLES")
    all_tables = set(t[0] for t in cursor.fetchall())
    return all_tables


def _get_table_definition(cursor, table):
    """
    Get column-level definition of one table
    :param cursor: cursor to database used to execute sql commands
    :param table: table of which columns definition retrieved
    :return: dictionary showing columns definition, set showing table constraints
    """
    cols_def_dict = dict()
    constraints = set()

    # get SHOW CREATE TABLE result
    cursor.execute(f"SHOW CREATE TABLE {table}")
    create_table_sql = cursor.fetchall()[0][1]

    # extract content inside ()
    queries_match = re.search(r"\(.*\)", create_table_sql, re.DOTALL)
    if queries_match:
        content = queries_match.group().strip()[1:-1].strip()
        queries = set(line.strip(" ,") for line in content.split("\n"))
        columns_def = set(query for query in queries if query.startswith("`"))
        constraints = queries - columns_def
        cols_def_dict = {item.split()[0].strip("`"): item for item in columns_def}

    return cols_def_dict, constraints


def _get_modified_tables(src_cursor, tgt_cursor):
    """
    Get tables modifications to make on target db when merging source db to target
    :param src_cursor: cursor to source database
    :param tgt_cursor: cursor to target database
    :return: dictionary showing which tables and what modifications to make on a db
    """
    modified_tables = dict()

    # intersection set: tables that both databases have
    inter_tables = _get_all_tables(src_cursor) & _get_all_tables(tgt_cursor)

    # iterate all shared tables and compare
    for t in inter_tables:
        # investigate how columns of table t affected and save to dictionary
        src_table_def, _ = _get_table_definition(src_cursor, t)
        tgt_table_def, _ = _get_table_definition(tgt_cursor, t)
        modified_tables[t] = _get_modified_columns(src_table_def, tgt_table_def)

    return modified_tables


def _get_modified_columns(src_table_def, tgt_table_def):
    """
    Get columns-level modifications to make on target table when mering source table into target
    :param src_table_def: definition of source table
    :param tgt_table_def: definition of target table
    :return: dictionary showing which columns and what modifications to make on a table
    """
    diff = DeepDiff(tgt_table_def, src_table_def)
    modified_cols = dict(added=[], removed=[], changed=[])
    diff_dict = diff.to_dict()

    # iterate all columns, get how they are modified
    for column in diff.affected_root_keys:
        root_key = f"root['{column}']"
        if root_key in diff_dict.get('dictionary_item_added', set()):
            modified_cols.get("added").append(column)
        elif root_key in diff_dict.get('dictionary_item_removed', set()):
            modified_cols.get("removed").append(column)
        elif root_key in diff.get("values_changed", {}).keys():
            modified_cols.get("changed").append(column)

    return modified_cols


def generate_queries(src_cursor, tgt_cursor):
    """
    Generate queries merging source database to target database
    :param src_cursor: cursor to source database
    :param tgt_cursor: cursor to target database
    :return: sql commands merging source database to target database
    """
    merge_queries = "-- Following queries are supposed to be executed on target database to match " \
                    "updates on source database:\n"
    merge_queries += f"USE {source_target.target_db_config.get('database')};\n"
    merge_queries += "\n-- Create new tables:\n"
    merge_queries += generate_create_tables_queries(src_cursor, tgt_cursor)
    merge_queries += "\n-- Drop deleted tables:\n"
    merge_queries += generate_drop_tables_queries(src_cursor, tgt_cursor)
    merge_queries += "\n-- Modify structures of updated tables:\n"
    merge_queries += generate_modify_tables_queries(src_cursor, tgt_cursor)

    return merge_queries


def main():
    # connect to source and target databases
    try:
        src_cursor, src_conn = connect_database(source_target.source_db_config)
        tgt_cursor, tgt_conn = connect_database(source_target.target_db_config)
    except Exception as e:
        print("Connections failed: " + str(e))
        exit(1)

    # generate queries merging source database to target database
    print("Generating queries merging source database to target database....")
    merge_queries = generate_queries(src_cursor, tgt_cursor)

    # close db connections
    src_cursor.close()
    src_conn.close()
    tgt_cursor.close()
    tgt_conn.close()

    # write queries to file
    with open("merge_queries_test.sql", "w") as f:
        f.writelines(merge_queries)
    print("Merging queries completed.")


def install_dependencies():
    subprocess.run(['pip', 'install', '-r', 'requirements.txt'])


if __name__ == '__main__':
    install_dependencies()
    main()
