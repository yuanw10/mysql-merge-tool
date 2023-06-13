import pymysql
from mysql_merge_tool import utils


def generate_create_tables_sql(source_cursor, target_cursor):
    """
    Generate sql commands creating tables when merging source db into target db
    :param source_cursor: source database cursor
    :param target_cursor: target database cursor
    :return: sql commands creating new tables added to source db
    """
    comment = "-- Create new tables:\n"
    commands = []
    new_tables = _get_all_tables(source_cursor) - _get_all_tables(target_cursor)
    for t in new_tables:
        source_cursor.execute(f"SHOW CREATE TABLE {t}")
        command = source_cursor.fetchall()[0][1]
        commands.append(f"{command};\n")
    return comment + "".join(commands) + "\n"


def generate_drop_tables_sql(source_cursor, target_cursor):
    """
    Generate sql commands dropping tables when merging source db into target db
    :param source_cursor: source database cursor
    :param target_cursor: target database cursor
    :return: sql commands dropping tables from target that were deleted from source
    """
    comment = "-- Drop deleted tables:\n"
    commands = []
    dropped_tables = _get_all_tables(target_cursor) - _get_all_tables(source_cursor)
    for t in dropped_tables:
        command = f"DROP TABLE {t}"
        commands.append(f"{command};\n")
    return comment + "".join(commands) + "\n"


def generate_modify_tables_sql(source_cursor, target_cursor):
    """
    Generate "ALTER TABLE" commands to modify updated tables when merging source to target
    :param source_cursor: source database cursor
    :param target_cursor: target database cursor
    :return: sql commands modifying tables shared by source and target databases
    """
    comment = "-- Modify structures of updated tables:\n"
    commands = []

    # intersection set: tables that both databases have
    inter_tables = _get_all_tables(source_cursor) & _get_all_tables(target_cursor)

    # iterate all shared tables
    for table in inter_tables:
        source_table_def = get_table_definition(table, source_cursor)
        target_table_def = get_table_definition(table, target_cursor)
        table_updates = utils.get_table_updates(source_table_def, target_table_def)
        command = utils.generate_modify_table_sql(table, table_updates)
        commands.append(command)
    return comment + "".join(commands) + "\n"


def get_table_definition(table, cursor):
    """
    Get definition of the specified table: columns and constraints
    :param table: table of which definition needs to get
    :param cursor: database cursor
    :return: definition of the specified table
    """
    cursor.execute(f"SHOW CREATE TABLE {table}")
    create_table_sql: str = cursor.fetchall()[0][1]
    table_def = utils.parse_table_definition(create_table_sql)
    return table_def


def _get_all_tables(cursor):
    """
    Fetch all tables in a database
    :param cursor: cursor to a database
    :return: a set of all tables stored in the database
    """
    cursor.execute("SHOW TABLES")
    all_tables = set(t[0] for t in cursor.fetchall())
    return all_tables


def generate_merge_sql(source_db_config, target_db_config):
    """
    Generate sql commands merging source database to target database
    :param source_db_config: source database configuration
    :param target_db_config: target database configuration
    :return: sql commands merging source database to target database
    """

    # connect to source and target databases
    try:
        source_conn = pymysql.connect(**source_db_config)
        target_conn = pymysql.connect(**target_db_config)
    except Exception as e:
        print("Connections failed: " + str(e))
        exit(1)

    merge_sql = ""
    merge_sql += generate_create_tables_sql(source_conn.cursor(), target_conn.cursor())
    merge_sql += generate_drop_tables_sql(source_conn.cursor(), target_conn.cursor())
    merge_sql += generate_modify_tables_sql(source_conn.cursor(), target_conn.cursor())

    # close db connections
    source_conn.close()
    target_conn.close()

    return merge_sql
