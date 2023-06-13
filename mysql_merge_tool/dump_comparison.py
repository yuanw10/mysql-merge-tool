import re
from mysql_merge_tool import utils


def generate_create_tables_sql(source_dump, target_dump):
    """
    Generate sql commands creating tables when merging source db into target db
    :param source_dump: source database mysql dump file
    :param target_dump: target database mysql dump file
    :return: sql commands creating new tables added to source db
    """
    comment = "-- Create new tables:\n"
    commands = []
    source_db = _parse_dump(source_dump)
    target_db = _parse_dump(target_dump)
    new_tables = set(source_db.keys()) - set(target_db.keys())

    for table in new_tables:
        command = source_db.get(table)
        commands.append(command+"\n")
    return comment + "".join(commands) + "\n"


def generate_drop_tables_sql(source_dump, target_dump):
    """
    Generate sql commands dropping tables when merging source db into target db
    :param source_dump: source database mysql dump file
    :param target_dump: target database mysql dump file
    :return: sql commands dropping tables from target that were deleted from source
    """
    comment = "-- Drop deleted tables:\n"
    commands = []
    source_db = _parse_dump(source_dump)
    target_db = _parse_dump(target_dump)
    dropped_tables = set(target_db.keys()) - set(source_db.keys())

    for table in dropped_tables:
        command = f"DROP TABLE {table}"
        commands.append(command+"\n")
    return comment + "".join(commands) + "\n"


def _parse_dump(dumpsql):
    """
    Parse mysql dump file to get CREATE TABLE command of each table
    :param dumpsql: mysql dump file
    :return: dictionary containing each table and CREATE TABLE command
    """
    all_tables = set()
    database = dict()

    # get all table names
    match = re.findall(r"CREATE TABLE `(\w+)`", dumpsql)
    if match:
        all_tables = set(match)

    # get all tables' CREATE TABLE commands
    for table in all_tables:
        match = re.search(rf"CREATE TABLE `{table}` .*?;", dumpsql, re.DOTALL)
        if match:
            database[table] = match.group(0)

    return database


def generate_modify_tables_sql(source_dump, target_dump):
    """
    Generate "ALTER TABLE" commands to modify updated tables when merging source to target
    :param source_dump: source database mysql dump file
    :param target_dump: target database mysql dump file
    :return: sql commands modifying tables shared by source and target databases
    """
    comment = "-- Modify structures of updated tables:\n"
    commands = []
    source_db = _parse_dump(source_dump)
    target_db = _parse_dump(target_dump)

    # intersection set: tables that both databases have
    inter_tables = set(source_db.keys()) & set(target_db.keys())

    for table in inter_tables:
        source_table_def = utils.parse_table_definition(source_db.get(table))
        target_table_def = utils.parse_table_definition(target_db.get(table))
        table_updates = utils.get_table_updates(source_table_def, target_table_def)
        command = utils.generate_modify_table_sql(table, table_updates)
        commands.append(command)

    return comment + "".join(commands) + "\n"


def generate_merge_sql(source_dump, target_dump):
    """
    Generate sql commands merging source database to target database
    :param source_dump: source database mysql dump file
    :param target_dump: target database mysql dump file
    :return: sql commands merging source database to target database
    """
    merge_sql = ""
    merge_sql += generate_create_tables_sql(source_dump, target_dump)
    merge_sql += generate_drop_tables_sql(source_dump, target_dump)
    merge_sql += generate_modify_tables_sql(source_dump, target_dump)

    return merge_sql
