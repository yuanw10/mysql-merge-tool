import re


def generate_create_tables_queries(source_dump, target_dump):
    comment = "-- Create new tables:\n"
    queries = []
    source_db = _parse_dump(source_dump)
    target_db = _parse_dump(target_dump)
    new_tables = set(source_db.keys()) - set(target_db.keys())

    for table in new_tables:
        command = source_db.get(table)
        queries.append(command+"\n")
    return comment + "".join(queries) + "\n"


def generate_drop_tables_queries(source_dump, target_dump):
    comment = "-- Drop deleted tables:\n"
    queries = []
    source_db = _parse_dump(source_dump)
    target_db = _parse_dump(target_dump)
    dropped_tables = set(target_db.keys()) - set(source_db.keys())

    for table in dropped_tables:
        command = f"DROP TABLE {table}"
        queries.append(command+"\n")
    return comment + "".join(queries) + "\n"


def _parse_dump(dumpsql):
    all_tables = set()
    database = dict()

    match = re.findall(r"CREATE TABLE `(\w+)`", dumpsql)
    if match:
        all_tables = set(match)

    for table in all_tables:
        match = re.search(rf"CREATE TABLE `{table}` .*?;", dumpsql, re.DOTALL)
        if match:
            database[table] = match.group(0)

    return database


# TODO
def generate_modify_tables_queries(source_dump, target_dump):
    pass


def generate_merge_sql(source_dump, target_dump):
    merge_sql = ""
    merge_sql += generate_create_tables_queries(source_dump, target_dump)
    merge_sql += generate_drop_tables_queries(source_dump, target_dump)
    merge_sql += generate_modify_tables_queries(source_dump, target_dump)

    return merge_sql
