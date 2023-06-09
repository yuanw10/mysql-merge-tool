import re
from deepdiff import DeepDiff


def parse_table_definition(create_table_sql):
    """
    Parse CREATE TABLE sql command to get table definition
    :param create_table_sql: CREATE TABLE sql command
    :return: dictionary containing column-level definition and constraints
    """
    cols_def_dict = dict()
    constraints = set()

    # extract content inside ()
    queries_match = re.search(r"\(.*\)", create_table_sql, re.DOTALL)
    if queries_match:
        content = queries_match.group(0).strip()[1:-1].strip()
        queries = set(line.strip(" ,") for line in content.split("\n"))
        columns_def = set(query for query in queries if query.startswith("`"))
        constraints = queries - columns_def
        cols_def_dict = {item.split()[0].strip("`"): item for item in columns_def}

    table_def = {"cols_def": cols_def_dict, "constraints": constraints}
    return table_def


def get_columns_modifications(source_cols_def, target_cols_def):
    """
    Get columns-level modifications to make on target table when mering source table into target
    :param source_cols_def: column-level definition of source table
    :param target_cols_def: column-level definition of target table
    :return: dictionary showing which columns and what modifications to make on target table
    """
    diff = DeepDiff(target_cols_def, source_cols_def)
    diff_dict = diff.to_dict()
    modifications = dict(added=[], removed=[], changed=[])

    # iterate all columns, get how they are modified
    for column in diff.affected_root_keys:
        root_key = f"root['{column}']"
        if root_key in diff_dict.get('dictionary_item_added', set()):
            modifications.get("added").append(source_cols_def.get(column))
        elif root_key in diff_dict.get('dictionary_item_removed', set()):
            modifications.get("removed").append(column)
        elif root_key in diff.get("values_changed", {}).keys():
            modifications.get("changed").append(source_cols_def.get(column))

    return modifications


def get_table_updates(source_table_def, target_table_def):
    """
    Get updates to make on target table to merge source to target
    :param source_table_def: definition of source table
    :param target_table_def: definition of target table
    :return: updates need to make on target table
    """
    table_updates = {
        "cols_updates": get_columns_modifications(source_table_def.get("cols_def"), target_table_def.get("cols_def")),
        "added_constraints": source_table_def.get("constraints") - target_table_def.get("constraints"),
        "removed_constraints": target_table_def.get("constraints") - source_table_def.get("constraints"),
    }

    return table_updates


def generate_modify_table_sql(table, updates):
    """
    Generate "ALTER TABLE" sql command to modify one updated table when merging source to target
    :param table: table that needs to be modified
    :param updates: dictionary containing what modifications to make on this table
    :return: sql commands modifying specified table
    """
    commands = []

    cols_updates = updates.get("cols_updates")
    added_constraints = updates.get("added_constraints")
    removed_constraints = updates.get("removed_constraints")

    # append sql commands modifying columns
    for added_col in cols_updates.get("added"):
        command = f"ALTER TABLE {table} ADD {added_col}"
        commands.append(f"{command};\n")
    for changed_col in cols_updates.get("changed"):
        command = f"ALTER TABLE {table} MODIFY {changed_col}"
        commands.append(f"{command};\n")
    for removed_col in cols_updates.get("removed"):
        command = f"ALTER TABLE {table} DROP {removed_col}"
        commands.append(f"{command};\n")

    if len(added_constraints) or len(removed_constraints):
        commands.append("-- Errors might be thrown if constraints are complicated\n"
                        "-- Double check when executing\n")

    # append commands adding new constraints
    for constraint in added_constraints:
        command = f"ALTER TABLE {table} ADD {constraint}"
        commands.append(f"{command};\n")

    # append commands removing new constraints
    for constraint in removed_constraints:
        command = f"ALTER TABLE {table} DROP {constraint}"
        commands.append(f"{command};\n")

    return "".join(commands)
