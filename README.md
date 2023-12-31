# MySQLMergeTool

## Usage
The MySQL Merge Tool is designed to compare two databases and generate an SQL file containing SQL commands to modify
definitions/structures of tables in the ***target*** database (merging source into target). The purpose of this tool is 
to improve data integrity and efficiency by updating tables in target database instead of recreating them using 
"DROP TABLE IF EXISTS x; CREATE TABLE x" command. The tool increases flexibility by adopting a command line approach


## Instructions
### To install requirements
```
pip install -r requirements.txt
```

### Command line
There are two options: 1) compare dump files, **OR** 2) compare the two databases directly:

#### 1) Option 1 - Compare databases (source -> target) using mysql dump files
```
python merge_sql_generator.py dump ...
```
Specify paths to source and target mysql dump files after `dump` using the following flags:

* `-s` source mysql dump file path
* `-t` target mysql dump file path

#### 2) Option 2 - Compare connected databases (source -> target) directly
```
python merge_sql_generator.py conn ...
```
Specify configurations of source and target databases after `conn` using the following flags:

* `-sh` source hostname 
* `-su` source username 
* `-sp` source password 
* `-sd` source sub-database
* `-th` target hostname 
* `-tu` target username 
* `-tp` target password 
* `-td` target sub-database

Adjust the command to use the specific Python interpreter version (python3, etc.)

### Output
Based on the differences found during the comparison between the two databases, an SQL file named `merge_sql.sql` 
containing the necessary SQL commands to modify the ***target*** database will be generated under the root directory. 
The SQL file includes statements to add, modify, or drop tables, columns, constraints in the target database as 
necessary. The SQL file does not contain or update entries.

## Examples
| Source Database | Target Database |
|----|----|
| <img src="Examples/source_schema.png" width=320> | <img src="Examples/target_schema.png" width=320> |

#### 1) Option 1 - Merge by comparing given dump files
- Input: [source](Examples/source_db.sql) and [target](Examples/target_db.sql) dump files
- Command:
  ```
  python merge_sql_generator.py dump -s Examples/source_db.sql -t Examples/target_db.sql
  ```

#### 2) Option 2 - Merge by connecting and comparing databases 
- Input: None
- Command: 
  ```
  python merge_sql_generator.py conn -sh localhost -su root -sp "" -sd source -th localhost -tu root -tp "" -td target
  ```

#### Output: [merge_sql.sql](Examples/merge_sql.sql)

## Limitations
- The tool's ability to generate accurate modification statements for complex table **constraints** may be limited. It is 
recommended to review the constraint modification statements as needed
- The tool supports MySQL databases only as it was developed based on MySQL commands
- The tool is designed to handle updates to definitions of tables and columns, and does not support other database
objects such as views, stored procedures, functions, etc.
