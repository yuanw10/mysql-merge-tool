# MySQLMergeTool

## Description
The MySQL Merge Tool is designed to compare two databases, merge the source database into the target database, 
and generate an SQL file containing SQL commands to modify the target database. The purpose of this tool is to improve
data integrity and efficiency by updating tables in target database instead of recreating them using "DROP TABLE 
IF EXISTS x; CREATE TABLE x" command. 


## Usage
> python merge_sql_generator.py dump ...

or

> python merge_sql_generator.py conn ...

### 1) To merge databases (source -> target) using mysql dump files
Specify paths to source and target mysql dump files after `dump` 
using the following flags:

* `-s` source mysql dump file path
* `-t` target mysql dump file path

Example running:
> python merge_sql_generator.py dump -s src_dump_path -t tgt_dump_path


### 2) To merge connected databases (source -> target)
Specify configurations of source and target databases after `conn` 
using the following flags:

* `-sh` source hostname 
* `-su` source username 
* `-sp` source password 
* `-sd` source sub-database
* `-th` target hostname 
* `-tu` target username 
* `-tp` target password 
* `-td` target sub-database

Example running:
> python merge_sql_generator.py conn -sh localhost -su root -sp 123 -sd source -th localhost -tu root -tp 123 -td target

### 3) Output
Based on the differences found during the comparison between the two databases, an SQL file named `merge_sql.sql` 
containing the necessary SQL commands to modify the target database will be generated under the root directory. 
The SQL file includes statements to add, modify, or delete database objects (tables, columns, constraints) as necessary

## Limitations
- The tool's ability to generate accurate modification statements for complex table **constraints** may be limited. It is 
recommended to review the constraint modification statements as needed
- The tool supports MySQL databases only as it was developed based on MySQL commands