# Datastore

The datastore was implemented using Python2.  Run by doing ```python datastore.py```.

## Commands

This datastore implements the following commands:

```create database <database_name>```

```list databases```

```use database <database_name>```

```create table <table_name> (col1 col2 col3..., col1_type col2_type col3_type...)```
The column type can either be ```int``` or ```varchar```

```insert into <table_name> (col1 col2..., col1_value col2_value...)```
If the column type is ```varchar```, the string must be enclosed in single quotes, i.e, ```'This is a varchar'```.

```select * from <table_name> col=value```
Similar to ```insert```, if the column value is a string, the value must be enclosed in single quotes.  The output is in CSV format.

## Unitest and code coverage

Install the ```coverage``` pip package.  First run ```coverage run unittest_datastore.py``` to execute all the test cases and then run ```coverage report -m``` to get the code coverage report.
