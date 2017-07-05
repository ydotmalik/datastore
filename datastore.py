import re

# Created Exception classes for common occuring exceptions
# Did not create class for exceptions that weren't used more than once
class CommandNotValidException(Exception):
  def __str__(self):
    return 'Invalid command'

# Base class for database exceptions
class DatabaseException(Exception):
  pass

class DatabaseNotSetException(DatabaseException):
  def __str__(self):
    return 'Database not set'

class TableNotExistException(DatabaseException):
  def __str__(self):
    return 'Table does not exist'

class ColumnNotExistException(DatabaseException):
  def __str__(self):
    return 'Column does not exist'

class DuplicateColumnException(DatabaseException):
  def __str__(self):
    return 'Duplicate columns'

class InvalidNumberException(DatabaseException):
  def __str__(self):
    return 'Invalid number'

class InvalidStringException(DatabaseException):
  def __str__(self):
    return 'Invalid string'

class Databases:
  def __init__(self):
    self._dbs = {}
    self._cur_db = None

  def db_exists(self, db_name):
    """ Determines whether a database exists """
    return db_name in self._dbs

  def add_db(self, db_name):
    """ Adds a database """
    self._dbs[db_name] = {}

  def get_db_names(self):
    """ Returns all the databases created """
    return self._dbs.keys()

  def set_cur_db(self, db_name):
    """ Sets the current database to @db_name """
    if db_name in self._dbs:
      self._cur_db = self._dbs[db_name]
    else:
      raise ValueError('Invalid database')

  def add_table(self, table_name, col_names, col_types):
    """ Creates a new table table

    table_name: name of table
    col_names: array of column names
    col_types: array of column types
    """
    if self._cur_db == None:
      raise DatabaseNotSetException

    if table_name in self._cur_db:
      raise ValueError('Table already exists')

    self._cur_db[table_name] = Table(col_names, col_types)

  def get_cur_db_tables(self):
    """ Gets all the tables for the current database """
    if self._cur_db == None:
      raise DatabaseNotSetException

    return self._cur_db.keys()

  def insert_table_row(self, table_name, col_names, col_values):
    """ Insert a row into a table 

    table_name: name of table
    col_names: array of column names
    col_values: string representing column values
    """
    if self._cur_db == None:
      raise DatabaseNotSetException

    if not table_name in self._cur_db:
      raise TableNotExistException

    self._cur_db[table_name].insert(col_names, col_values)

  def select_rows(self, table_name, col_name, col_value):
    if self._cur_db == None:
      raise DatabaseNotSetException

    if not table_name in self._cur_db:
      raise TableNotExistException

    return self._cur_db[table_name].select_rows(col_name, col_value)

class Table:
  def __init__(self, col_names, col_types):
    """ Constructor
    
    col_names: array of column names
    col_types: array of column types
    """
    if len(col_types) != len(col_names):
      raise ValueError('Column name and type mismatch')

    # All column names should be alpha numeric
    for col in col_names:
      if not col.isalnum():
        raise ValueError('Invalid column name')

    # All column types should be either int or varchar
    for col in col_types:
      if not col.lower() in ['int', 'varchar']:
        raise ValueError('Invalid column type')

    # All column names should be unique
    if len(col_names) != len(set(col_names)):
      raise DuplicateColumnException

    # Maps column names to their types
    self._cols = dict(zip(col_names, col_types))

    # An array of dictionaries containing all the rows in the table
    self._rows = []

  def __do_columns_exist(self, col_names):
    """Determine whether table has @col_names exists

    col_names: array of column names
    """
    for col in col_names:
      if not col in self._cols:
        return False

    return True

  def insert(self, col_names, col_values):
    """Attempt to insert into table

    col_names: list of column names
    col_values: string of column values

    If there aren't enough values in col_values, don't insert and throw an exception
    """
    if not self.__do_columns_exist(col_names):
      raise ColumnNotExistException

    # Check for duplicate column names
    if len(col_names) != len(set(col_names)):
      raise DuplicateColumnException

    insert_dict = {}

    # By default, column values are null unless if column exists in col_names
    for col in self._cols:
      insert_dict[col] = None

    for col in col_names:
      if self._cols[col] == 'int':
        # '^' is needed to avoid case of number inside quoted string
        search = re.search('^(-?\d+)', col_values)

        if search == None:
          raise InvalidNumberException

        insert_dict[col] = int(search.group(1))

        # For the next time around, start parsing after just parsed number
        col_values = col_values[len(search.group(1)):].strip()
      else:
        # This has to be varchar
        # TO DO: support escaping of '
        search = re.search("^'(.*?)'", col_values)
 
        if search == None:
          raise InvalidStringException

        insert_dict[col] = search.group(1)

        # Start parsing starting after current string.  '+ 2' to ignore single quotes
        col_values = col_values[len(search.group(1)) + 2:].strip()

    if len(col_values) != 0:
      raise ValueError('Invalid number of insert values')

    self._rows.append(insert_dict)

  def select_rows(self, col_name, col_value):
    out = ','.join(sorted(self._cols.keys()))

    if not col_name in self._cols:
      raise ColumnNotExistException

    for row in self._rows:
      if col_value.strip() == 'None':
        if row[col_name] == None:
          out = out + '\n' + ','.join(map(lambda x: str(x[1]), sorted(row.items())))
      elif self._cols[col_name] == 'int':
        if row[col_name] == int(col_value):
          out = out + '\n' + ','.join(map(lambda x: str(x[1]), sorted(row.items())))
      else:
        search = re.search("^'(.*?)'", col_value.strip())

        if search == None:
          raise InvalidStringException

        if row[col_name] == search.group(1):
          out = out + '\n' + ','.join(map(lambda x: str(x[1]), sorted(row.items())))

    return out

def try_create_db(dbs, command):
  """ Implements create database <database_name> command """
  search = re.search('create +database +(\w+) *$', command, re.IGNORECASE)

  try:
    db_name = search.group(1)
  except:
    raise CommandNotValidException

  if dbs.db_exists(db_name):
    raise ValueError('Database exists')

  dbs.add_db(db_name)

  return 'Database created'

def try_list_dbs(dbs, command):
  """ Implements list databases command """
  search = re.search('list +databases *$', command, re.IGNORECASE)

  if search == None:
    raise CommandNotValidException

  db_names = dbs.get_db_names()

  if len(db_names) == 0:
    return 'No databases'
  else:
    return ' '.join(db_names)

def try_use(dbs, command):
  """ Implements use database <database_name> command """
  search = re.search('use +database +(\w+) *$', command, re.IGNORECASE)

  if search == None:
    raise CommandNotValidException

  dbs.set_cur_db(search.group(1))

  return 'Current database ' + search.group(1)

def try_create_table(dbs, command):
  """ Implements create table <table_name> (col1 col2 ..., col1_type col2_type ...) """
  search = re.search('create +table +([a-z0-9]+) *\( *(.+) *, *(.+) *\) *$', command, re.IGNORECASE)

  if search == None or len(search.groups()) != 3:
    raise CommandNotValidException

  col_names = search.group(2).split(' ')
  col_types = search.group(3).split(' ')

  dbs.add_table(search.group(1), col_names, col_types)

  return 'Table created'

def try_list_tables(dbs, command):
  """ Implements list tables command """
  search = re.search('list +tables *$', command, re.IGNORECASE)

  if search == None:
    raise CommandNotValidException

  tables = dbs.get_cur_db_tables()

  if len(tables) == 0:
    return 'No tables'
  else:
    return ' '.join(tables)

def try_insert_into(dbs, command):
  """ Implements insert into <table name> (col1 col2 ..., col1_val, col2_val ...) """
  search = re.search('insert +into +([a-z0-9]+) *\( *(.+) *, *(.+) *\) *$', command, re.IGNORECASE)

  if search == None or len(search.groups()) != 3:
    raise CommandNotValidException

  col_names = search.group(2).split(' ')
  col_values = search.group(3)

  dbs.insert_table_row(search.group(1), col_names, col_values)

  return 'Inserted into table'

def try_select(dbs, command):
  """ Implements select * from <table name> col=value """
  search = re.search('select +\* +from +([a-z0-9]+) +where +([a-z0-9]+) *=(.*)', command, re.IGNORECASE)

  if search == None or len(search.groups()) != 3:
    raise CommandNotValidException

  return dbs.select_rows(search.group(1), search.group(2), search.group(3))

def process_command(dbs, command):
  # Try to determine whether command is valid by executing execute each command against a function.
  # If the function finds a syntax error exception, try executing the command against the next function,
  # until you run of out of functions
  # On success functions return a string, which is printed to console
  sub_cmds = [try_create_db, try_list_dbs, try_use, try_create_table, try_list_tables, try_insert_into, try_select]

  for sub_cmd in sub_cmds:
    try:
      return sub_cmd(dbs, command)
    except CommandNotValidException as e:
      pass
    except ValueError as e:
      return str(e)
    except DatabaseException as e:
      return str(e)

  return str(CommandNotValidException())

def main():
  dbs = Databases()

  while 1:
    command = raw_input('> ')

    print process_command(dbs, command)


if __name__ == "__main__":
  main()
