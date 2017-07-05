import datastore
from datastore import process_command
import unittest

class TestDataStore(unittest.TestCase):
  def test_no_dbs(self):
    dbs = datastore.Databases()

    self.assertEqual(process_command(dbs, 'list databases'), 'No databases')
    self.assertEqual(process_command(dbs, 'use database db'), 'Invalid database')
    self.assertEqual(process_command(dbs, 'create table tb (a b c, int int int)'), 'Database not set')
    self.assertEqual(process_command(dbs, 'list tables'), 'Database not set')
    self.assertEqual(process_command(dbs, 'insert into tb (a b c, 5 6 7)'), 'Database not set')
    self.assertEqual(process_command(dbs, 'select * from tb1 where a = 5'), 'Database not set')

  def test_invalid_commands(self):
    dbs = datastore.Databases()

    self.assertEqual(process_command(dbs, 'nothing'), 'Invalid command')
    self.assertEqual(process_command(dbs, 'create databases a d'), 'Invalid command')
    self.assertEqual(process_command(dbs, 'list databases a'), 'Invalid command')
    self.assertEqual(process_command(dbs, 'use database db a'), 'Invalid command')
    self.assertEqual(process_command(dbs, 'create table tb (a b c int int int)'), 'Invalid command')
    self.assertEqual(process_command(dbs, 'list tables s'), 'Invalid command')
    self.assertEqual(process_command(dbs, 'insert into tb (a b c 5 6 7)'), 'Invalid command')
    self.assertEqual(process_command(dbs, 'select * from tb1 where a  5'), 'Invalid command')

  def test_create_db(self):
    dbs = datastore.Databases()

    self.assertEqual(process_command(dbs, 'create database db'), 'Database created')
    self.assertEqual(process_command(dbs, 'list databases'), 'db')
    self.assertEqual(process_command(dbs, 'use database db'), 'Current database db')
    self.assertEqual(process_command(dbs, 'create database db'), 'Database exists')

  def test_invalid_table(self):
    dbs = datastore.Databases()

    self.assertEqual(process_command(dbs, 'create database db'), 'Database created')
    self.assertEqual(process_command(dbs, 'use database db'), 'Current database db')
    self.assertEqual(process_command(dbs, 'insert into tb (a, 5)'), 'Table does not exist')
    self.assertEqual(process_command(dbs, 'select * from tb where a = 5'), 'Table does not exist')
    self.assertEqual(process_command(dbs, 'create table tb (a b, int varchar)'), 'Table created')
    self.assertEqual(process_command(dbs, 'select * from tb where c = 4'), 'Column does not exist')
    self.assertEqual(process_command(dbs, 'insert into tb (c, 4)'), 'Column does not exist')
    self.assertEqual(process_command(dbs, 'create table tb2 (a a, int varchar)'), 'Duplicate columns')
    self.assertEqual(process_command(dbs, "insert into tb (a, 'hello')"), 'Invalid number')
    self.assertEqual(process_command(dbs, "insert into tb (b, 45)"), 'Invalid string')
    self.assertEqual(process_command(dbs, "insert into tb (a a, 45 56)"), 'Duplicate columns')
    self.assertEqual(process_command(dbs, "insert into tb (a b, 45 'dog' 'hello')"), 'Invalid number of insert values')
    self.assertEqual(process_command(dbs, 'create table tb (a a, int int)'), 'Table already exists')
    self.assertEqual(process_command(dbs, 'create table tb3 (a a, int)'), 'Column name and type mismatch')
    self.assertEqual(process_command(dbs, 'create table tb3 (a@#, int)'), 'Invalid column name')
    self.assertEqual(process_command(dbs, 'create table tb3 (a, float)'), 'Invalid column type')

  def test_int_table(self):
    dbs = datastore.Databases()

    self.assertEqual(process_command(dbs, 'create database db'), 'Database created')
    self.assertEqual(process_command(dbs, 'create table tb (a b, int int)'), 'Database not set')
    self.assertEqual(process_command(dbs, 'use database db'), 'Current database db')
    self.assertEqual(process_command(dbs, 'create table tb (a b, int int)'), 'Table created')
    self.assertEqual(process_command(dbs, 'list tables'), 'tb')
    self.assertEqual(process_command(dbs, 'insert into tb (a b, 4 5)'), 'Inserted into table')
    self.assertEqual(process_command(dbs, 'select * from tb where a = 4'), 'a,b\n4,5')
    self.assertEqual(process_command(dbs, 'select * from tb where a = 5'), 'a,b')
    self.assertEqual(process_command(dbs, 'insert into tb (a, 4)'), 'Inserted into table')
    self.assertEqual(process_command(dbs, 'select * from tb where a = 4'), 'a,b\n4,5\n4,None')

  def test_varchar_table(self):
    dbs = datastore.Databases()

    self.assertEqual(process_command(dbs, 'create database db'), 'Database created')
    self.assertEqual(process_command(dbs, 'create table tb (a b, int int)'), 'Database not set')
    self.assertEqual(process_command(dbs, 'use database db'), 'Current database db')
    self.assertEqual(process_command(dbs, 'create table tb (a b, varchar varchar)'), 'Table created')
    self.assertEqual(process_command(dbs, "insert into tb (a b, 'a val' 'b val')"), 'Inserted into table')
    self.assertEqual(process_command(dbs, "select * from tb where a = 'a val'"), 'a,b\na val,b val')
    self.assertEqual(process_command(dbs, "select * from tb where a = 'n'"), 'a,b')
    self.assertEqual(process_command(dbs, "insert into tb (a, 'a val')"), 'Inserted into table')
    self.assertEqual(process_command(dbs, "select * from tb where a = 'a val'"), 'a,b\na val,b val\na val,None')

  def test_none_select(self):
    dbs = datastore.Databases()

    self.assertEqual(process_command(dbs, 'create database db'), 'Database created')
    self.assertEqual(process_command(dbs, 'create table tb (a b, int int)'), 'Database not set')
    self.assertEqual(process_command(dbs, 'use database db'), 'Current database db')
    self.assertEqual(process_command(dbs, 'create table tb (a b, int int)'), 'Table created')
    self.assertEqual(process_command(dbs, 'insert into tb (a, 4)'), 'Inserted into table')
    self.assertEqual(process_command(dbs, "select * from tb where a = None"), 'a,b')
    self.assertEqual(process_command(dbs, "select * from tb where b = None"), 'a,b\n4,None')

  def test_int_varchar_table(self):
    dbs = datastore.Databases()
    
    self.assertEqual(process_command(dbs, 'create database db'), 'Database created')
    self.assertEqual(process_command(dbs, 'use database db'), 'Current database db')
    self.assertEqual(process_command(dbs, 'create table tb (a b, int varchar)'), 'Table created')
    self.assertEqual(process_command(dbs, "insert into tb (a b, 4 'col b')"), 'Inserted into table')
    self.assertEqual(process_command(dbs, "select * from tb where a = 4"), 'a,b\n4,col b')

if __name__ == '__main__':
    unittest.main()