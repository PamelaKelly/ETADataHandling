import unittest

from database_helper import database_setup

eta_db = database_setup("db_password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")

class test_db_setup(unittest.TestCase):
    def test_engine(self):
        self.assertEqual(str(type(eta_db.db_engine())), "<class 'sqlalchemy.engine.base.Engine'>")

    def test_db_name(self):
        self.assertEqual(eta_db.get_dbname(), 'eta')

    def test_db_uri(self):
        self.assertEqual(eta_db.get_uri(), 'eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com')

    def test_db_port(self):
        self.assertEqual(eta_db.get_port(), '3306')

    def test_set_db_name(self):
        self.assertEqual(eta_db.set_db_name('eta_database'), 'eta_database')

    def test_set_port(self):
        self.assertEqual(eta_db.set_port(80), 80)
        self.assertEqual(eta_db.set_port(3306), 3306)

    def test_set_uri(self):
        self.assertEqual(eta_db.set_uri('example'), 'example')

    def test_set_username(self):
        self.assertEqual(eta_db.set_username('eta_new'), 'eta_new')

    def test_create_table_success(self):
        self.assertEqual(eta_db.create_table('test_1', ['test_column1 INT NOT NULL', 'test_column_2 INT NOT NULL', 'test_column_3 VARCHAR(11)'], 'test_column_1', foreign_key=None, ref_table=None), 'Table test_1 created.')

    def test_create_table_missing_arg(self):
        self.assertEqual(eta_db.create_table('test_1', ['t1', 't2', 't3'], 't1', 't2'), 'TypeError')

    def test_create_table_incorrect_pk(self):
        # test that exception is caught if no primary key is provided
        self.assertEqual(eta_db.create_table('test_2', ['test_column1 INT NOT NULL', 'test_column_2 INT NOT NULL'], 'test', foreign_key=None, ref_table=None), "<class 'sqlalchemy.exc.ProgrammingError'>")

    def test_create_table_column(self):
        # tests that exception is caught if no columns are provided
        self.assertEqual(eta_db.create_table('test_3', None, 'test_column1'))

    def test_drop_table_success(self):
        eta_db.create_table('test', ['c1', 'c2', 'c3'], 'c1')
        self.assertEqual(eta_db.drop_table('test'), 1)

    def test_drop_table_fail(self):
        self.assertEqual(eta_db.drop_table('hello'), 0)

    def test_insert_stop(self):
        pass

    def test_delete(self):
        pass

    def test_send_query(self):
        pass

    def test_static_stops(self):
        pass

    def test_populate_stops(self):
        pass

# 0 usually the 'all-clear'