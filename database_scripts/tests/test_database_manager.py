import unittest

from database_scripts import database_manager as dm
from database_scripts import database_setup as ds

eta_db = dm.database_manager("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")

class test_db_setup(unittest.TestCase):

    def test_connect_engine(self):
        self.assertEqual(str(type(eta_db.connect_engine())), "<class 'sqlalchemy.engine.base.Engine'>")

    def test_db_name(self):
        self.assertEqual(eta_db.get_dbname(), 'eta')

    def test_db_uri(self):
        self.assertEqual(eta_db.get_uri(), 'eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com')

    def test_db_port(self):
        self.assertEqual(eta_db.get_port(), '3306')

    def test_set_db_name(self):
        self.assertEqual(eta_db.set_dbname('eta_database'), 'eta_database')

    def test_set_port(self):
        self.assertEqual(eta_db.set_port(80), 80)
        self.assertEqual(eta_db.set_port(3306), 3306)

    def test_set_uri(self):
        self.assertEqual(eta_db.set_uri('example'), 'example')

    def test_set_username(self):
        self.assertEqual(eta_db.set_username('eta_new'), 'eta_new')

    def test_get_tables(self):
        tables_list = ['bus_stops', 'timetables_dublin_bus', 'routes']
        expected_tables = [('bus_stops',), ('routes',), ('timetables_dublin_bus',)]
        expected_table_details = [[('stop_id', 'int(11)', 'NO', 'PRI', None, ''),
        ('stop_address', 'varchar(100)', 'NO', '', None, ''),
        ('latitude', 'float', 'NO', '', None, ''),
        ('longitude', 'float', 'NO', '', None, ''),
        ('year', 'int(11)', 'NO', 'PRI', None, '')
        ], [('journey_pattern', 'varchar(45)', 'NO', 'PRI', None, ''),
        ('first_stop', 'int(11)', 'NO', '', None, ''),
        ('last_stop', 'int(11)', 'NO', '', None, ''),
        ('day_category', 'varchar(45)', 'NO', 'PRI', None, ''),
        ('departure_time', 'time', 'NO', 'PRI', None, ''),
        ('line', 'varchar(45)', 'NO', '', None, '')
        ],[('journey_pattern', 'varchar(100)', 'NO', 'PRI', None, ''),
        ('stop_id', 'int(11)', 'NO', 'PRI', None, ''),
        ('position_on_route', 'float', 'NO', '', None, ''),
        ('line_id', 'varchar(10)', 'NO', '', None, '')
        ]]
        response = eta_db.get_tables(tables_list=tables_list)
        self.assertEqual(expected_tables, response[0])
        self.assertEqual(expected_table_details, response[1])


    def test_get_tables_no_list(self):
        expected_response = [('bus_stops',), ('routes',), ('timetables_dublin_bus',)]
        response = eta_db.get_tables(tables_list=None)
        print(type(expected_response))
        print("EXPECTED", expected_response)
        print(type(response))
        print("RESPONSE", response)
        self.assertEqual(expected_response, response)
