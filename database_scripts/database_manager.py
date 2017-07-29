"""
Python Module that deals with connection management for the db
"""

from sqlalchemy import *

metadata = MetaData()

bus_stops = Table('bus_stops', metadata,
                  Column('stop_id', Integer, primary_key=True, nullable=False),
                  Column('stop_address', String, nullable=False),
                  Column('latitude', FLOAT, nullable=False),
                  Column('longitude', FLOAT, nullable=False),
                  Column('year', Integer, primary_key=True, nullable=False))

routes = Table('routes', metadata,
              Column('journey_pattern', String, primary_key=True, nullable=False),
               Column('stop_id', Integer, primary_key=True, nullable=False),
               Column('position_on_route', FLOAT, nullable=False))

class database_manager():
    def __init__(self, password_file, URI, PORT, USERNAME, DB_NAME):
        self.__password_file = password_file
        self.__URI = URI
        self.__PORT = PORT
        self.__USERNAME = USERNAME
        self.__DB_NAME = DB_NAME

    def __str__(self):
        return "Database Name: " + self.__DB_NAME + " on port: " + self.__PORT

    def connect_engine(self):
        """
        :return: an engine connection pool for the database object
        """
        fh = open(self.__password_file)
        self.__PASSWORD = fh.readline().strip()
        eng = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(self.__USERNAME, self.__PASSWORD,
                self.__URI, self.__PORT, self.__DB_NAME))
        return eng

    def get_tables(self, tables_list):
        """
        :param tables_list: a list of the tables you want data for
        :return: a list of all tables in the database and descriptions
        for each of those tables
        """
        if len(tables_list) > 0:
            sql = "SHOW TABLES;"
            eng = self.connect_engine()
            tables = eng.execute(sql).fetchall()
            print("Tables in the Database: \n", tables)

            table_descriptions = []
            for table in tables_list:
                sql = "DESCRIBE " + table + ";"
                table_desc = eng.execute(sql).fetchall()
                print("Table Name: ", table, "\nTable Details: ", table_desc)
                table_descriptions.append(table_desc)

            eng.dispose()
            return tables, table_descriptions
        else:
            print("This database is empty")
            return None


