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
               Column('stop_id', Integer, ForeignKey("bus_stops.stop_id"), primary_key=True, nullable=False),
               Column('position_on_route', FLOAT, nullable=False),
               Column('line_id', String, nullable=False))

class database_manager():
    def __init__(self, password_file, URI, PORT, USERNAME, DB_NAME):
        self.__password_file = password_file
        self.__URI = URI
        self.__PORT = PORT
        self.__USERNAME = USERNAME
        self.__DB_NAME = DB_NAME

    def __str__(self):
        return "Database Name: " + self.__DB_NAME + " on port: " + self.__PORT + " \nEndpoint: " + self.__URI

    def get_uri(self):
        return self.__URI

    def set_uri(self, uri_new):
        self.__URI = uri_new
        return self.__URI

    def get_port(self):
        return self.__PORT

    def set_port(self, port_new):
        self.__PORT = port_new
        return self.__PORT

    def get_username(self):
        return self.__USERNAME

    def set_username(self, username_new):
        self.__USERNAME = username_new
        return self.__USERNAME

    def get_dbname(self):
        return self.__DB_NAME

    def set_dbname(self, dbname_new):
        self.__DB_NAME = dbname_new
        return self.__DB_NAME

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
        sql = "SHOW TABLES;"
        eng = self.connect_engine()
        tables = eng.execute(sql).fetchall()
        if (len(tables) < 1):
            print("This database is empty")
            eng.dispose()
            return None
        else:
            print("Tables in the Database: \n", tables)
            if tables_list != None:
                table_descriptions = []
                for table in tables_list:
                    sql = "DESCRIBE " + table + ";"
                    table_desc = eng.execute(sql).fetchall()
                    print("Table Name: ", table, "\nTable Details: ", table_desc)
                    table_descriptions.append(table_desc)

                eng.dispose()
                return tables, table_descriptions
            else:
                return tables


