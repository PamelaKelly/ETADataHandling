""" Python Module for Dealing with our Amazon RDS Instance """
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql.types import FLOAT, VARCHAR
import json
import os

Base = declarative_base()

class Stop(Base):
    __tablename__ = 'bus_stops'

    stop_id = Column(Integer, primary_key=True, nullable=False)
    stop_address = Column(String, nullable=False)
    latitude = Column(FLOAT, primary_key=True, nullable=False)
    longitude = Column(FLOAT, primary_key=True, nullable=False)
    year = Column(Integer, nullable=False)

    def __repr__(self):
        return """
        <Stop=(stop_id=%s,
        stop_address=%s,
        latitude=%s,
        longitude=%s,
        year=%s)>
        """%(self.stop_id, self.stop_address, self.latitude,
             self.longitude, self.year)

class Route(Base):
    __tablename__ = 'routes'

    journey_pattern = Column(String, primary_key=True, nullable=False)
    stop_id = Column(Integer, primary_key=True, nullable=False)
    position_on_route = Column(Integer)

    def __repr__(self):
        return """
        <Route=(journey_pattern=%s,
        stop_id=%s,
        position_on_route=%s>
        """%(self.journey_pattern, self.stop_id, self.position_on_route)

class database_setup():

    def __init__(self, password_file, URI, PORT, USERNAME, DB_NAME):
        self.__password_file = password_file
        self.__URI = URI
        self.__PORT = PORT
        self.__USERNAME = USERNAME
        self.__DB_NAME = DB_NAME
        self.__tables = []

    def get_uri(self):
        return self.__URI

    def get_dbname(self):
        return self.__DB_NAME

    def get_port(self):
        return self.__PORT

    def get_table_names(self):
        return self.__tables

    def set_uri(self, new_uri):
        self.__URI = new_uri
        return self.__URI

    def set_port(self, new_port):
        self.__PORT = new_port
        return self.__PORT

    def set_username(self, new_username):
        self.__USERNAME = new_username
        return self.__USERNAME

    def set_db_name(self, new_db_name):
        self.__DB_NAME = new_db_name
        return self.__DB_NAME

    def db_engine(self):
        fh = open(self.__password_file)
        self.__PASSWORD = fh.readline().strip()
        db_engine = create_engine("mysql+pymysql://{}:{}@{}:{}/{}".format(self.__USERNAME, self.__PASSWORD,
                self.__URI, self.__PORT, self.__DB_NAME))
        return db_engine

    def __str__(self):
        return "Database Name: " + self.__DB_NAME

    def get_tables(self, tables):
        try:
            full_table_description = []
            engine = self.db_engine()
            sql = "SHOW TABLES;"
            table_result = engine.execute(sql).fetchall()
            print("Tables in this Database: \n", table_result)
            if tables != None:
                for table in tables:
                    sql = "DESCRIBE " + table + ";"
                    result = engine.execute(sql).fetchall()
                    print("Table Details: \n", result)
                    full_table_description.append(result)
        except Exception as e:
            print("Error in the get_tables() function")
            print("Error Type: ", type(e))
            print("Error Details: ", e)
        finally:
            return full_table_description
            engine.dispose()

    def create_table(self, table_name, columns, primary_key, foreign_key, ref_table):
        try:
            engine = self.db_engine()
            sql = "CREATE TABLE IF NOT EXISTS " + table_name + " ("
            for i in range(len(columns)-1):
                sql += (str(columns[i]) + ", ")
            sql += (columns[len(columns)-1] + ") PRIMARY KEY " + primary_key)
            if foreign_key != None:
                sql += (", FOREIGN KEY (" + foreign_key + ") REFERENCES " + ref_table + "(" + foreign_key + "));")
            else:
                sql += ");"
            engine.execute(sql)
            return("Table " + table_name + " created.")
        except Exception as e:
            print("Error in the create_table() function")
            print("Error Type: ", type(e))
            print("Error Details: ", e)
            return(type(e))
        finally:
            engine.dispose()

    def drop_table(self, table):
        try:
            engine = self.db_engine()
            sql = "DROP TABLE `" + str(table) + "`;"
            engine.execute(sql)
        except Exception as e:
            print("Error in the drop_table() function")
            print("Error type: ", type(e))
            print("Error details: ", e)
            # throw e
        finally:
            engine.dispose()

    def delete(self, table, column, value):
        pass

    def insert_stops(self, stop_info, year):
        db = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
        engine = db.db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()

        with open(stop_info) as f:
            stops_json = json.load(f)
        for stop in stops_json:
            try:
                stop_obj = Stop(stop_id=stop,
                            stop_address=stops_json[stop]['stop_address'],
                            latitude=stops_json[stop]['latitude'],
                            longitude=stops_json[stop]['longitude'],
                            year=year)

                session.add(stop_obj)
                session.commit()
            except Exception as e:
                print("Error in the insert_stop() function")
                print("Error type: ", type(e))
                print("Error details: ", e)
                session.rollback()
                continue
        session.close()
        engine.dispose()

    def insert_routes(self, route_info):
        db = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
        engine = db.db_engine()
        Session = sessionmaker(bind=engine)
        session = Session()

        with open(route_info) as f:
            routes_json = json.load(f)
        count_routes = 0
        count_stops = 0
        for route in routes_json:
            count_routes += 1
            print("Route: ", route)
            stops = routes_json[route]["stops"]
            for stop in stops:
                try:
                    count_stops += 1
                    print("Stops: ", stop)
                    route_obj = Route(journey_pattern=route,
                                  stop_id=stop)

                    session.add(route_obj)
                    session.commit()

                except Exception as e:
                    print("Error in the insert_routes function")
                    print("Error Type: ", type(e))
                    print("Error Details: ", e)
                    session.rollback()
                    continue
        session.close()
        engine.dispose()

    def send_query(self, query):
        try:
            engine = self.db_engine()
            result = engine.execute(sql).fetchall()
            print(result)
        except Exception as e:
            print("Error in the send_query() function")
            print("Error type: ", type(e))
            print("Error details: ", e)
        finally:
            return result
            engine.dispose()
