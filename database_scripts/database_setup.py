""" Python Module for Dealing with our Amazon RDS Instance """

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.mysql.types import FLOAT, TIME
import json
from database_scripts import database_manager
import pandas as pd


#################### Defining Tables for the Database ###############################
Base = declarative_base()

class Stop(Base):
    __tablename__ = 'bus_stops'

    stop_id = Column(Integer, primary_key=True, nullable=False)
    stop_address = Column(String(100), nullable=False)
    latitude = Column(FLOAT, nullable=False)
    longitude = Column(FLOAT, nullable=False)
    year = Column(Integer, primary_key=True, nullable=False)

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

    journey_pattern = Column(String(100), primary_key=True, nullable=False)
    stop_id = Column(Integer, ForeignKey("bus_stops.stop_id"), primary_key=True, nullable=False)
    position_on_route = Column(FLOAT, nullable=False)
    line_id = Column(String(10), nullable=False)

    def __repr__(self):
        return """
        <Route=(journey_pattern=%s,
        stop_id=%s,
        position_on_route=%s,
        line_id=%s>
        """%(self.journey_pattern, self.stop_id, self.position_on_route, self.line_id)

class Timetable(Base):
    __tablename__ = 'timetables_dublin_bus'

    journey_pattern = Column(String(100), ForeignKey("routes.journey_pattern"), primary_key=True, nullable=False)
    departure_time = Column(TIME, primary_key=True, nullable=False)
    day_category = Column(String(100), primary_key=True, nullable=False)
    first_stop = Column(Integer, nullable=False)
    last_stop = Column(Integer, nullable=False)
    line = Column(String(10), nullable=False)

    def __repr__(self):
        return """
        <Timetable=(journey_pattern=%s,
        departure_time=%s,
        day_category=%s,
        first_stop=%s,
        last_stop=%s,
        line=%s>
        """%(self.journey_pattern, self.departure_time, self.day_category, self.first_stop, self.last_stop, self.line)


#####################################################################################

class database_setup():

    def __init__(self, db_mang):
        """
        :param db_mang: a database manager object
        """
        self.__db = db_mang

    def create_tables(self):
        """A function to create the tables defined above """
        eng = self.__db.connect_engine()
        Base.metadata.create_all(eng)
        eng.dispose()

    def clear_database(self):
        eng = self.__db.connect_engine()
        for tbl in reversed(Base.metadata.sorted_tables):
            print(tbl)
            eng.execute(tbl.delete())
        eng.dispose()

    def populate_stops(self, stop_info, year):
        """
        :param stop_info: location for a file that can be loaded as json
        :param year: the year
        updates the stops table with all the stops in the stop_info file
        """
        engine = self.__db.connect_engine()
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

    def populate_routes(self, route_info):
        """
        :param route_info: location for a file that can be loaded as json
        updates the routes table with all the stops in the stop_info file
        """
        engine = self.__db.connect_engine()
        Session = sessionmaker(bind=engine)
        session = Session()

        with open(route_info) as f:
            routes_json = json.load(f)
        for route in routes_json:
            print(route)
            stop_ids = routes_json[route].keys()
            for stop in stop_ids:
                if stop == "line_id":
                    line_id = routes_json[route]["line_id"]
                else:
                    distance = routes_json[route][stop]
                    try:
                        route_obj = Route(journey_pattern=route,
                                    stop_id=stop,
                                    position_on_route=distance,
                                    line_id = line_id)

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

    def populate_timetables(self, timetable_info):
        """
        :param timetable_info: a pandas dataframe with all timetable information
        :return: updates the database with the timetable information
        """
        engine = self.__db.connect_engine()
        Session = sessionmaker(bind=engine)
        session = Session()

        print("Number of rows: ", timetable_info.shape)

        for row in timetable_info.iterrows():
            journey_pattern = row[1]['journey_pattern']
            departure_time = row[1]['departure_time']
            day_category = row[1]['day_category']
            line_id = row[1]['line']
            first_stop = row[1]['first_stop']
            last_stop = row[1]['last_stop']

            try:
                timetable_object = Timetable(journey_pattern=journey_pattern,
                                             departure_time=departure_time,
                                             day_category=day_category,
                                             first_stop=first_stop,
                                             last_stop=last_stop,
                                             line=line_id)

                session.add(timetable_object)
                session.commit()

            except Exception as e:
                print("Error in the populate timetables function")
                print("Error type: ", type(e))
                print("Error details: ", e)
                session.rollback()
                continue

        session.close()
        engine.dispose()

    def send_query(self, query):
        """Sends a query to the database given the sql query"""
        eng = self.__db.connect_engine()
        eng.execute(query)
        eng.dispose()
        return


def main():
    """Run all steps necessary to create and populate the database"""
    db_obj = database_manager.database_manager("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
    db = database_setup(db_obj)
    print("Creating Tables...")
    db.create_tables()
    print("Populating stops...")
    db.populate_stops('../datasets/output_files/clean_stops_2017.txt', 2017)
    db.populate_stops('../datasets/input_files/2012_stops.txt', 2012)
    print("Populating routes...")
    db.populate_routes('../datasets/output_files/routes.txt')
    db_obj.get_tables(tables_list=['bus_stops', 'routes'])
    timetables = pd.read_csv('../datasets/output_files/time_table_2.csv', dtype={'Journey_Pattern_ID':object})
    print("Populating timetables...")
    db.populate_timetables(timetables)

main()



