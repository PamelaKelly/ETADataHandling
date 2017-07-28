from database_helper import database_setup

def static_stops_table():
    try:
        db_obj = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
        db_obj.create_table('bus_stops', ['stop_id INT NOT NULL', 'stop_address VARCHAR(200) NOT NULL',
                                          'latitude FLOAT NOT NULL', 'longitude FLOAT NOT NULL', 'year INT NOT NULL'],
                            "stop_id, latitude, longitude", foreign_key=None, ref_table=None)
    except Exception as e:
        print("Error in the static_stops_tables() function")
        print("Error Type: ", type(e))
        print("Error Details: ", e)
        return 0

def populate_stops_table():
    """ Function to repopulate static stops table with 2012 and 2017 stops, in the case of loss of data """
    # Currently the data is pulled from a local file - should this data be backed up somewhere else ? in the cloud ?
    db = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
    db.insert_stops('2012_stops.txt', 2012)
    #db.insert_stops('stop_info_2017.txt', 2017)


def static_routes_table():
    try:
        db_obj = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
        db_obj.create_table('routes', ['journey_pattern VARCHAR(20) NOT NULL', 'stop_id INT NOT NULL',
                                       'distance_from_terminus INT'], "journey_pattern, stop_id", "stop_id", "bus_stops")

    except Exception as e:
        print("Error in the static_routes_table() function")
        print("Error Type: ", type(e))
        print("Error details: ", e)
        return 0

def populate_routes():
    """ Function to repopulate static stops table with 2012 stops, in the case of loss of data """
    db = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
    db.insert_routes('routes_new.txt')

def static_timetables_table():
    try:
        db_obj = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
        db_obj.create_table('timetables', ['journey_pattern varchar(20) NOT NULL','departure_time TIME NOT NULL', 'day_category varchar(20) NOT NULL'],
                            'journey_pattern, departure_time, day_category','journey_pattern', 'routes')
    except Exception as e:
        print("Error in the static_timetables_function()")
        print("Error Type: ", type(e))
        print("Error Details: ", e)
        return 0

def populate_routes():
    """ Function to repopulate static routes table with 2012 routes, in the case of loss of data """
    db = database_setup("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
    db.insert_routes('routes_new.txt', 'distances_new.txt')

def main():
    """ Sets up all of the tables int he database and populates them """
    static_stops_table()
    populate_stops_table()
    static_routes_table()
    populate_routes()
    static_timetables_table()
    populate_routes()

eta_db = database_setup("db_password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")
populate_routes()
eta_db.get_tables(tables=['routes'])
#sql = "SELECT * from routes where journey_pattern = '00010001';"
#eta_db.send_query(sql)
