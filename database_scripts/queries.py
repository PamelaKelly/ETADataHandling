"""
Testing Queries for Front End
"""

from sqlalchemy.sql import *
from database_scripts.database_manager import bus_stops, routes, database_manager

db = database_manager("password.txt", "eta.cb0ofqejduea.eu-west-1.rds.amazonaws.com", "3306", "eta", "eta")

def route_selector():
    """Most Basic: Selects All Routes"""
    eng = db.connect_engine()
    s = select([routes])
    res = eng.execute(s).fetchall()
    for row in res:
        print(row)
    eng.dispose()
    return res

def single_route_ordered():
    """Selects a single route with correct stop order"""
    eng = db.connect_engine()
    s = select([routes]).where(routes.c.journey_pattern=='00010001').order_by('position_on_route')
    res = eng.execute(s).fetchall()
    for row in res:
        print(row)
    eng.dispose()
    return res

def connecting_stops(origin):
    """Selects all subsequent connecting stops on a route in order"""
    eng = db.connect_engine()
    s = select([routes]).where(and_(routes.c.journey_pattern=='00010001', routes.c.position_on_route >
        (select([routes.c.position_on_route]).where(and_(routes.c.journey_pattern=='00010001', routes.c.stop_id==origin))))).order_by('position_on_route')
    res = eng.execute(s).fetchall()
    for row in res:
        print(row)
    eng.dispose()
    return res


connecting_stops(374)

