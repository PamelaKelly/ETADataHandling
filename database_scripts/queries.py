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

def connecting_stops_two_queries(origin):
    """Selects all subsequent connecting stops on a single route for a given origin"""
    eng = db.connect_engine()
    res_list = []
    s_jp = select([routes.c.journey_pattern]).where(routes.c.stop_id==origin)
    journeys = eng.execute(s_jp).fetchall()
    for jp in journeys:
        s = select([routes]).where(and_(routes.c.journey_pattern == jp[0], routes.c.position_on_route >
                                        (select([routes.c.position_on_route]).where(
                                            and_(routes.c.journey_pattern == jp[0],
                                                 routes.c.stop_id == origin))))).order_by('position_on_route')
        res = eng.execute(s).fetchall()
        res_list.append(res)
    for r in res_list:
        for row in r:
            print(row)
    eng.dispose()
    return res_list

connecting_stops_two_queries(375)

def connecting_stops_single_query(origin):
    eng = db.connect_engine()
    s = select([routes]).where(and_(
        routes.c.journey_pattern.in_(
            select([routes.c.journey_pattern]).where(routes.c.stop_id==origin)
        ),

    ))

    s = select([routes]).where(and_(
        routes.c.position_on_route > (
            select([routes.c.position_on_route]).where(
                routes.c.stop_id==origin)
            ),
            routes.c.journey_pattern.in_(
            select([routes.c.journey_pattern]).where(routes.c.stop_id==origin)
            )
        )
    )
    res = eng.execute(s).fetchall()
    for row in res:
        print(row)
    eng.dispose()
    return res

def connections_inefficient(origin):
    res_list = []
    eng = db.connect_engine()
    s = select([routes]).where(and_(
        routes.c.journey_pattern.in_(
            select([routes.c.journey_pattern]).where(routes.c.stop_id==origin)
        ),
        routes.c.position_on_route > (
            select([routes.c.position_on_route]).where(and_(
            routes.c.journey_pattern.in_(
                select([routes.c.journey_pattern]).where(routes.c.stop_id==origin)
            ),
            routes.c.stop_id==origin)
        )
    )
    )
    ).order_by('position_on_route')
    res = eng.execute(s).fetchall()
    for row in res:
        print(row)
    res_list.append(res)
    for r in res_list:
        for row in r:
            print(row)
    eng.dispose()
    return res