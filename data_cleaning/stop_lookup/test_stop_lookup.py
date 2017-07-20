import json
import random
from time import sleep

from stop_lookup import nearest_stop, __COORDS_STOPS


class ExitLoop(Exception):
    """A hacky way of breaking out of nested loops"""
    pass


def test_nearest_stop():
    global JP_ID_STOPS

    # pick a random stop
    coords = random.choice(list(__COORDS_STOPS.keys()))
    stop_id = str(__COORDS_STOPS[tuple(coords)])

    # find a journeyPatternId for that stop
    # innefficient, but c'est la vie
    try:
        for jpId, d in JP_ID_STOPS.items():
            stops = d['stops']
            for stop in stops:
                if str(stop) == stop_id:
                    journey_pattern_id = jpId
                    raise ExitLoop
        else:
            # No journey pattern ID found for that stop
            raise ValueError('stop -> jpID lookup error')

    except ExitLoop:
        pass

    # FINALLY!
    pred_stop, pred_dist = nearest_stop(journey_pattern_id, *coords)
    assert pred_stop == stop_id
    assert pred_dist == 0

    # Make a TINY change in the coords so that the value isn't exact,
    # but it'll still be the closest stop

    coords = (float(coords[0]) + 0.0000000001,
              float(coords[1]) - 0.0000000002)

    pred_stop, pred_dist = nearest_stop(journey_pattern_id, *coords)
    assert pred_stop == stop_id
    assert pred_dist != 0


if __name__ == '__main__':

    with open('bus_routes.json') as f:
        JP_ID_STOPS = json.load(f)

    test_nearest_stop()
