from os.path import dirname
import pickle

from geopy.distance import distance


class __Cfg:
    tree_file = dirname(__file__) + '/stops_tree.pkl'
    stop_coords_file = dirname(__file__) + '/stop_coords_id.pkl'


class InvalidStopCoordinatesError(Exception):
    """Hopefully should never be raised. If it is,
    there is an inconstancy between stop values in a tree
    and in the coordintes -> stop dictionary. """
    pass


def nearest_stop(journey_pattern_id, lat, lon, max_dist=30):
    """Find the nearest stop for a given coordinate and given journeyPatternId
    @params:
        journey_pattern_id (str)
        lat (float)
        lon (float)
        max_dist (int) OR (float) # optional

    @returns:
        (str (stop_id) float (distance)) OR (None, None)

    @raises:
        ValueError (Journey pattern ID not found)
        InvalidStopCoordinatesError (SHOULDN'T ever be raised. 
                        Discrepency between data sources)
    """

    # these are global constants so that the files containing the
    # trees and stop coordinates are read only once (at import time),
    # rather than each time the function is called
    global __TREES
    global __COORDS_STOPS

    if type(journey_pattern_id) is not str:
        raise TypeError('{} is type {}, not str'.format(
            journey_pattern_id, type(journey_pattern_id)))

    try:
        t = __TREES[journey_pattern_id]
    except KeyError:
        raise ValueError(
            'No tree found for JourneyPatternId {}'.format(journey_pattern_id))

    # _ is the euclidean distance, not used.
    # idx is the location of the nearest neighbhour in the tree
    # k is the numer of nearest neightbours
    _, idx = t.query((lat, lon), k=1)
    coords = t.data[idx]
    # convert from numpy array to (float, float)
    coords = (float(coords[0]), float(coords[1]))

    d = distance((lat, lon), coords).meters

    if d > max_dist:
        return None, None

    try:
        stop_id = __COORDS_STOPS[coords]
    except KeyError:
        raise InvalidStopCoordinatesError

    return stop_id, d


with open(__Cfg.tree_file, 'rb') as f:
    __TREES = pickle.load(f)


with open(__Cfg.stop_coords_file, 'rb') as f:
    __COORDS_STOPS = pickle.load(f)
