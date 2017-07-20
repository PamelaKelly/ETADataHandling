import json
import pickle

from scipy.spatial import cKDTree


with open('data/stop_coords_id.pkl', 'rb') as f:
    coords_stop_id = pickle.load(f)


stop_coords = {}
for k, v in coords_stop_id.items():
    stop_coords[v] = k


with open('data/bus_routes.json') as f:
    route_stop_ids = json.load(f)


trees = {}
missing_stops = []
for route, stop_d in route_stop_ids.items():

    stop_ids = stop_d['stops']

    route_stops = []
    for stop_id in stop_ids:
        try:
            route_stops.append(stop_coords[str(stop_id)])
        except KeyError as e:
            missing_stops.append(stop_id)

    trees[route] = cKDTree(route_stops)


with open('data/stops_tree.pkl', 'wb') as f:
    pickle.dump(trees, f)

print('{} missing stops skipped: \n{}'.format(
    len(missing_stops), missing_stops))

print('{} trees saved'.format(len(trees.keys())))

print('done')
