"""
Module for extracting the route information from the raw data
Includes journey patterns, stops on those routes and distances from terminus to each stop
"""

import pandas as pd
import json
import re


class Routes():
    def __init__(self, datasource):
        self.__df = datasource
        self.__routes = {}

    def check_null_stops(self):
        print(self.__df['stop_id'].isnull().sum())

    def get_unique_routes(self):
        df_grouped = self.__df.groupby(['journey_pattern_id'])
        routes = {}
        for j_name, j_group in df_grouped:
            routes[str(j_name)] = {}
            line_id = str(j_name)[1:4:]
            line_id = re.sub('0', '', line_id)
            print(line_id)
            routes[str(j_name)]["line_id"] = line_id

        df_grouped = self.__df.groupby(['journey_pattern_id', 'stop_id'])
        for name, group in df_grouped:
            try:
                min_idx = group.ix[group['distance_from_stop'].idxmin(skipna=True)]
                dist = min_idx['distance']
                journey = min_idx['journey_pattern_id']
                stop = min_idx['stop_id']
                routes[str(journey)][str(stop)] = float(dist)
            except Exception as e:
                print("error of type: ", type(e))
                print(e)

        self.__routes = routes

    def get_routes(self):
        return self.__routes

def main():
    data = pd.read_csv('../datasets/between_phase/week_1_2012_nearest_stop.csv', dtype={"journey_pattern_id": str})
    print(data.columns)
    data = data.ix[1:]
    data['stop_id'] = data['stop_id'].astype(int)
    routes = Routes(data)
    routes.get_unique_routes()
    routes_dict = routes.get_routes()
    with open('../datasets/output_files/routes.txt', 'w') as f:
        json.dump(routes_dict, f)
    return routes_dict

main()
