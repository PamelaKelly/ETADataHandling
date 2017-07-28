"""
This file parses the final base table for route information.
A route is defined by a journey pattern id and the corresponding information provided
are the stops on that rooute and the distance from the terminus to each stop
Takes one week of data - pre cleaned
"""

import pandas as pd
import json


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

        df_grouped = self.__df.groupby(['journey_pattern_id', 'stop_id'])
        for name, group in df_grouped:
            try:
                min_idx = group.ix[group['distance_from_stop'].idxmin(skipna=True)]
                dist = min_idx['Distance']
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
    data = pd.read_csv('../datasets/output_files/base_table.csv', dtype={"journey_pattern_id": str})
    data = data.dropna(how='any', subset=['stop_id'])
    routes = Routes(data)
    routes.get_unique_routes()
    routes_dict = routes.get_routes()
    with open('../datasets/output_files/routes.txt', 'w') as f:
        json.dump(routes_dict, f)
    return routes_dict

main()
