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
                min = group['distance_from_stop']
                min_row = group.loc[(group['distance_from_stop'] == min)]
                dist = min_row['Distance']
                journey = min_row['journey_pattern_id']
                stop = min_row['stop_id']
                routes[str(journey)][str(stop)] = dist
            except Exception as e:
                print("error of type: ", type(e))
                print(e)

        self.__routes = routes

    def get_routes(self):
        return self.__routes

def main():
    data = pd.read_csv('../datasets/base_table.csv')
    routes = Routes(data)
    routes.get_unique_routes()
    routes_dict = routes.get_routes()
    print(type(routes_dict))
    with open('../datasets/routes.txt', 'w') as f:
        json.dump(routes_dict, f)
    return routes_dict

main()
