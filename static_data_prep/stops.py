"""Python Module for converting 2017 stop info into appropriate format for writing to database """

import pandas as pd
import json

class Stops():
    def __init__(self, data, year):
        self.__df = data
        self.__year = year

    def get_stops(self):
        stops_dict = {}
        if self.__year == 2017:
            for index, row in self.__df.iterrows():
                stops_dict[row['stop_id']] = {"stop_address": row['stop_address'], "latitude": row['latitude'], "longitude": row['longitude'], "year": self.__year}
        return stops_dict

def main():
    data_2017 = pd.read_csv('../datasets/input_files/2017_stops.csv')
    stops_2017 = Stops(data_2017, 2017)
    stops_dict_2017 = stops_2017.get_stops()
    with open('../datasets/output_files/clean_stops_2017.txt', 'w') as f:
        json.dump(stops_dict_2017, f)

    return stops_dict_2017

main()