import pandas as pd
import json

class Stops():
    def __init__(self, data, year):
        self.__df = data
        self.__year = year

    def get_stops(self):
        stops_dict = {}
        if self.__year == 2012:
            # don't think cleaning this is necessary anymore Conan updated to a clean stops file
            for index, row in self.__df.iterrows():
                stop_address = row['Name without locality'] + ", " + row['Locality']
                stop_id = row['Stop No']
                lat = row['lat']
                lon = row['lon']
                stops_dict[stop_id] = {"stop_address": stop_address, "latitude": lat, "longitude": lon, "year": self.__year}
        elif self.__year == 2017:
            for index, row in self.__df.iterrows():
                stops_dict[row['stop_id']] = {"stop_address": row['stop_address'], "latitude": row['latitude'], "longitude": row['longitude'], "year": self.__year}
        return stops_dict

def main():
    data_2012 = pd.read_csv('../datasets/input_files/2012_stops.txt')
    stops_2012 = Stops(data_2012, 2012)
    stops_dict_2012 = stops_2012.get_stops()
    with open('../datasets/output_files/clean_stops_2012.txt', 'w') as f:
        json.dump(stops_dict_2012, f)

    data_2017 = pd.read_csv('../datasets/input_files/2017_stops.csv')
    stops_2017 = Stops(data_2017, 2017)
    stops_dict_2017 = stops_2017.get_stops()
    with open('../datasets/output_files/clean_stops_2017.txt', 'w') as f:
        json.dump(stops_dict_2017, f)

    return stops_dict_2012, stops_dict_2017

main()