"""
Script to transform raw features and implement derived features
"""

import pandas as pd
from geopy.distance import distance
from stop_lookup.stop_lookup import nearest_stop
import time

class Base_Table():
    def __init__(self, cleaned_df):
        self.__df = cleaned_df

    def cooroutine_helper(self):
        """
        Helpter function to calculate distance feature
        """
        prev = yield
        running_distance = 0
        while True:
            nxt = yield running_distance
            running_distance += distance(prev, nxt).meters
            prev = nxt

    def add_distance_feature(self):
        """
        :return: updates data frame to include distance feature - distance from first
         stop on the route to current location in row
        """
        self.__df['Distance'] = -1
        vehicle_journey_groups = []
        df_grouped = self.__df.groupby(['journey_pattern_id', 'vehicle_journey_id'])
        for name, group in df_grouped:
            c = self.cooroutine_helper()
            next(c)
            for index, row in group.iterrows():
                dist = c.send((row['latitude'], row['longitude']))
                group.set_value(index, 'Distance', dist)
            vehicle_journey_groups.append(group)
        # removed functionality to deal with long journeys - need better way to do this
        self.__df = pd.concat(vehicle_journey_groups)

    def add_nearest_stop_distance(self):
        """
        :return: updates stop_id feature to closest stop id based on location and distance to that stop
        """
        invalid_journey_patterns = []
        for index, row in self.__df.iterrows():
            try:
                stop, distance = nearest_stop(row['journey_pattern_id'],
                                              row['latitude'],
                                              row['longitude'],
                                              max_dist=30)

                # max distance = 30 because AVL is only accurate within 30 meters
            except ValueError:
                stop = False
                distance = False
                # why make the whole journey invalid?
                invalid_journey_patterns.append(row['journey_pattern_id'])

        self.__df.set_value(index, 'stop_id', stop)
        self.__df.set_value(index, 'distance_from_stop', distance)

    def remove_null_stops(self):
        """
        :return: updates dataframe removing stop_ids that are null
        """
        self.__df = self.__df.loc[(self.__df['stop_id'] != False)]
        self.__df = self.__df.loc[(self.__df['stop_id'] != 'null')]
        self.__df = self.__df.loc[(self.__df['stop_id'] != None)]
        self.__df.dropna(axis=0, how='any', subset=['stop_id'])

    def add_datetime(self):
        self.__df['datetime'] = pd.to_datetime(self.__df['timestamp'], unit='s')
        self.__df['datetime'] = self.__df['datetime'].astype('datetime64[ns]')

    def add_hour(self):
        self.__df['hour'] = self.__df['datetime'].dt.hour

    def add_day(self):
        self.__df['day'] = self.__df['datetime'].dt.weekday_name

    def add_weekend(self):
        self.__df['weekend'] = self.__df['day'].map({'Monday': 0, 'Tuesday': 0, 'Wednesday': 0, 'Thursday': 0,
                                            'Friday': 0, 'Saturday': 1, 'Sunday': 1})

    def time_bin_helper(self, tm):
        if tm <= 4:
            return 'early_am'
        elif tm >= 5 and tm <= 12:
            return 'am'
        elif tm >= 12 and tm <= 20:
            return 'pm'
        elif tm >= 21:
            return 'late_pm'

    def add_time_bin(self):
        self.__df['time_bin'] = self.__df['hour'].map(lambda x: self.time_bin_helper(x))

    def add_travel_time(self):
        zscore = lambda x: (x - x.min())
        self.__df['travel_time'] = self.__df.groupby(['vehicle_journey_id', 'journey_pattern_id', 'time_frame'])['timestamp'].transform(zscore)


    def add_weather(self, weather_data):
        """
        :param weather_data: dictionary containing weather data
        :return: an updated dataframe with weather information
        """
        self.__df = pd.merge(self.__df, weather_data, how='left', left_on=['time_frame', 'time_bin'], right_on = ['time_frame', 'time_bin'])


    def congestion_feature(self):
        """
        :return: adds congestion feature indicating the number of stops on journey in city centre
        """
        pass

    def get_df(self):
        return self.__df

def main():
    """
    Run the feature engineering process
    """
    print(time.time())
    data = '../datasets/output/files/clean_df.csv'
    print("Process Beginning")
    print("Reading Clean CSV")
    clean_df = pd.read_csv(data)
    base_table = Base_Table(clean_df)
    print("Adding datetime")
    base_table.add_datetime()
    print("Adding Day")
    base_table.add_day()
    print("Adding Hour")
    base_table.add_hour()
    print("Adding Time Bin")
    base_table.add_time_bin()
    print("Adding Weekend Boolean")
    base_table.add_weekend()
    print("Adding Distance")
    base_table.add_distance_feature()
    print("Updating Stop Id")
    base_table.add_nearest_stop_distance()
    print("Filtering Data")
    base_table.remove_null_stops()
    print("Adding Travel Time")
    base_table.add_travel_time()
    print("Adding Congestion")
    base_table.congestion_feature()
    bs = base_table.get_df()
    bs.to_csv('../datasets/output_files/base_table.csv')
    return bs

main()