"""
Testing suite for data cleaning process
"""

import unittest
import pandas as pd
from data_cleaning import Clean

data = ['../datasets/input_files/siri.20121106.csv', '../datasets/input_files/siri.20121107.csv', '../datasets/input_files/siri.20121108.csv',
        '../datasets/input_files/siri.20121109.csv',
        '../datasets/input_files/siri.20121110.csv', '../datasets/input_files/siri.20121111.csv', '../datasets/input_files/siri.20121112.csv']
column_names = ['timestamp', 'line_id', 'direction', 'journey_pattern_id', 'time_frame', 'vehicle_journey_id',
                'operator', 'congestion', 'longitude', 'latitude', 'delay', 'block_id', 'vehicle_id', 'stop_id',
                'at_stop']

class test_data_cleaning(unittest.TestCase):

    def test_prep_df(self):
        clean_df = Clean(data, column_names)
        cdf = clean_df.get_df()
        print(type(cdf))
        self.assertEqual(str(type(cdf)), "<class 'pandas.core.frame.DataFrame'>")

    def test_deal_with_midnight_journeys(self):
        pass

    def test_groupby_df(self):
        pass

    def test_coor(self):
        pass

    def test_add_distance_all_runs(self):
        pass

    def test_add_nearest_stop(self):
        pass

    def filter_data(self):
        df_new = pd.read_csv('../datasets/base_table.csv')
        num_nulls = df_new['stop_id'].isnull().sum()
        self.assertEqual(num_nulls, 0)

    def test_add_time_column(self):
        pass

    def test_add_datetime_columns(self):
        pass

    def test_add_time_bin_column(self):
        pass

    def test_add_weather_columns(self):
        pass

    def test_add_day_of_week_columns(self):
        pass

    def test_add_hour_column(self):
        pass

    def test_add_congestion_features(self):
        pass

    def test_main(self):
        pass
