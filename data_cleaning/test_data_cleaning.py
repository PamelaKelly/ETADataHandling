"""Testing suite for data cleaning process 
Write tests before implementing functions
and refactor as we go"""

# pd.to_datetime(df['timestamp'], unit='s')

import unittest
import pandas as pd
import data_cleaning as dc

df = pd.read_csv('../datasets/siri.20121106.csv')

class test_data_cleaning(unittest.TestCase):
    def test_prep_df(self):
        df = dc.prep_df()
        self.assertEqual(df.columns, ['timestamp', 'line_id', 'direction', 'journey_pattern_id',
    'time_frame', 'vehicle_journey_id', 'operator', 'congestion', 'longitude',
    'latitude', 'delay', 'block_id', 'vehicle_id', 'stop_id', 'at_stop'])

    def test_concat_dataframes(self):
        pass

    def test_drop_columns(self):
        pass

    def test_deal_with_midnight_journeys(self):
        pass

    def test_groupby_df(self):
        pass

    def test_coor(self):
        pass

    def test_add_distance_all_runs(self):
        pass

    def test_add_mean_distance(self):
        pass

    def test_add_nearest_stop(self):
        pass

    def add_filter_down_data(self):
        pass

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