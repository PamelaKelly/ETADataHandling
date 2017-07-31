"""Testing suite for feature engineering script"""
import pandas as pd
import unittest
from feature_engineering import Base_Table, main

df = pd.read_csv('../datasets/between_files/clean_df.csv')
base_table = main()

class test_feature_engineering(unittest.TestCase):

    def test_add_distance_feature(self):
        # test that column is added to df
        self.assertIn('Distance', base_table.columns)

        # test that there are no -1 values for distance
        minimum = base_table['Distance'].min()
        self.assertGreater(minimum, -1)

    def test_add_nearest_stop_distance(self):


    def test_remove_null_stops(self):
        pass

    def test_add_datetime(self):
        pass

    def test_add_hour(self):
        pass

    def test_add_day(self):
        pass

    def test_add_weekend(self):
        pass

    def test_add_time_bin(self):
        pass

    def test_add_travel_time(self):
        pass

    def test_add_weather(self):
        pass

    def test_congestion_feature(self):
        pass

def filter_data(self):
    df_new = pd.read_csv('../datasets/base_table.csv')
    num_nulls = df_new['stop_id'].isnull().sum()
    self.assertEqual(num_nulls, 0)