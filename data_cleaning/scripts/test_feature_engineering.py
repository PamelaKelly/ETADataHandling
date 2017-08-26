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

def filter_data(self):
    df_new = pd.read_csv('../datasets/base_table.csv')
    num_nulls = df_new['stop_id'].isnull().sum()
    self.assertEqual(num_nulls, 0)