"""
Testing suite for data cleaning process
"""

import unittest
import pandas as pd
from data_cleaning import Clean, main

cdf = main()

class test_data_cleaning(unittest.TestCase):

    def test_prep_df(self):
        # check return type is a pandas dataframe
        self.assertEqual(str(type(cdf)), "<class 'pandas.core.frame.DataFrame'>")

        # check null journey_patterns are removed
        null_journeys = cdf['journey_pattern_id'].isnull().sum()
        self.assertEqual(null_journeys, 0)

        # check null stop_ids are removed
        null_stops = cdf['stop_id'].isnull().sum()
        self.assertEqual(null_stops, 0)

        # check mixed type columns have been fixed
        jp_type = cdf['journey_pattern_id'].dtype
        stop_type = cdf['stop_id'].dtype
        self.assertEqual(jp_type, 'object')
        self.assertEqual(stop_type, 'int64')

    def test_drop_columns(self):
        columns = cdf.columns
        print(columns)
        self.assertEqual(columns, ['unnamed', 'timestamp', 'journey_pattern_id', 'time_frame', 'vehicle_journey_id',
                                   'longitude', 'latitude', 'stop_id'])

    def test_remove_incomplete_runs(self):
        df_grouped = cdf.groupby(['vehicle_journey_id', 'time_frame', 'journey_pattern_id'])
        df_short_journeys = df_grouped.filter(lambda x: len(x) < 45)
        self.assertEqual(len(df_short_journeys), 0)



