from os.path import expanduser

import pandas as pd
from geopy.distance import distance
from stop_lookup.stop_lookup import nearest_stop

def add_nearest_stop(df):
    """
    :param df:
    :return:
    """
    count = 0
    error_count = 0
    for index, row in df.iterrows():
        count += 1
        if count % 10000 == 0:
            print(count)
        print(row['Journey_Pattern_ID'])
        try:
            stop, distance = nearest_stop(row['Journey_Pattern_ID'],
                               row['Lat_WGS84'],
                               row['Lon_WGS84'],
                               max_dist=30)
        except Exception as e:
            error_count += 1
            print("Error count: ", error_count)
            print(e)
            print(type(e))
            # we should probably use a logging module for this, but depending
            # on how extenesively we are logging this might be easier


            #throws error textIOWrapper object has no attribute 'writeline'
            #with open(Cfg.log_file, 'at') as f:
            #    f.writeline('JourneyPatternId {} not found in trees'.format(
            #        row['journey_pattern_id']))

            stop = False
            distance = False

        df.set_value(index, 'stop_id', stop)
        df.set_value(index, 'distance_from_stop', distance)
    return df

df = pd.read_csv('dublin_2012_week1_new.csv')
df = add_nearest_stop(df)
df.to_csv('nearest_stop_test.csv')
print(df['Journey_Pattern_ID'].nunique())
df = df.loc[(df['stop_id'] != False)]
print(df['Journey_Pattern_ID'].nunique())