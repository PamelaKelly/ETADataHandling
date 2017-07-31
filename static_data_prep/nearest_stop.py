import pandas as pd
import json
from stop_lookup.stop_lookup import nearest_stop

def add_nearest_stop_distance(df):
    """
    :return: updates stop_id feature to closest stop id based on location and distance to that stop
    """
    for index, row in df.iterrows():
        try:
            stop, distance = nearest_stop(row['journey_pattern_id'],
                                          row['latitude'],
                                          row['longitude'],
                                          max_dist=30)
            if stop == None or distance == None:
                df.set_value(index, 'distance_from_stop', -1)
                continue
                # max distance = 30 because AVL is only accurate within 30 meters
        except ValueError:
            continue

        df.set_value(index, 'stop_id', stop)
        df.set_value(index, 'distance_from_stop', distance)

    df = df.loc[(df['distance_from_stop'] != -1)]
    df = df.loc[(df['distance_from_stop'] != None)]
    df.dropna(axis=0, how='any', subset=['distance_from_stop'])
    return df

def main():
    columns=['timestamp', 'journey_pattern_id', 'time_date', 'vehicle_journey_id', 'longitude', 'latitude', 'stop_id',
             'at_stop', 'first_stop', 'last_stop', 'distance', 'trip_time',
             'datetime', 'hour', 'day_of_week', 'midweek', 'time_bin', 'cloud', 'rain', 'temp', 'wind']
    data = pd.read_csv('../datasets/output_files/dublin_2012_week1_distance.csv', names=columns, dtype={"journey_pattern_id": str})
    data = data.ix[1:]
    data['stop_id'] = data['stop_id'].astype(int)
    data = add_nearest_stop_distance(data)
    data = data.dropna(how='any', subset=['stop_id'])
    data.to_csv('../datasets/between_phase/week_1_2012_nearest_stop.csv')

main()