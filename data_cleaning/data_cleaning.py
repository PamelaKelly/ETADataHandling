"""File to consolidate data cleaning process"""

from os.path import expanduser

import pandas as pd
from geopy.distance import distance


class Cfg:
    in_file = expanduser('~/datasets/siri.20121106.csv')
    log_file = 'data_cleaning.log'


def prep_df():
    """
    :return: a dataframe with the appropriate column headings
    """
    column_names = ['timestamp', 'line_id', 'direction', 'journey_pattern_id',
                    'time_frame', 'vehicle_journey_id', 'operator', 'congestion', 'longitude',
                    'latitude', 'delay', 'block_id', 'vehicle_id', 'stop_id', 'at_stop']
    print("Reading csv...")
    df = pd.read_csv(Cfg.in_file, names=column_names)

    print("Dropping duplicates...")
    df.drop_duplicates(inplace=True)

    print("Drop all journey_pattern_id that are null...")
    df = df.drop(df.index['journey_pattern_id'] =='null')

    print("Reduce Scale of Timestamp...")
    df['timestamp'] = df['timestamp']//1000000
    return df


def concat_dataframes(dataframes):
    """
    :param dataframes: a list of dataframes to be concatenated into one
    column_names must be the same for all dataframes
    :return: one dataframe combining all dataframes in the list
    """
    return(pd.concat(dataframes))


def drop_columns(df):
    """
    :param df: dataframe with all columns still included
    :return: dataframe with unwanted columns removed
    """
    unwanted = ['direction', 'line_id', 'operator', 'congestion',
                'delay', 'stop_id', 'at_stop', 'block_id']
    return df.drop(unwanted, axis=1, inplace=True)


def remove_nulls(row):
    """
    helper function for deal_with_midnight_journeys
    :param row:
    :return:
    """
    if pd.isnull(row['next_timestamp']):
        return row['timestamp']
    else:
        return row['next_timestamp']

def deal_with_midinght_journeys(df):
    """
    :param df:
    :return:
    """
    df['next_timestamp'] = df.groupby(['vehicle_journey_id', 'journey_pattern_id', 'time_frame'])['timestamp'].shift(-1)
    df['next_timestamp'] = df.apply(remove_nulls, axis=1)
    df['next_timestamp'] = df['next_timestamp'].astype(int)
    df['between_time'] = df['next_timestamp'] - df['timestamp']
    df=df.drop(df.index[df['between_time']>120])
    df.drop(['next_timestamp', 'between_time'], axis=1, inplace=True)
    return df

def group_df(groupby_params, df):
    """
    :param groupby_params: list of feature names as strings by which to group by
    :param df: dataframe that is to be grouped
    :return: a pandas groupby object
    """
    if len(groupby_params) == 0:
        print("You did not provide any groupby parameters")
        return None
    else:
        df_grouped = df.groupby(groupby_params)
        return df_grouped


def remove_incomplete_runs(df):
    """
    :param df: prepped dataframe
    :return: df with journeys less than 45 rows removed
    """
    df_grouped = group_df(['vehicle_journey_id', 'time_frame', 'journey_pattern_id'])
    df_short_journeys = df_grouped.filter(lambda x: len(x) < 45)
    df = pd.concat([df, df_short_journeys]).drop_duplicates(keep=False)
    return df

def coor():
    prev = yield
    running_dist = 0
    while True:
        next_ = yield running_dist
        running_dist += distance(prev, next_).meters
        prev = next_


def add_distance_all_runs(df):
    """
    :param df: dataframe before distance has been added
    :return: dataframe with distances for each run
    """
    df['Distance'] = -1
    vj_groups = []
    df_grouped = group_df(['journey_pattern_id'], df)
    for journey_name, journey_group in df_grouped:
        vehicle_groups = group_df(['vehicle_journey_id'], journey_group)
        for v_name, v_group in vehicle_groups:
            c = coor()
            next(c)
            for index, row in v_group.iterrows():
                dist = c.send((row['latitude'], row['longitude']))
                v_group.set_value(index, 'Distance', dist)
            if v_group['Distance'].max() < 36000:
                vj_groups.append(v_group)

    return concat_dataframes(vj_groups)


def add_mean_distance(df):
    """
    :param df: dataframe with unique distances for all runs of journeys.
    This function groups by stop_id so the df param should have already
    been filtered through with the correct stop_ids input.
    :return: dataframe with mean distance input for each instance of a
    journey_pattern and stop_id combination.
    """
    updated_groups = []
    df_grouped = group_df(['journey_pattern_id'], df)
    for journey_name, journey_group in df_grouped:
        df_stops = group_df(['stop_id'], journey_group)
        for stop_name, stop_group in df_stops:
            mean_dist = stop_group['Distance'].mean()
            for index, row in stop_group.iterrows():
                row['Distance'] = mean_dist
        updated_groups.append(stop_group)

    return concat_dataframes(updated_groups)


def add_nearest_stop(df):
    """
    :param df:
    :return:
    """
    for index, row in df.iterrows():
        try:
            stop, _ = nearest_stop(row['journey_pattern_id'],
                               row['latitude'],
                               row['longitude'],
                               max_dist=30)
        except ValueError:
            # we should probably use a logging module for this, but depending
            # on how extenesively we are logging this might be easier
            with open(Cfg.out_dir + Cfg.log_file, 'at') as f:
                f.writeline('JourneyPatternId {} not found in trees'.format(
                    row['journey_pattern_id']))

            stop = False

        df.set_value(index, 'stop_id', stop)
    return df


def filter_down_data(df):
    """
    :param df: a dataframe with correct stop_ids already added
    :return: a dataframe where null stop_ids are removed
    """
    df = df.drop(df.index['stop_id'] == False)
    return df


def add_time_column(df):
    """
    :param df:
    :return:
    """
    zscore = lambda x: (x - x.min())
    df['travel_time'] = df.groupby(['vehicle_journey_id', 'journey_pattern_id',
                                    'time_frame'])['timestamp'].transform(zscore)
    df = df.drop(df.index[df['travel_time']>=14400])
    # need to refactor this to have a cut off of the last stop
    # instead of 4 hours...
    return df


def add_datetime_column(df):
    """
    :param df:
    :return:
    """
    df['datetime'] = pd.to_datetime(df['timestampe'], unit='s')
    df['datetime'] = df['datetime'].astype('datetime64[ns]')
    return df


def add_time_bin_column(df):
    """
    :param df:
    :return:
    """
    df['time_bin'] = 'null'
    for index, row in df.iterrows():
        hour = row['hour']
        if hour <= 4:
            time_bin = 'early_am'
        elif hour >= 5 and hour <= 12:
            time_bin = 'am'
        elif hour >= 12 and hour <= 20:
            time_bin = 'pm'
        elif hour >= 21:
            time_bin = 'late_pm'
        df.set_value(index, 'time_bin', time_bin)

    return df


def add_weather_columns(df, weather_data):
    """
    :param df: a dataframe
    :param weather_data: a json file with weather data in the appropriate format
    :return: a dataframe with weather data added
    """
    df['wind'] = 0
    df['rain'] = 0
    df['cloud'] = 0
    df['temp'] = 0
    weather_options = ['wind', 'rain', 'cloud', 'temp']
    for index, row in df.iterrows():
        date = row['time_frame'][8:]
        for opt in weather_options:
            value = weather_data[date][row['time_bin']][opt]
            df.set_value(index, opt, value)

    return df


def add_day_of_week_columns(df):
    """
    :param df:
    :return:
    """
    df['day'] = df['datetime'].day
    return df


def add_hour_column(df):
    """
    :param df:
    :return:
    """
    df['hour'] = df['datetime'].hour
    return df


def add_congestion_features(df):
    """
    @args take a dataframe

    decide on how to deal with congestion/location
    and implement feature

    returns an updated dataframe
    """
    pass


def main():
    """
    Run the main data cleaning and feature adding process
    and return a final data frame for use in modelling
    """
    df = prep_df()
    df = drop_columns(df)
    df = add_datetime_column(df)
    df = remove_incomplete_runs(df)
    df = deal_with_midinght_journeys(df)
    df = add_time_bin_column(df)
    df = add_hour_column(df)
    df = add_weather_columns(df)
    df = add_day_of_week_columns(df)
    df = add_nearest_stop(df)
    df = add_distance_all_runs(df)
    df = add_mean_distance(df)
    df = add_time_column(df)


if __name__ == '__main__':
    main()
