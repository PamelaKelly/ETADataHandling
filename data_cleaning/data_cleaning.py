"""
File to consolidate data cleaning process for the ETA
Dublin Bus Project
"""
import pandas as pd
import time

class Clean():
    def __init__(self, data_source, column_names):
        """
        :param data_source: filename or list of filenames
        :return initialises a data frame from the data sources provided
        """
        for file in data_source:
            frames = []
            df_temp = pd.read_csv(file, names=column_names)
            frames.append(df_temp)

        self.__df = pd.concat(frames)

    def prep_df(self):
        """
        :return: a dataframe without duplicates or null values
        and timestamp converted to seconds from ms
        """

        # drop duplicate rows in dataframe
        self.__df.drop_duplicates(inplace=True)

        # drop journey pattern ids that are null
        self.__df = self.__df[self.__df.journey_pattern_id != 'null']

        #reduce timestamp from milliseconds to seconds
        self.__df['timestamp'] = self.__df['timestamp'] // 1000000

        # fix columns with mixed data types
        # how to fix stop id when nulls still present
        self.__df['journey_pattern_id'] = self.__df['journey_pattern_id'].astype(str)

    def drop_columns(self):
        """
        Drops the columns that won't be used
        :return: updates data frame by dropping columns
        """
        # define list of unwanted columns
        unwanted = ['direction', 'line_id', 'operator', 'congestion', 'delay',
                    'at_stop', 'block_id']

        # drop unwanted columns
        self.__df.drop(unwanted, axis=1, inplace=True)
        print(type(self.__df))

    def midnight_journeys_helper(self, row):
        """
        :param row: takes a row from a df and removes it if timestamp is null
        :return: returns current rows timestamp if next_timestamp is null
        or next_timestamp if not null
        """
        if pd.isnull(row['next_timestamp']):
            return row['timestamp']
        else:
            return row['next_timestamp']

    def midnight_journeys(self):
        """
        :return: updates dataframe to get rid of the midnight journeys issue
        """

        # create a new column next_timestamp and populate it with the timestamp for
        # the next row in each individual run
        self.__df['next_timestamp'] = self.__df.groupby(['vehicle_journey_id',
                                                         'journey_pattern_id', 'time_frame'])['timestamp'].shift(-1)

        # how are nulls removed here?
        self.__df['next_timestamp'] = self.__df.apply(self.midnight_journeys_helper, axis=1)

        self.__df['next_timestamp'] = self.__df['next_timestamp'].astype(int)
        # time between rows
        self.__df['between_time'] = self.__df['next_timestamp'] - self.__df['timestamp']

        # what does this line do?
        self.__df = self.__df.drop(self.__df.index[self.__df['between_time'] > 120])
        self.__df.drop(['next_timestamp', 'between_time'], axis=1, inplace=True)

    def remove_incomplete_runs(self):
        """
        :return:
        """
        df_grouped = self.__df.groupby(['vehicle_journey_id', 'time_frame', 'journey_pattern_id'])
        df_short_journeys = df_grouped.filter(lambda x: len(x) < 45)
        self.__df = pd.concat([self.__df, df_short_journeys]).drop_duplicates(keep=False)

    def get_df(self):
        return self.__df

def unsure():
    """
    some things from the notebook that I'm not sure what they do
    """
    # Only keep the first row for every trip at the same Stop(keep both at or not_at stop)
    #df = df.drop_duplicates(['Vehicle_Journey_ID', 'Journey_Pattern_ID', 'Date', 'Stop_ID', 'At_Stop'])
    pass

def main():
    print(time.time())
    print("Process Beginning")
    print("Reading files")
    data = ['../datasets/input_files/siri.20121106.csv', '../datasets/input_files/siri.20121107.csv', '../datasets/input_files/siri.20121108.csv', '../datasets/input_files/siri.20121109.csv',
            '../datasets/input_files/siri.20121110.csv', '../datasets/input_files/siri.20121111.csv', '../datasets/input_files/siri.20121112.csv']
    column_names = ['timestamp', 'line_id', 'direction', 'journey_pattern_id', 'time_frame', 'vehicle_journey_id',
                    'operator', 'congestion', 'longitude', 'latitude', 'delay', 'block_id', 'vehicle_id', 'stop_id', 'at_stop']
    print("Cleaning Data")
    clean_df = Clean(data, column_names)
    clean_df.prep_df()
    clean_df.drop_columns()
    clean_df.midnight_journeys()
    clean_df.remove_incomplete_runs()
    cleaned_df = clean_df.get_df()
    cleaned_df.to_csv('../datasets/output_files/clean_df.csv')
    print("Done")
    print(time.time())
    return clean_df

main()