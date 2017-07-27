"""
File to consolidate data cleaning process for the ETA
Dublin Bus Project
"""
import pandas as pd
import json

class Clean():
    def __init__(self, data_source):
        """
        :param data_source: filename or list of filenames
        :return initialises a data frame from the data sources provided
        """
        if type(data_source) == '<string>':
            # data source is a filename
            # read file as dataframe
            self.__df = pd.read_csv(data_source)
        elif type(data_source) == '<list>':
            self.__df = pd.concat(data_source)
        else:
            print("You did not provide a valid data source")
            return None

    def prep_df(self):
        """
        :return: a dataframe without duplicates or null values
        and timestamp converted to seconds from ms
        """
        # define columns names
        column_names = ['timestamp', 'line_id', 'direction',
                        'journey_pattern_id', 'time_frame', 'vehicle_journey_id',
                        'operator', 'congestion', 'longitude', 'latitude',
                        'delay', 'block_id', 'vehicle_id', 'stop_id', 'at_stop']

        # drop duplicate rows in dataframe
        self.__df.drop_duplicates(inplace=True)

        # drop journey pattern ids that are null
        self.__df = self.__df[self.__df.journey_pattern_id != 'null']

        #reduce timestamp from milliseconds to seconds
        self.__df['timestamp'] = self.__df['timestamp'] // 1000000

    def drop_columns(self):
        """
        Drops the columns that won't be used
        :return: updates data frame by dropping columns
        """
        # define list of unwanted columns
        unwanted = ['direction', 'line_id', 'operator', 'congestion', 'delay',
                    'at_stop', 'block_id']

        # drop unwanted columns
        self.__df = self.__df.drop(unwanted, axis=1, inplace=True)

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
        self.__df['next_timestamp'] = self.__df.apply(self.midnight_journeys_helper(), axis=1)

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


def main():
    data = ''
    clean_df = Clean(data)
    clean_df.prep_df()
    clean_df.drop_columns()
    clean_df.midnight_journeys()
    clean_df.remove_incomplete_runs()
    clean_df.to_csv('clean_df.csv')
    return clean_df

if __name__ == 'main':
    main()