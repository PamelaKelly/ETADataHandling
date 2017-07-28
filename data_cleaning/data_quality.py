"""
File to check that all major data quality issues have successfully been dealt with
"""

import pandas as pd

class Quality_Check():
    def __init__(self, datasource):
        self.__df = datasource

    def check_nulls(self):
        columns = self.__df.columns
        for c in columns:
            if self.__df[c].isnull().sum() > 0:
                print("Nulls present in column: ", c)
                return "Fail"
        print("Pass")

def main():
    data = pd.read_csv('../datasets/base_table.csv')
    qc = Quality_Check(data)
    qc.check_nulls()

main()