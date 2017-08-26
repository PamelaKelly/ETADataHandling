# ETA_Data_Handling
Data Cleaning and Modelling for ETA Dublin Bus Project

Please Note: Datasets are not included as the size of these was too large.
If you would like to run the scripts locally please contact us for the datasets.

Guide to Folders:

1. Data Cleaning: 
Consolidation of all data cleaning processes into one place. 

Core scripts used:
Data_Cleaning_2012_Week1.ipynb - Final data cleaning file for first week of data.
Data_Cleaning_Process_ETA.ipynb - the final file for cleaning the full data set
data_cleaning.py - Deprecated - behind Jupyter Notebooks.

Scripts/
data_cleaning.py - an object oriented version of the data_cleaning process - not used for final process
data_quality.py - check null values have been removed
feature_engineering.py - an object oriented version of the feature engineering process - not used for final process
test_data_cleaning.py - used to debug during the data cleaning process
test_feature_engineering.py - used to debug during the feature engineering process

stop_lookup/
helper package to enable the use of nearest neighbour lookups

2. Data Modelling

Modelling_all_data.ipynb - the final modelling process for all of the data
Modelling_Every_Journey_Pattern_ID.ipynb - modelling the individual journey patterns
Modelling_For_One_Week_Data.ipynb - Testing a number of models (SVM, ANN, RFR, LR) on a subset of the first week
of data.
Modelling_LinearRegression_RandomForestRegressor.ipynb - Testing and Comparing these two models on the first week of
data.

3. Data Quality

data_quality_one_week_v1.ipynb - first iteration of data quality on one week of data (first week Nov)
data_quality_report_one_week_v2.ipynb - second iteration of data quality on one week of data (first week Nov)
data_quality_report3_all_data.ipynb - third iteration of data quality on full dataset.

4. Database Scripts

database_manager.py - handles setting up the database details and connections - abstract from specifics of project
database_setup.py - handles creating the schema, and populating the database specifically for this project
queries.py - testing ORM queries for front end to try to help with optimization of queries.

tests/
test_database_manager.py - test the database_manager script

5. Static Data Prep

stop_lookup/
Helper folder to enable nearest neighbour lookups - duplicated due to import issues.
2012_stops.ipynb - parses and preps the 2012 stop information, including conversion of location coordinates.
Get_Timetable.ipynb - Extracts timetables from raw data.
map_timetables_journey_patterns.py - attempts to map timetables scraped from web archive to journey pattersn - unsuccessful
nearest_stop.py - adds nearest stop to dataset in order to aid routes.py
routes.py - extracts route information from raw data
stops.py - parses 2017 stop data.
timetable_scraper - scraped 2012 timetables from web archive

