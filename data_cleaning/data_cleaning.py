"""File to consolidate data cleaning process"""

def convert_csv_to_df(csv_file):
	"""
	@args takes a csv file and converts it into a dataframe
	
	will this take one day or one week?? 
	"""
	pass

def drop_columns(df):
	"""
	@args takes a dataframe
	
	drops any columns of features that aren't useful
	
	returns an updated dataframe
	"""


def deal_with_midinght_journeys(df):
		"""
		@args takes a dataframe
		
		deals with journeys that crossover from one day into another
		causing erros in the time function
		
		returns an updated dataframe. 
		"""
		pass
	
def sort_journeys(groupby_params, df):
	""" 
	Helper Function! 
	
	Divide data into groups by specified groupby_params
	and returns a pandas groupby object.

	@args takes a list of features to groupby and a dataframe. 

	"""
	pass
	

	
def trimming_journeys(df):
	"""
	@args takes a dataframe
	
	Trims each journey by determining the 'start' and 
	'end' of each run. Drops rows where bus is idling for
	example. Use sort_journeys function for quick grouping. 
	
	"""
	pass
	
	
	
def add_distance_all_runs(grouped_journeys):
	"""
	Function to calculate the distance from terminal for every 
	run of every journey. 
	
	@args takes a pandas groupby object grouped by 
	jp_id and vj_id. 
	
	Return an updated dataframe with distance added. 
	"""
	pass
	
def add_mean_distance(dataframe_all_distances):
	"""
	Calculate the mean distance of each journeyPatternID
	by grouping by jp_id and stop_id and getting the mean of 
	the distance feature. 
	
	@args takes a dataframe where distance for every run 
	has already been calculated and inputed. 
	
	returns a dataframe where mean distance for each stop on
	each journey has been calculated and the mean value has 
	been assigned to every row for that stop/jp_id combination. 
	
	"""
	pass
	
	
def add_nearest_stop(df):
	"""
	Updates stop_id feature based on geolocation
	If no stop within a certain radius assigns null
	
	@args takes a dataframe with distance already calculated
	
	returns a dataframe with updated stop_id 
	"""
	pass
	
def filter_down_data(df):

	"""
	Filters through the dataframe removing rows where stop_id
	is null
	
	@args takes a dataframe where stop_id feature has been updated
	
	returns an updated (hopefully smaller) dataframe. 
	"""
	pass
	
def add_time_feature(df):
	"""
	@args takes a dataframe. 
	
	Uses sort_journeys for quick grouping. 
	
	Calculates the time for each run of each journey. 
	
	Returns an updated dataframe. 
	"""
	pass
	
def add_weather(df):
	"""
	@args takes a dataframe
	
	Adds weather to dataframe - inc wind, rain, 
	temp and cloud. 
	
	returns an updated dataframe. 
	"""
	pass
	
	
def add_day_of_week(df):
	"""
	@args takes a dataframe
	
	adds a new feature day of the week
	
	returns an updated dataframe. 
	"""
	pass
	
def add_hour_feature(df):
	"""
	@args takes a dataframe
	
	Adds a feature binning timestamp into hour
	of the day 
	
	returns an upated dataframe 
	
	"""
	pass

	
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
	pass
	
