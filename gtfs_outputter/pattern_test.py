from os import path

import os
import df_helper
import pandas as pd
import logging
import numpy as np

feed = {}
tables = {}

def optional_field(index, column, dataframe, default='N/A'):
	row = dataframe.iloc[index]
	return row[column] if (column in dataframe.columns and not pd.isnull(row[column])) else default

for f in os.listdir("./agencies/vta/raw_csv/"):
	if f[-4:] == ".csv":
		with open("./agencies/vta/raw_csv/" + f, 'rb') as csvfile:
			feed[f[:-4]] = df_helper.csv2df(csvfile)

agency_id = 10

columns = ['agency_id', 'route_short_name', 'route_dir', 'pattern_id', 'stop_id', 'seq', 'is_time_point', 'version']
tables["Route_stop_seq"] = pd.DataFrame()

# sorted routes (sorted trip_ids don't correspond to sorted routes, so don't actually sort)
sr = feed['routes']
# sorted trips
st = feed['trips'].sort_values('trip_id').reset_index(drop=True)
# sorted stop times (trip_id, then stop_sequence)
sst = feed['stop_times'].sort_values(['trip_id', 'stop_sequence']).reset_index(drop=True)

trip2pattern = {}
route_names = {}
route_id = -1
route_name = ''
tag = ''
stop_idx = 0
checksum = 0
patterns = {}

# TODO(erchpito) add some documentations
for i, row in st.iterrows():
	trip_id = row['trip_id']
	dir_id = row['direction_id']
	trip_stops = []
	if row['route_id'] != route_id:
		route_id = row['route_id']
		if route_id not in route_names:
			route_row = sr.loc[sr['route_id'] == route_id].iloc[0]
			route_names[route_id] = route_row['route_short_name' if 'route_short_name' in route_row else 'route_long_name']
			patterns['{0}_{1}'.format(route_names[route_id], 0)] = []
			patterns['{0}_{1}'.format(route_names[route_id], 1)] = []
		route_name = route_names[route_id]
	tag = '{0}_{1}'.format(route_name, dir_id)
	while stop_idx < len(sst.index) and sst.iloc[stop_idx]['trip_id'] == trip_id:
		trip_stops += [sst.iloc[stop_idx]['stop_id']]
		stop_idx += 1
	if str(trip_stops) not in patterns[tag]:
		patterns[tag] += [str(trip_stops)]

	pattern_id = "{0}_{1}".format(tag, patterns[tag].index(str(trip_stops)) + 1)
	trip2pattern[trip_id] = pattern_id

	for i in range(len(trip_stops)):
		new_row = {}
		new_row['agency_id'] = agency_id
		new_row['route_short_name'] = route_name
		new_row['route_dir'] = dir_id
		new_row['pattern_id'] = pattern_id
		new_row['stop_id'] = str(trip_stops[i])
		new_row['seq'] = i + 1
		new_row['is_time_point'] = int(optional_field(stop_idx - len(trip_stops) + i, 'timepoint', sst, 0))
		new_row['version'] = checksum
		tables["Route_stop_seq"] = tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)
	print trip_id
for tags in patterns:
	for sequence in patterns[tag]:
		print "{0}: {1} = {2}".format(tag, patterns[tag].index(sequence) + 1, sequence)
print tables['Route_stop_seq']


	# probs not O(n) since it has to look for all the sequences before it goes through them linearly.

# for i, row in feed['routes'].iterrows():
# 	route_id = row['route_id']
# 	patterns = []
# 	for j, subrow in feed['trips'].loc[feed['trips']['route_id'] == route_id].iterrows():
# 		trip_id = subrow['trip_id']
# 		direction_id = subrow['direction_id'] if 'direction_id' in subrow else 0
# 		trip_id_block = feed['stop_times'].loc[feed['stop_times']['trip_id'] == trip_id]
# 		sequence = trip_id_block['stop_id'].tolist()
# 		if str(sequence) not in patterns:
# 			patterns += [str(sequence)]
# 		pattern_num = patterns.index(str(sequence)) + 1
# 		route_short_name = str(optional_field(i, 'route_short_name', feed['routes'], feed['routes'].iloc[i]['route_long_name']))
# 		pattern_id = "{0}_{1}_{2}".format(route_short_name, direction_id, pattern_num)
# 		for k, subsubrow in trip_id_block.iterrows():
# 			new_row = {}
# 			new_row['agency_id'] = agency_id
# 			new_row['route_short_name'] = route_short_name
# 			new_row['route_dir'] = direction_id
# 			new_row['pattern_id'] = pattern_id
# 			pattern_id = new_row['pattern_id']
# 			new_row['stop_id'] = str(subsubrow['stop_id'])
# 			new_row['seq'] = subsubrow['stop_sequence']
# 			new_row['is_time_point'] = int(optional_field(k, 'timepoint', feed['stop_times'], 0))
# 			new_row['version'] = checksum
# 			tables["Route_stop_seq"] = tables["Route_stop_seq"].append(pd.Series(new_row), ignore_index=True)
# 		trip2pattern[trip_id] = pattern_id
	# for sequence in patterns:
	# 		logging.debug("{0}: {1} = {2}".format(route_id, patterns.index(sequence) + 1, sequence))