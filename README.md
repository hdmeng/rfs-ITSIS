# rfs-ITSIS
The working directory of ITSIS project 

## `gtfs_outputter`
`outputter.py` is a Python program that gathers transit agency GTFS feeds (both static and realtime) and computes tables as described in *Tasks of PATH Dynamic Transit Research (Thru Feb 2016)*. 

### Files
File | Description | Comment 
--- | --- | ---
`df_helper.py` | helper methods for Pandas DataFrames |  
`gtfs_helper.py` | helper methods and classes for reading static and realtime GTFS feeds |
`outputter.py` | main script for generating the tables |
`transit_agencies.py` | helper methods for getting transit agency data |

### Running `outputter.py`
-----
#### With a MySQL Database
1. Make sure your MySQL Server is online, and that you have the privilege to create a database and tables.
2. Run ```python outputter.py AGENCY -u USERNAME -db DATABASE``` in the Terminal, where `AGENCY` is the input name of an agency listed in `transit_agencies.py`, `USERNAME` is your username for the MySQL server, and `DATABASE` is the name of the database you would like to connect to or create.
3. Enter your password for the MySQL server
4. For more options, you can use flags; for example, running ```python outputter.py AGENCY -u USERNAME -db DATABASE -n -d``` would replace all existing tables in the database and print out debug logs in the terminal.

> *MySQL implementation is incomplete; tables are not guaranteed to be stored correctly*

#### With .csv Files
1. Run ```python outputter.py AGENCY -l``` in the Terminal, where `AGENCY` is the input name of an agency listed in `transit_agencies.py`.
2. For more options, you can use flags; for example, running ```python outputter.py AGENCY -n -d``` would replace all existing .csv files locally and print out debug logs in the terminal.

### General Process
-----
1. Check to see if local static GTFS files exists and are valid (via `feed_end_date` in `feed_info.csv`). If so, load the static GTFS files into Pandas DataFrames; otherwise, use the static GTFS link in `transit_agencies.py` to download and store the static GTFS data.
2. Download realtime GTFS feeds from link, and calculate tables using both types of GTFS feeds. If table is already present in either the MySQL database or as a local .csv file, simply load the table.

### Flags
Flag | Action | Input | Comment
--- | --- | --- | ---
`-a` | runs on all transit agencies | |
`-d` | runs on debug mode | |
`-db` | set database | a database name | `outputter` will ask for a database name again if the provide database name is invalid
`-h` | set host | a host name | `outputter` will ask for a hostname again if the provide hostname is invalid; defaults to `localhost`
`-i` | runs on info mode | | *currently unused*
`-l` | reads and writes table to .csv files locally | |
`-n` | overrides all tables and saved data with data pulled from online | |
`-u` | set user |  a username | `outputter` will ask for a username again if the provide username is invalid

### Transit Agencies
Agency | Input | Agency Code | Static GTFS | Realtime GTFS (Alert) | Realtime GTFS (Trip Update) | Realtime GTFS (Vehicle Position)
--- | --- | --- | --- | --- | --- | --- |
[Bay Area Rapid Transit (BART)](http://www.bart.gov) | `bart` | `0` | :white_check_mark: | :white_check_mark: | :white_check_mark: | 
[Santa Clara Valley Transportation Authority (VTA)](http://www.vta.org) | `vta` | `10` | :white_check_mark: | | :white_check_mark: | :white_check_mark: 
[Tri Delta Transit](http://trideltatransit.com) | `tri_delta` | `11` | :white_check_mark: | :white_check_mark: | :white_check_mark: 
> *Agency Code are completely arbitrary; they just need to be different*

### Classes and Methods
-----
## `df_helper.py`
```python
def csv2df(csv_file):
  # loads csv_file as a pandas.DataFrame
  # 
  # File csv_file - a .csv
  #
  # Returns:
  #   pandas.DataFrame 
  
def df2sql(dataframe, df_name, login, exist_flag='append'):
  # saves a pandas.DataFrame as a MySQL table
  #
  # pandas.DataFrame dataframe - a table
  # String df_name - table name
  # dict login - login credentials
  # String exist_flag - 'append' or 'replace'
  
def sql2df(df_name, login):
  # loads a MySQL table as a pandas.DataFrame
  #
  # String df_name - table name
  # dict login - login credentials
  # 
  # Returns:
  #   pandas.DataFrame
```

## `gtfs_helper.py`
```python
class TripUpdate(object):
  def __init__(self, trip_update):
    # makes TripUpdate instance out of a trip_update
    #
    # message trip_update - trip update from a realtime GTFS Trip Update feed
    #
    # Returns:
    #   TripUpdate
  
  def get_trip_descriptor(self):
    # get trip descriptor of a TripUpdate (process_trip_descriptor must be called first)
    #
    # Returns:
    #   dict
  
  def get_stop_time_updates(self):
    # get list of Stop Time Updates (process_stop_time_updates must be called first)
    #
    # Returns:
    #   list
  
  def process_trip_descriptor(self):
    # loads trip descriptor from TripUpdate data
    #
    # Returns:
    #   dict
  
  def process_stop_time_updates(self):
    # loads stop time updates from TripUpdate data
    #
    # Returns:
    #   list
  
  def process_stop_time_update(self, stu):
    # loads stop time update from TripUpdate data
    #
    # message stu - stop time update from a trip dpdate
    #
    # Returns:
    #   dict
  
  def process_stop_time_event(self, ste):
    # loads stop time event from TripUpdate data
    #
    # message ste - stop time event from a stop time update
    #
    # Returns:
    #   dict

class VehiclePosition(object):
  def __init__(self, vehicle_position):
    # makes VehiclePosition instance out of a vehicle_position
    #
    # message vehicle_position - vehicle posiiton from a realtime GTFS Vehicle Position feed
    #
    # Returns:
    #   VehiclePosition
  
  def get_trip_descriptor(self):
    # get trip descriptor of a VehiclePosition (process_trip_descriptor must be called first)
    #
    # Returns:
    #   dict
  
  def get_position(self):
    # get position of a VehiclePosition (process_position must be called first)
    #
    # Returns:
    #   dict
  
  def get_vehicle_descriptor(self):
    # get vehicle descriptor of a VehiclePosition (process_vehicle_descriptor must be called first)
    #
    # Returns:
    #   dict
  
  def get_update_fields(self):
    # get misc data of a VehiclePosition
    #
    # Returns:
    #   dict
  
  def process_trip_descriptor(self):
    # loads trip descriptor from VehiclePosition data
    #
    # Returns:
    #   dict
  
  def process_position(self):
    # loads position from VehiclePosition data
    #
    # Returns:
    #   dict
  
  def process_vehicle_descriptor(self):
    # loads vehicle descriptor from VehiclePosition data
    #
    # Returns:
    #   dict

# THIS DOES NOT WORK PROPERLY
def hasher(file, checksum, blocksize=65536):
  # generates a checksum for a file
  # 
  # File file - a file
  # func checksum - a checksum function from hashlib
  # int blocksize - amount of bytes to read from file at a time
  #
  # Returns:
  #   int
  
def get_realtime(agency, mode):
  # gets a realtiem GTFS feed given an agency and type
  #
  # String agency - agency name
  # String mode - type of realtime GTFS feed
  # 
  # Returns:
  #   gtfs_realtime_pb2
  
def get_static(agency, refresh):
  # loads static GTFS data as pandas.DataFrame, either from online or from local files
  # saves static GTFS data if pulled from online in raw .zip, raw .txt, and .csv
  # also returns checksum
  #
  # String agency - agency name
  # bool refresh - whether to pull new data
  # 
  # Returns:
  #   dict, int
```

## `outputter.py`
```python
# THIS IS THE BULK OF THE CODE, WHERE EACH TABLE IS CREATED; THIS IS INCOMPLETE
def interpret(agency, static_feed, trip_update_feed, alert_feed, vehicle_position_feed, checksum, refresh, login, local):
  # takes static and realtime GTFS feeds and loads tables as listed in 
  # "Tasks of PATH Dynamic Transit Research (Thru Feb 2016)"
  # 
  # String agency - agency name
  # dict static_feed - dict of pandas.DataFrames representing the static GTFS tables
  # gtfs_realtime_pb2 trip_update_feed - realtime GTFS Trip Update feed, or none if link not provided
  # gtfs_realtime_pb2 alert_feed - realtime GTFS Alert feed, or none if link not provided
  # gtfs_realtime_pb2 vehicle_position_feed - realtime GTFS Vehicle Position feed, or none if link not provided
  # int checksum - checksum of static GTFS .zip file
  # bool refresh - whether to replaced all calculated tables, either in database or locally
  # dict login - login credentials
  # bool local - whether to read and write from local .csv files or from a MySQL database
  
  def optional_field(index, column, dataframe, default='N/A'):
    # attempts to obtain data from an optional field in a table
    #
    # int index - the index of a row in a pandas.DataFrame
    # String column - field name
    # pandas.DataFrame dataframe - a table
    # String default - output if entry does not exist in dataframe
    #
    # Returns:
    #   varies
    
  def can_read_table(table):
    # checks to see if table exists
    #
    # String table - table name
    #
    # Returns:
    #   bool
  
  def read_table(table):
    # reads table from local .csv file or from a MySQL database
    # 
    # String table - table name
    #
    # Returns:
    #   pandas.DataFrame
  
  def write_table(table):
    # writes table to local .csv file or to a MySQL database
    #
    # String table - table name
  
# HOUR == 99 LOGIC UNIMPLEMENTED  
def datetimeFromHMS(timestamp):
  # converts a timestamp to a datetime.datetime object, considering 24+ hour for next day representation
  #
  # String timestamp - a timestamp
  #
  # Returns:
  #   datetime.datetime
  
def main(argv):
  # runs outputter.py script
  #
  # list argv - command line arguments
```

## `transit_agencies.py`
```python
def get(agency, field):
  # gets info from an agency
  #
  # String agency - agency name
  # String field - field name
  #
  # Returns:
  #   String

def isValidAgency(agency):
  # checks if given agency name is listed in agency_dict
  # i.e., if we have info on said agency
  #
  # String agency - agency name
  #
  # Returns:
  #   bool
```

## Resources
+ [Google General Transit Feed Specification (GTFS)](https://developers.google.com/transit/)
+ [Google Protocol Buffers (Protobuf)](https://developers.google.com/protocol-buffers/)
+ [google/transitfeed](https://github.com/google/transitfeed)
+ [Pandas](http://pandas.pydata.org)
+ [511.org](http://511.org)
+ [GTFS Data Exchange](http://www.gtfs-data-exchange.com) [*Shutting Down*]
+ [TransitFeeds](http://transitfeeds.com)
+ [Transitland](https://transit.land)
+ [TransitTime](http://api.transitime.org)

## See Also
+ [California Partners for Advanced Transportation Technology](http://www.path.berkeley.edu)
+ [Institute of Transportation Studies](http://its.berkeley.edu)
+ [University of California, Berkeley](http://www.berkeley.edu)

## Creators
**Wei-Bin Zhang**

**Joshua Meng**
+ [github.com/hdmeng](https://github.com/hdmeng)

**Kun Zhou**

**Eric Chen**
+ [github.com/erchpito](https://github.com/erchpito)
