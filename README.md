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
2. 

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
