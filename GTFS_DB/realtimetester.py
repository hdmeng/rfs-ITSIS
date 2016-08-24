from google.transit import gtfs_realtime_pb2
import urllib

feed = gtfs_realtime_pb2.FeedMessage()

#comment one out to see realtime info for each agency
#this gives information for VTA
response = urllib.urlopen('http://api.transitime.org/api/v1/key/5ec0de94/agency/vta/command/gtfs-rt/vehiclePositions')

#this gives information for Bart
response = urllib.urlopen('http://api.bart.gov/gtfsrt/tripupdate.aspx')
feed.ParseFromString(response.read())
i = 0
for entity in feed.entity:
    if (i > 5):
        break
    print entity
    # print entity
    i += 1

