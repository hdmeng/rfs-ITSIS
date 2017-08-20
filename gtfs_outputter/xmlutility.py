from datetime import datetime
import requests

PREDICTION_FIELDS = ['routeShortName', 'routeName', 'routeId', 'stopId', 'stopName', 'stopCode', 'destinations']
DESTINATION_FIELDS = ['directionId', 'dir', 'headsign', 'predictions']
DESTINATION_PREDICTION_FIELDS = ['time', 'sec', 'min', 'departure', 'trip', 'tripPattern', 'vehicle', 'notYetDeparted']

VEHICLE_FIELDS = ['schAdhStr', 'direction', 'vehicleType', 'routeShortName', 'serviceName', 'loc', 'trip', 'headsign', 'serviceId', 'routeId', 'nextStopName', 'routeName', 'blockMthd', 'nextStopId', 'schAdh', 'tripPattern', 'id', 'block']
LOC_FIELDS = ['lat', 'lon', 'heading', 'time']

url = 'http://api.transitime.org/api/v1/key/5ec0de94/agency/{0}/command/{1}?{2}'

def get_transittime(agency, mode):
    # request GTFS-Static
    url.format('tridelta', 'vehicleDetails', 'r=393')  # for vehicles
    url.format('tridelta', 'predictions', 'rs=393|818889')  # for route-stop
    request = requests.get(url.format('tridelta', 'predictions', 'rs=393|818889'))

    # if unsuccessful
    if request.status_code != 200:
        return None

    data = request.json()
    predictions = (data['preds'] if 'preds' in data.keys() else data)['predictions']
    for prediction in predictions:
        for destination in prediction['destinations']:
            for dest_prediction in destination['predictions']
                print(datetime.fromtimestamp(dest_prediction['time']).strftime('%H:%M:%S'))

    data = request.json()
    data['vehicles']  # a list of vehicles
