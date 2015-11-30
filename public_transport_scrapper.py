# coding: utf-8
import pandas

from rozkladzik_api import get_public_transport_time
from utils import high_traffic_date, low_traffic_date, weekend_midday_date, fetch_all, make_failure_aware

destination = (50.0658072, 19.9500146)

data = pandas.read_pickle("/home/ppastuszka/Dokumenty/cleanedapartmentdata.pkl")

data[u'latlong'] = zip(data[u'Szerokość geograficzna'], data[u'Długosć geograficzna'])

gps_points = list(data['latlong'].unique())

# @make_failure_aware
def find_travel_time(gps_data):
    return {
        'latlong': gps_data,
        u'Czas dojazdu komunikacją w korkach': get_public_transport_time(gps_data, destination, high_traffic_date),
        u'Czas dojazdu komunikacją bez korków': get_public_transport_time(gps_data, destination, low_traffic_date),
        u'Czas dojazdu komunikacją w weekend': get_public_transport_time(gps_data, destination, weekend_midday_date),
    }

from multiprocessing import Pool
pool = Pool(30)

travel_data = pandas.DataFrame(fetch_all(gps_points, find_travel_time, pool))
data = data.merge(travel_data, on='latlong')
data = data.drop('latlong', 1)

with open("/home/ppastuszka/Dokumenty/cleanedapartmentdata2.csv", "wb") as f:
    data.to_csv(f, encoding='utf-8')

data.to_pickle("/home/ppastuszka/Dokumenty/cleanedapartmentdata2.pkl")