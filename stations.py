import os

import numpy as np
import pandas as pd
from geopy.distance import distance as geo_distance

DATA_DIR='./data'


def get_default_station_metadata():
    path = os.path.join(DATA_DIR, 'stations', 'stations.csv')
    return pd.read_csv(path)
        

class StationLocator(object):
    def __init__(self, station_metadata=None):
        if station_metadata is None:
            self.station_metadata = get_default_station_metadata()
        else:
            self.station_metadata = station_metadata
    def station_distances(self, lat, lon):
        distances = []
        index = []
        for station in self.station_metadata.itertuples():
            index.append(station.Index)
            if np.isnan(lat) or np.isnan(lon):
                distances.append(np.nan)
            else:
                distance = geo_distance([lat,lon], [station.latitude, station.longitude]).km
                distances.append(distance)
        distances = pd.Series(distances, index=index)
        return distances
    def nearest_station(self, df, lat_col='latitude', lon_col='longitude'):
        nearest = []
        distance = []
        index = []
        for point in df.itertuples():
            latitude = getattr(point, lat_col)
            longitude = getattr(point, lon_col)
            index.append(point.Index)
            if np.isnan(latitude) or np.isnan(longitude):
                nearest.append(np.nan)
                distance.append(np.nan)
            else:
                distances = self.station_distances(latitude, longitude)
                min_distance = distances.min()
                nearest.append(self.station_metadata.loc[distances.idxmin()]['name'])
                distance.append(min_distance)
        return pd.DataFrame({
            'nearest_station': nearest,
            'distance_km': distance,
        }, index=index)