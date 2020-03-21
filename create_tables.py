import sys
import pandas as pd
import geopandas as gpd
import numpy as np
from cartopy import crs
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from models import Base, Stop


# Populate the stations table
def populateStationsTable():
    df = pd.read_csv('resources/stops.txt')
    df = df.where(df.notnull(), None)
    for index, s in df.iterrows():
        thisstop = Stop(s['stop_id'], s['stop_name'], stop_code=s['stop_code'],
                        desc=s['stop_desc'], stop_lat=s['stop_lat'],
                        stop_lon=s['stop_lon'], zone_id=s['zone_id'],
                        stop_url=s['stop_url'],
                        location_type=s['location_type'],
                        parent_station=s['parent_station'])
        session.merge(thisstop)
    # Make a catchall object for completely Unknown stations:

    thisstop = Stop('Unknown', 'Unknown')
    session.merge(thisstop)
    session.commit()


def initializeStationsAndLines(lines_geojson, stations_geojson, stationsDF):
    '''load the locations of stations and lines in the NYC subway system.
    Args:
        lines_geojson (string): path to the lines_geojson file
        stations_geojson (string): path to the stations_geojson file
            from: https://data.cityofnewyork.us/Transportation/Subway-Stations/arq3-7z49
        stationsDF (pd.DataFrame): dataframe of database table
            from: https://data.cityofnewyork.us/Transportation/Subway-Lines/3qz8-muuu

    Returns:
        (stations, lines) (tuple of dataframes including "geometry"
                            columns for plotting with geoviews)
    '''

    def lookUpStationByLoc(lat, lon, stationsDF):
        '''return the station id of the station closest to stop_lat, stop_lon.

        Args:
            lat (float): latitude
            lon (float): longitude
            stationsDF (dataframe): dataframe of station information
                                    (from database)

        Returns:
            station_id, station_name (string, string): id and name of the
                                    station closest to the given lat and lon
        '''
        longs = stationsDF['stop_lon']
        lats = stationsDF['stop_lat']

        d = np.sqrt(np.power((longs - lon), 2) + np.power((lats - lat), 2))

        stationindices = np.where(d == np.nanmin(np.array(d)))
        station_ids = np.array(stationsDF['id'])[np.array(stationindices)]
        station_names = np.array(stationsDF['name'])[np.array(stationindices)]
        parent_stations = np.array(
            stationsDF['parent_station'])[np.array(stationindices)]
        a = pd.isnull(np.array(parent_stations.flatten(), dtype=object))
        return station_ids.flatten()[a][0], station_names.flatten()[a][0]

    def addStationID_andNameToGeoPandas(geopandasDF, stationsDF):
        '''inserts columns stop_id and stop_name from the stationsDF
        (from gtfs) into the geopandasDF'''

        geopandasDF['stop_id'] = geopandasDF.apply(
            lambda row: lookUpStationByLoc(
                row['geometry'].y, row['geometry'].x, stationsDF)[0], axis=1)
        geopandasDF['stop_name'] = geopandasDF.apply(
            lambda row: lookUpStationByLoc(
                row['geometry'].y, row['geometry'].x, stationsDF)[1], axis=1)

    lines = gpd.read_file(lines_geojson, crs=crs.LambertConformal())
    stations = gpd.read_file(stations_geojson, crs=crs.LambertConformal())
    addStationID_andNameToGeoPandas(stations, stationsDF)

    return (stations, lines)


if __name__ == '__main__':

    print("Enter database name: ")
    name = sys.stdin.readline()

    engine = create_engine(f'postgresql://tbartsch:test@localhost/{name}')

    Base.metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    populateStationsTable()

    # make auxiliary tables for station and line geometry
    stations_str = '/home/tbartsch/data/mtadata/subway_geo/subway_geo.geojson'
    lines_str = '/home/tbartsch/data/mtadata/subway_geo/subway_stations_geo.geojson'

    stationsDF = pd.read_sql_table("Stop", engine, schema="public")
    stations, lines = initializeStationsAndLines(stations_str, lines_str, stationsDF)
    stations['geometry'] = stations.geometry.apply(lambda g: g.wkt)
    stations.to_sql("Stop_geojson", engine, schema="public", if_exists='replace')
    lines['geometry'] = lines.geometry.apply(lambda g: g.wkt)
    lines.to_sql("Line_geojson", engine, schema="public",if_exists='replace')
