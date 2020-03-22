import pandas as pd
import numpy as np
import asyncio
import time
import colorcet as cc
import geopandas as gpd
import datetime as datet
from shapely import wkt

from cartopy import crs
from SubwayMapModel import CurrentTransitTimeDelays
from models import Line
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from tornado import gen

# homemade modules
import sys
# sys.path.append('/home/tbartsch/source/repos')
sys.path.append('../../')
import mtatracking.MTAdatamodel as MTAdatamodel
from mtatracking.MTAgeo import addStationID_andNameToGeoPandas
from mtatracking.MTAdatamine import MTAdatamine
from mtatracking.MTAdatamodel import SubwaySystem
from mtatracking.utils import utils

_executor = ThreadPoolExecutor(1)


class SubwayMapData():
    '''class to encapsulate stations and lines dataframes.
    Implements observer pattern. Register callbacks in
    self._stations_observers and self._lines_observers.
    '''

    def __init__(self, session):
        '''initialize a new SubwayMapData object
        Args:
            session: sqlalchemy session to subway system database
        '''
        self._stations_observers = []
        self._lines_observers = []

        self.stationsdf = initializeStations(session)
        self.linesdf = initializeLines(session)

        self._selected_dir = 'N'
        self._selected_line = 'Q'
        self.session = session

        self._line_ids = sorted(
            list(set([l.name for l in session.query(Line).all()])))

    @property
    def line_ids(self):
        '''list of all line ids in the system'''
        return self._line_ids

    @property
    def selected_dir(self):
        '''the direction selected in the view'''
        return self._selected_dir

    @selected_dir.setter
    def selected_dir(self, v):
        self._selected_dir = v[:1]  # only save first letter (North = N)

    @property
    def selected_line(self):
        '''the line selected in the view'''
        return self._selected_line

    @selected_line.setter
    def selected_line(self, v):
        self._selected_line = v
        print('highlighting line ', v)
        if v == 'All':
            self.linesdf = colorizeAllLines(self.linesdf)
        else:
            self.linesdf = highlightOneLine(self.linesdf, v)

    @property
    def stationsdf(self):
        return self._stationsdf

    @stationsdf.setter
    def stationsdf(self, v):
        self._stationsdf = v
        for callback in self._stations_observers:
            callback(self._stationsdf)

    @property
    def linesdf(self):
        return self._linesdf

    @linesdf.setter
    def linesdf(self, v):
        self._linesdf = v
        for callback in self._lines_observers:
            callback(self._linesdf)

    def bind_to_stationsdf(self, callback):
        print('bound')
        self._stations_observers.append(callback)

    def bind_to_linesdf(self, callback):
        print('bound')
        self._lines_observers.append(callback)

    def PushStationsDF(self):
        '''await functions that update the stationsdf, then await this function.
        This triggers push of the dataframe to the view.
        '''
        print('hello from the push callback')
        for callback in self._stations_observers:
            callback(self._stationsdf)

    async def queryDB_async(self, loop):
        '''get information about the current position of trains
        in the subway system, track their position and update the
        probability of train delays for traversed segments of the system.
        This in turn should then trigger callbacks in the setter of the
        stationsdf property.

        Args:
            loop: IOLoop for async execution
        '''

        # reset all stations to grey.
        self.stationsdf.loc[:, 'color'] = '#585858'
        self.stationsgeo.loc[:, 'displaysize'] = 4
        self.stationsgeo.loc[:, 'MTAdelay'] = False
        self.stationsgeo.loc[:, 'waittimecolor'] = '#585858'
        self.stationsgeo.loc[:, 'delay_prob'] = np.nan
        self.stationsgeo.loc[:, 'waittime_str'] = 'unknown'
        self.stationsgeo.loc[:, 'inboundtrain'] = 'N/A'
        self.stationsgeo.loc[:, 'inbound_from'] = 'N/A'

        await self._updateStationsDfDelayInfo(delays, trains, stations, current_time, loop)
        await self._updateStationsDfWaitTime(myRTsys, stations, current_time, self.selected_dir, self.selected_line)

        self.stationsdf = stations
        print('done with iteration')
        #delays_filename = 'delays' + datetime.today().strftime('%Y-%m-%d') + '.pkl'
        #utils.write(delays, delays_filename)

    @gen.coroutine
    def update(self, stations):
        self.stationsdf = stations
        

    async def _getdata(self, dmine, feed_id, waittime):
        tracking_results = dmine.TrackTrains(feed_id)
        await asyncio.sleep(waittime)
        return tracking_results


    async def _updateStationsDfDelayInfo(self, delays, trains, stations, current_time, loop):
        '''update 'color' and 'displaysize' columns in the data frame, reflecting the probability that a subway will reach a station with a delay
        
        Args:
            delays: dictionary of delay objects
            trains: the trains we are currently tracking
            stations: stations data frame
            current_time: current time stamp
            loop: IOLoop for async execution
        
        '''
        ids = np.asarray([(train.route_id, train.direction) for train in trains])
        for line_id, delay in delays.items():
            line = line_id[:-1]
            direction = line_id[-1:]
            these_trains = trains[np.bitwise_and(ids[:,0] == line, ids[:,1] == direction)]
            #print('updating line ' + line_id)
            await loop.run_in_executor(_executor, delay.updateDelayProbs, these_trains, current_time)
            
            for train in these_trains:
                #get the MTA delay info and populate df with that
                MTADelayMessages = train.MTADelayMessages
                if len(MTADelayMessages) > 0:
                    if(np.abs(current_time - np.max(MTADelayMessages))) < 40:
                        arr_station = train.arrival_station_id[:-1]
                        stations.loc[stations['stop_id']==arr_station, 'MTAdelay']=True

            if (line == self.selected_line or self.selected_line == 'All') and direction == self.selected_dir:
                for key, val in delay.delayProbs.items():
                    k = key.split()
                    if not np.isnan(val):
                        col = cc.CET_D4[int(np.floor(val*255))]
                        size = 5 + 3 * val
                    else:
                        # col = cc.CET_D4[0]
                        col = '#585858'
                        size = 5
                    stations.loc[stations['stop_id']==k[2][:-1], 'color']=col
                    stations.loc[stations['stop_id']==k[2][:-1], 'displaysize']=size
                    stations.loc[stations['stop_id']==k[2][:-1], 'delay_prob']=val
                    stations.loc[stations['stop_id']==k[2][:-1], 'inboundtrain']=delay.train_ids[key]
                    stations.loc[stations['stop_id']==k[2][:-1], 'inbound_from']=k[0][:-1]


    async def _updateStationsDfWaitTime(self, subwaysys, stationsdf, currenttime, selected_dir, selected_line):
        '''update "waittime", "waittimedisplaysize", and "waittimecolor" column in data frame, reflecting the time (in seconds) that has passed since the last train visited this station.
        This is trivial if we are only interested in trains of a particular line, but gets more tricky if the user selected to view "All" lines
        
        Args: 
            subwaysys: subway system object containing the most recent tracking data
            stationsdf: stations data frame 
        '''
        for station_id, station in subwaysys.stations.items():
            if station_id is not None and len(station_id) > 1:
                station_dir = station_id[-1:]
                s_id = station_id[:-1]
                wait_time = None
                if station_dir == selected_dir and selected_line is not 'All': #make sure we are performing this update according to the direction selected by the user
                    wait_time = station.timeSinceLastTrainOfLineStoppedHere(selected_line, selected_dir, currenttime)
                elif station_dir == selected_dir and selected_line == 'All':
                    wait_times = []
                    #iterate over all lines that stop here
                    lines_this_station = list(station.trains_stopped.keys()) #contains direction (i.e. QN instead of Q)
                    lines_this_station = list(set([ele[:-1] for ele in lines_this_station]))
                    for line in lines_this_station:
                        wait_times.append(station.timeSinceLastTrainOfLineStoppedHere(line, selected_dir, currenttime))
                    wait_times = np.array(wait_times)
                    wts = wait_times[wait_times != None]
                    if len(wts) > 0:
                        wait_time = np.min(wait_times[wait_times != None])
                    else:
                        wait_time = None
                if(wait_time is not None):
                    stationsdf.loc[stationsdf['stop_id']==s_id, 'waittime']=wait_time #str(datet.timedelta(seconds=wait_time))
                    stationsdf.loc[stationsdf['stop_id']==s_id, 'waittime_str'] = timedispstring(wait_time)
                    #spread colors over 30 min. We want to eventually replace this with a scaling by sdev
                    if(int(np.floor(wait_time/(30*60)*255)) < 255):
                        col = cc.fire[int(np.floor(wait_time/(30*60)*255))]
                    else:
                        col = cc.fire[255]
                    stationsdf.loc[stationsdf['stop_id']==s_id, 'waittimecolor']=col
                    stationsdf.loc[stationsdf['stop_id']==s_id, 'waittimedisplaysize']=5 #constant size in this display mode        


def initializeStations(session):
    ''' load the locations of stations in the NYC subway system.
    Args:
        session (string): sqlalchemy database session

    Returns:
        stations: dataframe including "geometry" columns for
                plotting with geoviews
    '''

    engine = session.get_bind()
    df = pd.read_sql_table("Stop_geojson", engine, schema="public")
    df['geometry'] = df['geometry'].apply(wkt.loads)
    stationsgeo = gpd.GeoDataFrame(df, geometry='geometry')

    stationsgeo['color'] = '#585858'
    stationsgeo['displaysize'] = 3
    stationsgeo['delay_prob'] = np.nan
    stationsgeo['MTAdelay'] = False
    stationsgeo['inboundtrain'] = 'N/A'
    stationsgeo['inbound_from'] = 'N/A'

    stationsgeo['waittime'] = np.nan
    stationsgeo['waittime_str'] = 'unknown'
    stationsgeo['waittimedisplaysize'] = 3
    stationsgeo['waittimecolor'] = '#585858'
    stationsgeo['waittimedisplaysize'] = 3

    return stationsgeo


def initializeLines(session):
    ''' load the locations of lines in the NYC subway system.
    Args:
        session (string): sqlalchemy database session

    Returns:
        lines: dataframe including "geometry" columns for
                plotting with geoviews
    '''
    engine = session.get_bind()

    df = pd.read_sql_table("Line_geojson", engine, schema="public")
    df['geometry'] = df['geometry'].apply(wkt.loads)
    linesgeo = gpd.GeoDataFrame(df, geometry='geometry')
    linesgeo['color'] = cc.blues[1]
    linesgeo = colorizeAllLines(linesgeo)
    return linesgeo


def colorizeAllLines(linesdf):
    ''' set all lines in the linesdf to their respective colors.
    Args:
        linesdf: the lines dataframe

    Returns:
        linesdf: lines dataframe with modified colors column
    '''

    for line_id in linesdf.name.unique():
        linesdf.loc[
            linesdf['name'].str.contains(line_id), 'color'
            ] = LineColor(line_id)

    return linesdf


def highlightOneLine(linesdf, lineid):
    ''' set a single line in the linesdf to its respective color.
    All others are set to grey.
    Args:
        linesdf: the lines dataframe
        lineid: id of the line to colorize.
            This can be either with or without its direction
            ('Q' and 'QN' produce the same result)

    Returns:
        linesdf: lines dataframe with modified colors column
    '''
    lineid = lineid[0]
    linesdf['color'] = '#484848'
    linesdf.loc[
        linesdf['name'].str.contains(lineid), 'color'
        ] = LineColor(lineid)

    return linesdf


def LineColor(lineid):
    '''return the color of line lineid
    Args:
        lineid: id of the line to colorize.
        This can be either with or without its direction
        ('Q' and 'QN' produce the same result)
    Returns:
        color
    '''

    lineid = lineid[0]

    colors = [
        '#2850ad',
        '#ff6319',
        '#6cbe45',
        '#a7a9ac',
        '#996633',
        '#fccc0a',
        '#ee352e',
        '#00933c',
        '#b933ad',
        '#00add0',
        '#808183']

    lines_ids = [
        ['A', 'C', 'E'],
        ['B', 'D', 'F', 'M'],
        ['G'],
        ['L'],
        ['J', 'Z'],
        ['N', 'Q', 'R', 'W'],
        ['1', '2', '3'],
        ['4', '5', '6'],
        ['7'],
        ['T'],
        ['S']]

    c = pd.Series(colors)
    ids = pd.DataFrame(lines_ids)

    return c[(ids == lineid).any(axis=1)].to_numpy()[0]

def timedispstring(secs):
    hms = str(datet.timedelta(seconds=round(secs))).split(':')
    if hms[0] == '0':
        if hms[1].lstrip("0") == '':
            return hms[2].lstrip("0") + ' s'
        else:
            return hms[1].lstrip("0") + ' min ' + hms[2].lstrip("0") + ' s'
    else:
        return hms[0].lstrip("0") + ' hours ' + hms[1].lstrip("0") + ' min ' + hms[2].lstrip("0") + ' s'