import numpy as np
import pandas as pd
from pkgutil import get_data
import io
from itertools import combinations
from collections import defaultdict, Counter
from datetime import datetime, timedelta
from mtatracking_v2.models import (
    Train,
    Stop,
    Trains_stopped,
    Trip_update,
    Transit_time_fit,
    Line,
    Line_stops
)
from mtatracking_v2.mean_transit_times import (
    getTransitTimes,
    computeMeanTransitTimes,
    populate_database_with_fit_results
)
from sqlalchemy.orm.exc import NoResultFound


def StationCodesToNames(codes, session):
    '''convert a list of station codes to station names'''
    stations = session.query(Stop).filter(Stop.id.in_(codes)).all()
    stations_sorted = sorted(stations, key=lambda x: codes.index(x.id))
    station_names = [station.name for station in stations_sorted]
    return station_names


def getDistinctLines(session):
    '''return set of distinct subway lines in system'''
    lines = session.query(Trip_update.line_id).distinct()\
        .order_by(Trip_update.line_id).all()
    lines = [el[0] for el in lines]
    return lines


def getStationsAlongLine(line_id, direction, time_start, time_end, session):
    '''return a list of stations along a line.
    Compute this from historic subway system data
    This corresponds to all stations at which at least 5% of trains of
    that line have stopped (5% hard coded in SQL).

    Args:
        line_id (string): id of the subway line, for example 'Q'
        direction (string): direction. We currently only support 'N' or 'S'.
                            This could change if the MTA changes their mind.
        time_start (datetime): timepoint in the historic data at which to
                               start the computation
        time_end (datetime): timepoint in the historic data at which to
                               stop the computation
        session: the SQLAlchemy database session.

    Returns:
        stations: list of station ids along this subway line.
    '''

    sqltext = get_data('mtatracking_v2',
                       'sql_queries/stations_in_line_unordered.sql')
    stations = session.execute(
        sqltext.decode("utf-8").format(line_id, direction,
                                       time_start, time_end)
        ).fetchall()
    if stations:
        stations = list(np.array(stations)[:, 0])
    else:
        print('warning, no stations found')
    return list(stations)


def getStationIDsAlongLine_ordered(
                        line_id, direction, time_start, time_end, session):
    '''DEPRECATED! USE getStationIDsAlongLine_static INSTEAD

    return a list of stations for one line, IN THE MOST LIKELY ORDER.
    Compute this from historic subway system data.

    Args:
        line_id (string): id of the subway line, for example 'Q'
        direction (string): direction. We currently only support 'N' or 'S'.
                            This could change if the MTA changes their mind.
        time_start (datetime): timepoint in the historic data at which to
                                start the computation
        time_end (datetime): timepoint in the historic data at which to
                                stop the computation
        session: the SQLAlchemy database session.

    Returns:
        stations (list): ordered list of stations IDs
    '''

    # first get unordered stations:
    stations_along_line = getStationsAlongLine(
        line_id, direction, time_start, time_end, session)

    # find all trains that visited all of these stations
    sets_of_trains = []
    for stationid in stations_along_line:
        trains_stopped = session.query(
            Trains_stopped).filter(Trains_stopped.stop_id == stationid)
        train_ids = list(set([ts.train_unique_num for ts in trains_stopped]))
        sets_of_trains.append(train_ids)

    # intersect each list of trains with all other lists.
    # Check which one intersects with the most.

    common_trains = set(sets_of_trains[0])
    for t in sets_of_trains:
        common_trains = common_trains.intersection(t)

    trains = list(common_trains)

    # this should now be easy: at each station just sum up the timestamps
    # of all trains, then sort.
    avg_departure_times = []

    # TODO: the following could be made way faster
    # by implementing entirely in SQL. Did not have time to fight with that
    # right now. Especially considering that we will not be calling this
    # function super often.
    for stationid in stations_along_line:
        avg_departure_time = 0
        for train in trains:
            try:
                ts = session.query(Trains_stopped).join(Train).filter(
                    (Trains_stopped.stop_id == stationid) & (
                        Trains_stopped.train_unique_num == train)).all()
            except NoResultFound as e:
                print('No data for this train in database', e)
            avg_departure_time += (
                ts[-1].stop_time - datetime(1970, 1, 1)).total_seconds()
        avg_departure_times.append(avg_departure_time)

    stations_ordered = [
        s for _, s in sorted(zip(avg_departure_times, stations_along_line))]

    return stations_ordered


def depositStationsInLineInDB(line_name, direction, station_IDs_ordered,
                              timestamp, session):
    '''Update the line definition in the database
    Args:
        line_name: name of the subway line (e.g. 'Q')
        direction: direction (N or S)
        station_IDs_ordered: output of getStationIDsAlongLine_static.
            This is a dict of dicts.
        timestamp: timestamp to associate with this line definition
        session: SQLAlchemy session to our database
    '''
    line = Line(line_name, direction, timestamp)
    session.add(line)
    session.flush()
    days = ['Weekday', 'Saturday', 'Sunday']
    for day in days:
        startTime = None
        for times, stations in station_IDs_ordered[day].items():
            from_hour, to_hour = times.split('-')
            if stations:
                # we have stations for the new time segment
                # does the new segment start where the last ended?
                if startTime is None:
                    startTime = from_hour
                    currentStop = to_hour
                else:
                    if from_hour == currentStop:
                        # everything is fine
                        startTime = from_hour
                        currentStop = to_hour
                    else:
                        # something went wrong, notify the user
                        print('segment time mismatch!')
                        print('line ' + line_name + ' direction ' + direction + ' day ' + str(day))
                        startTime = currentStop  # try to fix the mismatch
                        currentStop = to_hour
                for i, station_id in enumerate(stations):
                    lstop = Line_stops(
                        station_id, line.id, i, day, startTime, currentStop)
                    session.add(lstop)
    session.commit()


def getStationIDsAlongLine_static(
                        line_id, direction,
                        filename_static_trips='stop_times.txt'):
    stoptimes = get_data(
                'mtatracking_v2', 'static/' + filename_static_trips)
    trips = pd.read_csv(io.BytesIO(stoptimes), encoding='utf8')

    def parse_trip_id(trip_id):
        """Decode the trip id and find trip origin time, line number,
        and direction

        Returns:
            Tuple of (origin time, line, direction, path id)
        """
        ts = trip_id.split('_')
        day = ts[0].split('-')[-2]
        # origin time of the trip in hours past midnight.
        origin_time = int(int(ts[1])/100/60)
        trip_path = ts[2]
        line = trip_path.split('.')[0]
        path_id = trip_path.split('.')[-1]
        direction = path_id[0]
        path_id = path_id[1:]
        return (day, origin_time, line, direction, path_id)

    def get_station_order_lists(trips):
        res = defaultdict(list)
        tg = trips.groupby('trip_id')
        for g in tg:
            day, hour, route, direction, path_id = parse_trip_id(g[0])
            res[(route, direction, day, hour)].append(g[1]['stop_id'])
        return res

    def most_common_station_order_in_class(res, c=('2', 'N', 'Weekday', 17)):
        '''return the most common sequence of stations visited by a
        line for a given day and hour

        Returns:
            (fraction of trains with this stop sequence, stop sequence)
        '''
        tuples = [tuple(r) for r in res[c]]

        counts = Counter(tuples)

        if(counts):
            counts_sorted = sorted(counts.items(), key=lambda c: -c[1])

            common_order = counts_sorted[0][0]
            num_order = counts_sorted[0][1]

            if len(tuples) != 0:
                ratio = num_order/len(tuples)
            else:
                ratio = 1
            return ratio, common_order
        else:
            return 1, ()

    def get_station_order(tripsdf, line_id='2', direction='N'):
        '''return all station orders and valid time ranges for these orders'''
        res = get_station_order_lists(tripsdf)
        days = ['Weekday', 'Saturday', 'Sunday']
        hours = np.arange(24)
        results = {}
        for day in days:
            seqs = []
            for hour in hours:
                frac, seq =\
                    most_common_station_order_in_class(
                        res, c=(line_id, direction, day, hour))
                seqs.append(seq)
                if frac < 0.5:
                    print('Warning: fraction low for '
                          + str(day) + ' ' + str(hour) +
                          ' hours' + ' fraction: ' + str(frac))
            currentseq = seqs[0]
            start = 0
            daydict = {}
            for index, seq in enumerate(seqs):
                if currentseq is ():
                    if index < len(seqs)-1:
                        currentseq = seqs[index+1]
                    continue
                if seq and currentseq == seq:
                    daydict[str(start) + '-' + str(index)] = currentseq
                    currentseq = seq
                    start = index
            daydict[str(start) + '-' + str(24)] = currentseq
            results[day] = daydict
        return results

    stations = get_station_order(trips, line_id, direction)
    return stations


def updateAllLineDefinitionsInDB(
        session, current_date, path_to_static_trips='stop_times.txt'):
    '''From static data compute the definition of all lines and
    deposit into database'''

    lines = getDistinctLines(session)
    directions = ['N', 'S']
    for line in lines:
        for direction in directions:
            print('working on line ' + line + direction)
            ordered_stations = getStationIDsAlongLine_static(
                        line, direction,
                        filename_static_trips='stop_times.txt')
            print('have stations, depositing')
            depositStationsInLineInDB(line, direction, ordered_stations,
                                      current_date, session)


def getStationObjectsAlongLine_ordered(
                    line_id, direction, time_start, time_end, session):
    '''return a list of station objects for one line, IN THE MOST LIKELY ORDER.
    Compute this from historic subway system data.

    Args:
        line_id (string): id of the subway line, for example 'Q'
        direction (string): direction. We currently only support 'N' or 'S'.
                            This could change if the MTA changes their mind.
        time_start (datetime): timepoint in the historic data at which to
                                start the computation
        time_end (datetime): timepoint in the historic data at which to
                                stop the computation
        session: the SQLAlchemy database session.

    Returns:
        stations (list): ordered list of stations objects.
    '''

    station_ids = getStationIDsAlongLine_ordered(
        line_id, direction, time_start, time_end, session)
    stations = session.query(Stop).filter(Stop.id.in_(station_ids)).all()

    return sorted(stations, key=lambda x: station_ids.index(x.id))


def findDelaysAndSetDelayedAttrib(time_start, time_end, session):
    '''find delayed trains in historic data and set their delayed attribute.
    Analyzes ALL lines, and both directions, so this can take a while
     Args:
        time_start (datetime): timepoint in the historic data at which to
                                start the computation
        time_end (datetime): timepoint in the historic data at which to
                                stop the computation
        session: the SQLAlchemy database session.
    This function modifies the train objects of line line_id (it updates their
    delayed attribute). Changes are committed back to the database.
    '''
    directions = ['N', 'S']
    lines = getDistinctLines(session)
    for line in lines:
        for direction in directions:
            print('Working on ' + line + direction)
            thisHTD = historicTrainDelays(
                line, direction, time_start, time_end, session)
            thisHTD.checkAllTrainsInLine(n=8)


def getLatestSavedLinedefinition(line_id, direction, day, hour, session):
    '''retrieve an ordered list of stations from the database.
    You must have previously computed this ordered list and saved it.
    Computing the ordered list is computationally expensive, so use this
    function instead of a recomputation whenever possible.'''

    sqltext = get_data('mtatracking_v2',
                       'sql_queries/retrieve_ordered_stations.sql')
    stations = session.execute(
        sqltext.decode("utf-8").format(line_id, direction, day, hour)
        ).fetchall()
    if len(stations) > 1:
        stations = list(np.array(stations)[:, 0])
    else:
        return []
    return list(stations)


class historicTrainDelays():
    '''finds train delays in historic data and can update database'''

    def __init__(self, line_id, direction, time_start, time_end, session):
        '''create a historicTrainDelays instance.
        Args:
            line_id (string): id of the subway line, for example 'Q'
            direction (string): direction. We currently only support 'N'
                                or 'S'. This could change if the MTA
                                changes its mind.
            time_start (datetime): timepoint in the historic data at which to
                                    start the computation
            time_end (datetime): timepoint in the historic data at which to
                                    stop the computation
            session: the SQLAlchemy database session.
        '''
        self.line_id = line_id
        self.direction = direction
        self.time_start = time_start
        self.time_end = time_end
        self.session = session

        self.meansAndSdev_fit_dict = self.getHistoricMeansAndSdevs()
        self.trains = self.getTrains()
        self.getHistoricMeansAndSdevs()

    def getTrains(self):
        self.trains = self.session.query(Train).join(Trip_update).filter(
                    (Trip_update.line_id == self.line_id)
                    & (Trip_update.direction == self.direction)
                    & (Train.first_seen_timestamp > self.time_start)
                    & (Train.first_seen_timestamp < self.time_end)
                    ).all()
        return self.trains

    def getHistoricMeansAndSdevs(self):
        '''for outlier detection we need historic data.
        Get fit results from DB. If the database does not contain what we
        want we will fit it ourselves later and deposit it into the DB

        Returns: dict of fit objects. Keys: (orig_id, dest_id)
        '''
        meansAndSdev_fit = self.session.query(Transit_time_fit)\
            .filter(
                (Transit_time_fit.line_id == self.line_id)
                & (self.time_start >= Transit_time_fit.fit_start_datetime)
                & (self.time_end <= Transit_time_fit.fit_end_datetime)
                )\
            .all()

        self.meansAndSdev_fit_dict = {}
        for fit in meansAndSdev_fit:
            self.meansAndSdev_fit_dict[
                (fit.stop_id_origin, fit.stop_id_destination)] = fit

        return self.meansAndSdev_fit_dict

    def checkDelaysOfTrain(self, train, n=8, many_trains=False):
        '''for each stop of this train, check whether we had a delay.
        If there was a delay, update the Trains_stopped object.

        Args:
            train (Train): train object
            n (float): number of standard deviations beyond mean for
                       which we call a train delayed.
        '''
        for orig, dest in zip(train.stopped_at[::-1][:-1],
                              train.stopped_at[::-1][1:]):
            transit_time = dest.stop_time - orig.stop_time
            # compare to mean and sdev
            median, sdev = self.getMedianTravelTime(
                orig, dest, dest.stop_time)
            if median and sdev:
                if transit_time > timedelta(seconds=(median + n*sdev)):
                    dest.delayed = True
                    if many_trains is False:
                        self.session.commit()
                else:
                    dest.delayed = False
                    if many_trains is False:
                        self.session.commit()
                dest.delayed_magnitude = (
                    transit_time - timedelta(seconds=median))\
                    / timedelta(seconds=sdev)
        if many_trains is False:
            self.session.commit()

    def getMedianTravelTime(self, orig, dest, timestamp):
        '''Return the median travel time between orig and dest
        at time timestamp

        Args:
            orig (Stop ORM object)
            dest (Stop ORM object)
            timestamp (datetime)

        Returns:
            median, sdev
        '''

        if (orig.stop_id, dest.stop_id) in self.meansAndSdev_fit_dict:
            fit = self.meansAndSdev_fit_dict[(orig.stop_id, dest.stop_id)]
        else:
            # we haven't yet performed this fit, do it now
            transit_times = getTransitTimes(
                orig.stop_id, dest.stop_id,
                self.line_id, self.time_start, self.time_end, self.session)
            print('new fit, ' + orig.stop_id + ' to ' + dest.stop_id)
            res, sdev = computeMeanTransitTimes(transit_times)
            if res is None:
                self.meansAndSdev_fit_dict[(orig.stop_id, dest.stop_id)] = None
                return None, None
            fit = populate_database_with_fit_results(
                self.session, res, sdev, orig.stop_id, dest.stop_id,
                self.line_id, self.direction, self.time_start,
                self.time_end)
            self.meansAndSdev_fit_dict[(orig.stop_id, dest.stop_id)] = fit

        # determine delay
        # find the correct mean (we need the one that
        # belongs to our timestamp)
        sdev = None
        median = None
        if fit:
            for mean_tt in fit.medians:
                if ((timestamp >= mean_tt.seg_start_datetime)
                        and (timestamp <= mean_tt.seg_end_datetime)):
                    median = mean_tt.median
                    sdev = mean_tt.sdev
        return median, sdev

    def checkAllTrainsInLine(self, n=8, many_trains=True):
        print('numtrains to process: ', len(self.trains))
        for i, train in enumerate(self.trains):
            print(i)
            self.checkDelaysOfTrain(train, n, many_trains=many_trains)
        self.session.commit()
