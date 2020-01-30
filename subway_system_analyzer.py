import numpy as np
from pkgutil import get_data
from datetime import datetime, timedelta
from mtatracking_v2.models import (
    Train,
    Stop,
    Stop_time_update,
    Trains_stopped,
    Trip_update,
    Alert_message,
    Vehicle_message,
    Transit_time_fit
)
from mtatracking_v2.mean_transit_times import (
    getTransitTimes,
    computeMeanTransitTimes,
    populate_database_with_fit_results
)
from sqlalchemy.orm.exc import NoResultFound
from sqlalchemy.orm.exc import MultipleResultsFound


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
    stations = list(np.array(stations)[:, 0])
    return list(stations)


def getStationIDsAlongLine_ordered(
                        line_id, direction, time_start, time_end, session):
    '''return a list of stations for one line, IN THE MOST LIKELY ORDER.
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
                print(e)
            avg_departure_time += (
                ts[-1].stop_time - datetime(1970, 1, 1)).total_seconds()
        avg_departure_times.append(avg_departure_time)

    stations_ordered = [
        s for _, s in sorted(zip(avg_departure_times, stations_along_line))]

    return stations_ordered


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


def findDelaysAndSetDelayedAttrib(
                    line_id, direction, time_start, time_end, session):
    '''find delayed trains in historic data and set their delayed attribute
     Args:
        line_id (string): id of the subway line, for example 'Q'
        direction (string): direction. We currently only support 'N' or 'S'.
                            This could change if the MTA changes their mind.
        time_start (datetime): timepoint in the historic data at which to
                                start the computation
        time_end (datetime): timepoint in the historic data at which to
                                stop the computation
        session: the SQLAlchemy database session.
    This function modifies the train objects of line line_id (it updates their
    delayed attribute). Changes are committed back to the database.
    '''


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

    def checkDelaysOfTrain(self, train, n=8):
        '''for each stop of this train, check whether we had a delay.
        If there was a delay, update the Trains_stopped object.

        Args:
            train (Train): train object
            n (float): number of standard deviations beyond mean for
                       which we call a train delayed.
        '''
        self.getHistoricMeansAndSdevs()
        ttimes = None
        dels = None
        t= None
        for orig, dest in zip(train.stopped_at[::-1][:-1],
                              train.stopped_at[::-1][1:]):
            transit_time = dest.stop_time - orig.stop_time

            # compare to mean and sdev
            if (orig.stop_id, dest.stop_id) in self.meansAndSdev_fit_dict:
                fit = self.meansAndSdev_fit_dict[(orig.stop_id, dest.stop_id)]
            else:
                # we haven't yet performed this fit, do it now
                transit_times = getTransitTimes(
                    orig.stop_id, dest.stop_id,
                    self.line_id, self.time_start, self.time_end, self.session)
                res, sdev = computeMeanTransitTimes(transit_times)
                if res is None:
                    continue
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
            for mean_tt in fit.medians:
                if ((dest.stop_time >= mean_tt.seg_start_datetime)
                   and (dest.stop_time <= mean_tt.seg_end_datetime)):
                    median = mean_tt.median
                    sdev = mean_tt.sdev
            if median and sdev:
                if dest.stop_id == 'D42N' and orig.stop_id == 'D43N':
                    t = dest.stop_time
                    ttimes = transit_time
                    dels = dest.delayed
                    print("**********************************")
                    print('transit time: ', transit_time)
                    print('median: ', median)
                    print('sdev ', sdev)
                    print('threshold: ', timedelta(seconds=(median + n*sdev)))
                    print('delayed: ', transit_time > timedelta(seconds=(median + n*sdev)))
                if transit_time > timedelta(seconds=(median + n*sdev)):
                    dest.delayed = True
                    self.session.commit()
                else:
                    dest.delayed = False
                    self.session.commit()
            else:
                if dest.stop_id == 'D42N' and orig.stop_id == 'D43N':
                    print('warning: could not find segment '
                            'that matches timestamp', dest.stop_time)
                    print('available segs: ')
                    for mean_tt in fit.medians:
                        print('start: ' + str(mean_tt.seg_start_datetime) + ", stop: " + str(mean_tt.seg_end_datetime))

        self.session.commit()
        return (t, ttimes, dels)

    def checkAllTrainsInLine(self):

        for train in self.trains:
            self.checkDelaysOfTrain(train, n=3)
