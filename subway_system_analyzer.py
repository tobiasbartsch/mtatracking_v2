import numpy as np
from pkgutil import get_data
from datetime import datetime
from mtatracking_v2.models import (Train,
                                   Stop,
                                   Stop_time_update,
                                   Trains_stopped,
                                   Trip_update,
                                   Alert_message,
                                   Vehicle_message
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
