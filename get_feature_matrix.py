from pkgutil import get_data
import pandas as pd
from mtatracking_v2.subway_system_analyzer import (
    historicTrainDelays,
    getLatestSavedLinedefinition)
from mtatracking_v2.models import (Line,
                                   Line_stops,
                                   Stop)
import numpy as np


def getTransitTimeMatrix(origin_id, dest_id, line_id, direction,
                         time_start, time_end, session):
    '''Get transit time feature matrix for transit between two stations
    along a subway line.
    Executes as much as possible in SQL to improve performance.

    Args:
        origin_id (string): id of the origin station
        dest_id (string): id of the destination station
        line_id (string): id of the subway line, for example 'Q'
        direction (string): direction. We currently only support 'N' or 'S'.
                            This could change if the MTA changes their mind.
        time_start (datetime): timepoint in the historic data at which to
                               start the computation
        time_end (datetime): timepoint in the historic data at which to
                               stop the computation
        session: the SQLAlchemy database session.

    Returns:
        features, labels: dataframe of features. labels are the observed
                          transit times
    '''

    sqltext = get_data(
        'mtatracking_v2', 'sql_queries/featurematrix_transitTime.sql')
    transit_times_matrix = session.execute(
        sqltext.decode("utf-8").format(origin_id, dest_id, line_id, direction,
                                       time_start, time_end)
        )
    df = pd.DataFrame(transit_times_matrix.fetchall(),
                      columns=transit_times_matrix.keys())
    df.set_index(['origin_time',
                  'train_unique_num',
                  'transit_time',
                  'arrival_time',
                  'stop_id'], inplace=True)

    features = df.unstack(level=-1)
    features.columns = ['_'.join(col).strip() for
                        col in features.columns.values]
    features = features.reset_index()
    features = features.dropna()
    for column in features:
        if features[column].dtype == '<m8[ns]':
            features[column] = features[column].dt.total_seconds()
    labels = features['transit_time']
    features = features.drop(labels=['transit_time', 'arrival_time'], axis=1)
    features['weekday'] = (features['origin_time'].dt.weekday < 5)
    features['hour'] = features['origin_time'].dt.hour
    features = features.drop(labels=['origin_time',
                                     'train_unique_num'], axis=1)
    return features, labels


def getMedianTransitTimeMatrix(line_id, direction, time_start,
                               time_end, timestamps, session):
    '''get the median transit times (from stasi fits)
    between each pair of stations in line_id and direction
    for each timestamp in 'timestamps'

    Args:
        line_id (string): id of the subway line, for example 'Q'
        direction (string): direction. We currently only support 'N' or 'S'.
                            This could change if the MTA changes their mind.
        time_start (datetime): timepoint in the historic data at which to
                               start the computation
        time_end (datetime): timepoint in the historic data at which to
                               stop the computation
        timestamps (list of datetime): timepoints for which to find median
                                       transit times. The feature matrix will
                                       have one row for each of these.
        session: the SQLAlchemy database session.


    Returns: features
    '''
    htd = historicTrainDelays(line_id, direction,
                              time_start, time_end, session)
    station_ids = getLatestSavedLinedefinition(line_id, direction, session)
    stations = session.query(Stop).filter(Stop.id.in_(station_ids)).all()

    median_travel_times = {}
    for orig, dest in zip(stations[:-1], stations[1:]):
        median_travel_time = []
        for timestamp in timestamps:
            mtt, _ = htd.getMedianTravelTime(
                    orig, dest, timestamp)
            median_travel_time.append(mtt)
        median_travel_times[orig.id + '_' + dest.id] =\
            median_travel_time
    median_travel_times['timestamps'] = timestamps
    features = pd.DataFrame(median_travel_times)
    return features
