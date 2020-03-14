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
                         line_ids_features, directions_features,
                         time_start, time_end, linedef_hour,
                         linedef_day, session):
    '''Get transit time feature matrix for transit between two stations
    along a subway line.
    Executes as much as possible in SQL to improve performance.

    Args:
        origin_id (string): id of the origin station
        dest_id (string): id of the destination station
        line_id (string): id of the subway line, for example 'Q'
        direction (string): direction. We currently only support 'N' or 'S'.
                            This could change if the MTA changes their mind.
        line_ids_features (list): list of line_ids indicating the lines to
                                  include as features in the matrix.
                                  (e.g. ['Q', '4', 'A'])
                                  NOTE: You will probably want to include your
                                  origin -> destination line in this list.
        directions_features (list): list of directions to include as features.
                                    (e.g. ['N'] or ['N', 'S'])
        time_start (datetime): timepoint in the historic data at which to
                               start the computation
        time_end (datetime): timepoint in the historic data at which to
                               stop the computation
        linedef_hour (integer): Unfortunately the line definitions change
                                during the day.
                                Specify an hour of day for which you want to
                                retrieve them.
        linedef_day (String): One of 'Weekday', 'Saturday', or 'Sunday'.
        session: the SQLAlchemy database session.

    Returns:
        features, labels: dataframe of features. labels are the observed
                          transit times
    '''

    feature_dfs = {}
    train_uniques_list = []
    labels = None
    for f_line_id in line_ids_features:
        for f_direction in directions_features:
            print('working on: ' + f_line_id + f_direction)
            sqltext = get_data(
                'mtatracking_v2', 'sql_queries/featurematrix_transitTime.sql')
            transit_times_matrix = session.execute(
                sqltext.decode("utf-8").format(
                    origin_id,
                    dest_id,
                    line_id,
                    direction,
                    time_start,
                    time_end,
                    f_line_id,
                    f_direction,
                    linedef_hour,
                    linedef_day
                    )
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
            features = features.drop(
                labels=['transit_time', 'arrival_time'], axis=1)
            features['weekday'] = (features['origin_time'].dt.weekday < 5)
            features['hour'] = features['origin_time'].dt.hour
            train_uniques = features['train_unique_num']
            train_uniques_list.append(train_uniques)
            features = features.drop(labels=['origin_time',
                                             'train_unique_num'], axis=1)
            cat = f_line_id + f_direction
            feature_dfs[cat] = features

    for cat, df in feature_dfs.items():
        new_cols = [cat + '_' + col for col in df.columns]
        df.columns = new_cols

    features = pd.concat(list(feature_dfs.values()), axis=1)

    return features, labels, train_uniques_list


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


def get_MTA_predicted_transit_time(
        train_unique_num, origin_id, dest_id, session):

    sqltext = get_data(
        'mtatracking_v2', 'sql_queries/mta_predicted_transit_time.sql')
    transit_times_matrix = session.execute(
        sqltext.decode("utf-8").format(
            train_unique_num,
            origin_id,
            dest_id
            )
        )
    df = pd.DataFrame(transit_times_matrix.fetchall(),
                      columns=transit_times_matrix.keys())
    return df


def get_current_features_NOW(origin_id, dest_id, line_id, direction,
                             line_ids_features, directions_features,
                             linedef_hour,
                             linedef_day, session):
    feature_dfs = {}
    for f_line_id in line_ids_features:
        for f_direction in directions_features:
            print('working on: ' + f_line_id + f_direction)
            print(linedef_day)
            print(linedef_hour)
            sqltext = get_data(
                'mtatracking_v2', 'sql_queries/features_now.sql')
            transit_times_matrix = session.execute(
                sqltext.decode("utf-8").format(
                    f_line_id,
                    f_direction,
                    linedef_hour,
                    linedef_day
                    )
                )
            df = pd.DataFrame(transit_times_matrix.fetchall(),
                              columns=transit_times_matrix.keys())
            df.set_index(['origin_time',
                          'stop_id'], inplace=True)

            features = df.unstack(level=-1)
            features.columns = ['_'.join(col).strip() for
                                col in features.columns.values]
            features = features.reset_index()
            features = features.dropna()
            for column in features:
                if features[column].dtype == '<m8[ns]':
                    features[column] = features[column].dt.total_seconds()
            if features.empty is False:
                print(features['origin_time'])
                features['weekday'] = (features['origin_time'].dt.weekday < 5)
                features['hour'] = features['origin_time'].dt.hour
                features = features.drop(labels=['origin_time'], axis=1)
            else:
                features['weekday'] = None
                features['hour'] = None
            cat = f_line_id + f_direction
            feature_dfs[cat] = features

    for cat, df in feature_dfs.items():
        new_cols = [cat + '_' + col for col in df.columns]
        df.columns = new_cols

    features = pd.concat(list(feature_dfs.values()), axis=1)
    # add the last feature: the transit time the MTA predicts
    sqltext = get_data('mtatracking_v2', 'sql_queries/MTApredictionNow.sql')
    print('get last feature')
    transit_times_matrix = session.execute(
                        sqltext.decode("utf-8").format(
                            line_id,
                            direction,
                            origin_id,
                            dest_id
                        )
    )
    df = pd.DataFrame(transit_times_matrix.fetchall(),
                      columns=transit_times_matrix.keys())
    features['mta_prediction'] = df['transit_time'].dt.total_seconds()

    return features
