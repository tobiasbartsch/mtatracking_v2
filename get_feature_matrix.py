from pkgutil import get_data
import pandas as pd


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
    labels = features['transit_time']
    features = features.drop(labels=['transit_time', 'arrival_time'], axis=1)

    return features, labels
