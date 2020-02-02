from pkgutil import get_data


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
        transit_times
    '''
    sqltext = get_data(
        'mtatracking_v2', 'sql_queries/featurematrix_transitTime.sql')
    transit_times_matrix = session.execute(
        sqltext.decode("utf-8").format(origin_id, dest_id, line_id, direction,
                                       time_start, time_end)
        ).fetchall()
    return transit_times_matrix
