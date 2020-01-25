from pkgutil import get_data
from mtatracking_v2.STaSI import w1, fitSTaSIModel, sdevFromW1
import numpy as np
from mtatracking_v2.models import Transit_time_fit


def getTransitTimes(origin_id, dest_id, line_id,
                    time_start, time_end, session):
    '''Get transit times between two stations along a subway line.
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
    '''

    sqltext = get_data('mtatracking_v2', 'sql_queries/transit_times.sql')
    transit_times = session.execute(
        sqltext.decode("utf-8").format(origin_id, dest_id, line_id,
                                       time_start, time_end)
        ).fetchall()
    return transit_times


def computeMeanTransitTimes(transit_times):
    '''Fit transit time data with the STaSI algorithm.

    Args:
        transit_times: list of tuples (datetime, timedelta) describing
                       transit time vs datetime

    Returns:
        result (pandas.df): dataframe of results, containing
                            mean transit times, sdevs, and their
                            start and stop datetimes.
    '''
    transit_times = np.array(transit_times)
    to_seconds_vectorized = np.vectorize(lambda x: x.total_seconds())
    transit_times_s = transit_times.copy()
    transit_times_s[:, 1] = to_seconds_vectorized(transit_times[:, 1])
    # remove outliers 10 sigma beyond mean. Remove negative times:
    w1s = w1(transit_times_s[:, 1])
    sigma = sdevFromW1(w1s)

    transit_times_filtered = transit_times_s[(
        np.abs((transit_times_s[:, 1]) - np.median(
            (transit_times_s[:, 1]))) < 10 * sigma)
        & (transit_times_s[:, 1] > 0)]
    # if the time series is zero, return None
    if len(transit_times_filtered) == 0:
        return None

    fit, means, results, MDLs = fitSTaSIModel(transit_times_filtered[:, 1])

    # currently the results dataframe contains indices;
    # make those into time stamps.
    start_stamps = np.asarray(transit_times_filtered[:, 0])[results['start'].values]
    stop_stamps = np.asarray(transit_times_filtered[:, 0])[results['stop'].values]
    results['seg_start_datetime'] = start_stamps
    results['seg_end_datetime'] = stop_stamps
    results = results.drop('start', axis=1)
    results = results.drop('stop', axis=1)

    return results


def populate_database_with_fit_results(session, results, origin_id,
                                       destination_id, line_id, direction,
                                       fit_start_datetime, fit_end_datetime):
    '''Save the results of our MeanTransitTimes fit in the database

    Args:
        session: sqlalchemy session to our database
        results (pd.DataFrame): pandas dataframe of fit results
        origin_id: ID of the origin station
        destination_id: ID of the destination station
        line_id: ID of the subway line (e.g. '2')
        direction: north ('N') or south ('S')
        fit_start_datetime: first time point in the fitted data
        fit_end_datetime: last time point in the fitted data
    '''
    newfit = Transit_time_fit(
        origin_id, destination_id, line_id, direction,
        fit_start_datetime, fit_end_datetime
    )
    session.add(newfit)
    session.commit()
    results['fit_id'] = newfit.id
    results.to_sql('Mean_transit_time', session.connection(),
                   if_exists='append', index=False)
    session.commit()
