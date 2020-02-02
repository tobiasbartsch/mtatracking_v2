from pkgutil import get_data
from mtatracking_v2.STaSI import w1, fitSTaSIModel, sdevFromW1
import numpy as np
from mtatracking_v2.models import Transit_time_fit
from datetime import timedelta


def getTransitTimes(origin_id, dest_id, line_id,
                    time_start, time_end, session):
    '''Get transit times between two stations along a subway line.
    Executes as much as possible in SQL to improve performance.

    Args:
        origin_id (string): id of the origin station
        dest_id (string): id of the destination station
        line_id (string): id of the subway line, for example 'Q'
        time_start (datetime): timepoint in the historic data at which to
                               start the computation
        time_end (datetime): timepoint in the historic data at which to
                               stop the computation
        session: the SQLAlchemy database session.

    Returns:
        transit_times
    '''
    if(time_end - time_start < timedelta(days=30)):
        print('warning: time should be at least one month')
    sqltext = get_data('mtatracking_v2', 'sql_queries/transit_times.sql')
    transit_times = session.execute(
        sqltext.decode("utf-8").format(origin_id, dest_id, line_id,
                                       time_start, time_end)
        ).fetchall()
    return transit_times


def getTransitTimes_and_delay(origin_id, dest_id, line_id,
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
        transit_times, delays
    '''
    if(time_end - time_start < timedelta(days=30)):
        print('warning: time should be at least one month')
    sqltext = get_data('mtatracking_v2',
                       'sql_queries/transit_times_and_delay.sql')
    transit_times = session.execute(
        sqltext.decode("utf-8").format(origin_id, dest_id, line_id,
                                       time_start, time_end)
        ).fetchall()
    return transit_times


def _removeShortStates(results, dt=timedelta(hours=4)):
    '''remove segments that are shorter than dt'''
    if len(results) == 1:
        return results

    results['length'] =\
        results['seg_end_datetime'] - results['seg_start_datetime']

    for i, row in results.iterrows():
        if row['length'] < dt:
            # found a row we want to delete. Should we append this state
            # to the row before or after?
            if i == 0:
                # have to use row after
                results.loc[i+1, 'seg_start_datetime'] =\
                    row['seg_start_datetime']
                # set the vals of the current row to be those of row after
                results.loc[i, :] = results.loc[i+1, :]
                results.loc[i, 'length'] = timedelta(seconds=0)
                continue
            if i == len(results)-1:
                # have to use row before
                results.loc[i-1, 'seg_end_datetime'] =\
                    row['seg_end_datetime']
                break
            if np.abs(row['median']-results.loc[i-1, 'median']) >\
                    np.abs(row['median']-results.loc[i+1, 'median']):
                # row after
                results.loc[i+1, 'seg_start_datetime'] =\
                    row['seg_start_datetime']
                # set the vals of the current row to be those of row after
                results.loc[i, :] = results.loc[i+1, :]
                results.loc[i, 'length'] = timedelta(seconds=0)
            else:
                # row before
                results.loc[i-1, 'seg_end_datetime'] =\
                    row['seg_end_datetime']
                # set the vals of the current row to be those of row before
                results.loc[i, :] = results.loc[i-1, :]
                results.loc[i, 'length'] = timedelta(seconds=0)

    results = results[results['length'] > dt]
    results = results.drop('length', axis=1)
    return results


def computeMeanTransitTimes(transit_times):
    '''Fit transit time data with the STaSI algorithm.

    Args:
        transit_times: list of tuples (datetime, timedelta) describing
                       transit time vs datetime

    Returns:
        (result (pandas.df), sdev):

            result: dataframe of results, containing
            mean transit times, sdevs, and their
            start and stop datetimes,

            sdev: standard deviation of entire (non-segmentized) trace
    '''
    transit_times = np.array(transit_times)
    if len(transit_times) < 2:
        return None, None
    to_seconds_vectorized = np.vectorize(lambda x: x.total_seconds())
    transit_times_s = transit_times.copy()
    transit_times_s[:, 2] = to_seconds_vectorized(transit_times[:, 2])

    # Remove negative times:
    transit_times_filtered = transit_times_s[
        transit_times_s[:, 2] > 0]
    if len(transit_times_filtered) < 2:
        return None, None

    # remove outliers 40 sigma beyond mean.
    w1s = w1(transit_times_filtered[:, 2])
    sigma = sdevFromW1(w1s)
    if sigma == 0:
        return None, None

    transit_times_filtered = transit_times_filtered[(
        np.abs((transit_times_filtered[:, 2]) - np.median(
            (transit_times_filtered[:, 2]))) < 40 * sigma)
            ]
    # if the time series is zero, return None
    if len(transit_times_filtered) == 0:
        return None, None

    fit, means, sdevs, results, MDLs = fitSTaSIModel(
        transit_times_filtered[:, 2])
    if results is None:
        return None, None
    # currently the results dataframe contains indices;
    # make those into time stamps.
    start_stamps = np.asarray(
        transit_times_filtered[:, 1])[results['start'].values]
    stop_stamps = np.asarray(
        transit_times_filtered[:, 1])[results['stop'].values]
    results['seg_start_datetime'] = start_stamps
    results['seg_end_datetime'] = stop_stamps
    results = results.drop('start', axis=1)
    results = results.drop('stop', axis=1)

    results = _removeShortStates(results)

    return results, sigma


def populate_database_with_fit_results(session, results, sdev, origin_id,
                                       destination_id, line_id, direction,
                                       fit_start_datetime, fit_end_datetime):
    '''Save the results of our MeanTransitTimes fit in the database

    Args:
        session: sqlalchemy session to our database
        results (pd.DataFrame): pandas dataframe of fit results
        sdev (float or integer): standard deviation
                    (in seconds, of entire trace) used for fit.
        origin_id: ID of the origin station
        destination_id: ID of the destination station
        line_id: ID of the subway line (e.g. '2')
        direction: north ('N') or south ('S')
        fit_start_datetime: first time point in the fitted data
        fit_end_datetime: last time point in the fitted data
    '''
    newfit = Transit_time_fit(
        origin_id, destination_id, line_id, direction, sdev,
        fit_start_datetime, fit_end_datetime
    )
    session.add(newfit)
    session.commit()
    results['fit_id'] = newfit.id
    results.to_sql('Mean_transit_time', session.connection(),
                   if_exists='append', index=False)
    session.commit()
    return newfit
