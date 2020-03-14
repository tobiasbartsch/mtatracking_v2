import sys
sys.path.append('/home/tbartsch/source/repos')

from collections import defaultdict
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mtatracking_v2.models import (
    Transit_time_fit,
    Line,
    Line_stops
)
from mtatracking_v2.subway_system_analyzer import (
    getLatestSavedLinedefinition, StationCodesToNames)


def initDB():
    engine = create_engine(
        'postgresql://tbartsch:test@localhost/mtatrackingv2_dev')
    Session = sessionmaker(bind=engine)
    session = Session()

    return session


def getTransitTimeFits(session):
    fits = session.query(Transit_time_fit).all()
    return fits


# get initial data
def find_best_median(k, fit, best_transit_times_line):
    line_id = fit.line_id + ' ' + fit.direction
    best_median = None
    best_date = None
    for median in fit.medians:
        if best_median is None or median.median < best_median:
            best_median = median.median
            best_date = median.seg_end_datetime
    if best_median is not None:
        if line_id not in best_transit_times_line:
            best_transit_times_line[line_id][k] = (best_median, best_date)
        else:
            if k not in best_transit_times_line[line_id]:
                best_transit_times_line[line_id][k] = (best_median, best_date)
            else:
                if best_transit_times_line[line_id][k][0] > best_median:
                    best_transit_times_line[line_id][k]\
                        = (best_median, best_date)
                elif best_transit_times_line[line_id][k][0]\
                    == best_median and\
                        best_transit_times_line[line_id][k][1] < best_date:
                    best_transit_times_line[line_id][k]\
                        = (best_median, best_date)
    return best_transit_times_line


def find_latest_median(k, fit, latest_transit_times_line):
    line_id = fit.line_id + ' ' + fit.direction
    latest_median = None
    latest_date = None
    for median in fit.medians:
        if latest_median is None or median.seg_end_datetime > latest_date:
            latest_median = median.median
            latest_date = median.seg_end_datetime
    if latest_median is not None:
        if line_id not in latest_transit_times_line:
            latest_transit_times_line[line_id][k]\
                = (latest_median, latest_date)
        else:
            if k not in latest_transit_times_line[line_id]:
                latest_transit_times_line[line_id][k]\
                    = (latest_median, latest_date)
            else:
                if latest_transit_times_line[line_id][k][1] > latest_date:
                    latest_transit_times_line[line_id][k]\
                        = (latest_median, latest_date)
    return latest_transit_times_line


def makeBestAndLatestTransitTimesDicts(fits):
    best_transit_times_line = defaultdict(dict)
    latest_transit_times_line = defaultdict(dict)
    for fit in fits:
        origin = fit.stop_id_origin
        destination = fit.stop_id_destination
        k = origin + ' ' + destination
        # find best median in this fit
        best_transit_times_line = find_best_median(
            k, fit, best_transit_times_line)
        latest_transit_times_line = find_latest_median(
            k, fit, latest_transit_times_line)

    return best_transit_times_line, latest_transit_times_line


def getData(session, day='Weekday', hour=8):
    fits = getTransitTimeFits(session)
    best_transit_times_line, latest_transit_times_line\
        = makeBestAndLatestTransitTimesDicts(fits)

    linedefs = {
        k: getLatestSavedLinedefinition(k.split()[0], k.split()[1],
                                        day, hour, session)
        for k in best_transit_times_line.keys()}

    station_names = {
        k: StationCodesToNames(linedef, session)
        for k, linedef in linedefs.items()}

    return (linedefs,
            latest_transit_times_line,
            best_transit_times_line,
            station_names)
