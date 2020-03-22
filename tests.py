import sys
sys.path.append('/home/tbartsch/source/repos')

from mtatracking_v2.subway_system_analyzer import historicTrainDelays
from mtatracking_v2.subway_system_analyzer import getStationObjectsAlongLine_ordered, getStationIDsAlongLine_static
from mtatracking_v2.mean_transit_times import getTransitTimes, computeMeanTransitTimes
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime


def test_getStationsAlongLine_ordered():
    engine = create_engine(
        'postgresql://tbartsch:test@localhost/mtatrackingv2_dev')

    Session = sessionmaker(bind=engine)
    session = Session()
    getStationObjectsAlongLine_ordered(
        '2', 'N', datetime(2019, 7, 8), datetime(2019, 7, 10), session)


def test_historicTrainDelays():
    engine = create_engine(
        'postgresql://tbartsch:test@localhost/mtatrackingv2')

    Session = sessionmaker(bind=engine)
    session = Session()

    htd =  historicTrainDelays('Q', 'N', datetime(2019, 5, 1), datetime(2019, 5, 31), session)
    htd.checkAllTrainsInLine(n=8)


def test_removeShortStates():
    engine = create_engine(
        'postgresql://tbartsch:test@localhost/mtatrackingv2_dev')

    Session = sessionmaker(bind=engine)
    session = Session()

    tt = getTransitTimes('D43N', 'D42N', 'Q',
                    datetime(2020, 1, 1), datetime(2021, 1, 1), session)
    r, s = computeMeanTransitTimes(tt)

def test_getStationIDsAlongLine_static():
    engine = create_engine(
    'postgresql://tbartsch:test@localhost/mtatrackingv2_dev')

    Session = sessionmaker(bind=engine)
    session = Session()

    getStationIDsAlongLine_static(
                        'Q', 'N',
                        filename_static_trips='stop_times.txt')

test_getStationIDsAlongLine_static()
