import sys
sys.path.append('/home/tbartsch/source/repos')

from mtatracking_v2.subway_system_analyzer import getStationsAlongLine_ordered
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from datetime import datetime


def test_getStationsAlongLine_ordered():
    engine = create_engine(
        'postgresql://tbartsch:test@localhost/mtatrackingv2')

    Session = sessionmaker(bind=engine)
    session = Session()
    getStationsAlongLine_ordered(
        '2', 'N', datetime(2019, 7, 8), datetime(2019, 7, 10), session)


test_getStationsAlongLine_ordered()
