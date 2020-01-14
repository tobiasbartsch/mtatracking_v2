# homemade modules
import sys
sys.path.append('/home/tbartsch/source/repos')
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from mtatracking_v2.models import Base, Stop

engine = create_engine('postgresql://tbartsch:test@localhost/mtatrackingv2')

Base.metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


# Populate the stations table
def populateStationsTable():
    df = pd.read_csv('resources/stops.txt')
    df = df.where(df.notnull(), None)
    for index, s in df.iterrows():
        thisstop = Stop(s['stop_id'], s['stop_name'], stop_code=s['stop_code'],
                        desc=s['stop_desc'], stop_lat=s['stop_lat'],
                        stop_lon=s['stop_lon'], zone_id=s['zone_id'],
                        stop_url=s['stop_url'],
                        location_type=s['location_type'],
                        parent_station=s['parent_station'])
        session.merge(thisstop)
    # Make a catchall object for completely Unknown stations:

    thisstop = Stop('Unknown', 'Unknown')
    session.merge(thisstop)
    session.commit()


populateStationsTable()
