import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from importlib.resources import path

import resources
from models import Base, Stop

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
        session.add(thisstop)
    session.commit()


populateStationsTable()
