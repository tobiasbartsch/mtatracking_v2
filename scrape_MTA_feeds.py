import sys
# sys.path.append('/home/tbartsch/source/repos')
import time
from urllib.request import Request
import urllib.request
import gtfs_realtime_pb2 as gtfs_realtime_pb2
from SubwaySystem import SubwaySystem
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def makeSubSys():
    print("enter database name: ")
    dbname = sys.stdin.readline()
    engine = create_engine(f'postgresql://tbartsch:test@localhost/{dbname}')

    Session = sessionmaker(bind=engine)
    session = Session()
    session_fit_update = Session()

    subsys = SubwaySystem(session, session_fit_update)
    return subsys


def TrackTrains(key, feed_ids):
    """Query the locations and status of all trains of a specific set of lines.

    Args:
        feed_ids (list of int): IDs of the set of subway lines to track.
        For example, feed_ids=['gtfs-nqrw'] are the NQRW trains.

    Returns: List of gtfs_realtime_pb2.FeedMessage of tracked feeds.
    """

    data = None
    messagelist = []
    while data is None:
        for id in feed_ids:
            url = 'https://api-endpoint.mta.info/'\
                'Dataservice/mtagtfsfeeds/nyct%2F' + str(id)
            req = Request(url, None, {"x-api-key": str(key)})
            try:
                with urllib.request.urlopen(req) as response:
                    print(id)
                    data = response.read()
                    feed_message = gtfs_realtime_pb2.FeedMessage()
                    feed_message.ParseFromString(data)
                    messagelist.append(feed_message)
            except Exception as e:
                print(e)
                time.sleep(5)
                continue
    return messagelist


def TrackAllAndAttachForever(key, dt=20):
    feed_ids = ['gtfs-ace', 'gtfs-bdfm', 'gtfs-g', 'gtfs-jz',
                'gtfs-nqrw', 'gtfs-l', 'gtfs', 'gtfs-7', 'gtfs-si']

    subwaysys = makeSubSys()

    while True:
        message_list = TrackTrains(key, feed_ids)
        subwaysys.attach_tracking_data(message_list)
        time.sleep(dt)


if __name__ == "__main__":
    key = input("Enter your MTA realtime access key: ")
    TrackAllAndAttachForever(key)
