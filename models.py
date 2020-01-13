import numpy as np
import datetime
from datetime import datetime as ddatetime
from datetime import timedelta

import mtatracking_v2.nyct_subway_pb2 as nyct_subway_pb2

from pytz import timezone

from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.types import DateTime, Boolean, Time, Date, Float
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Train(Base):
    __tablename__ = 'Train'

    # unique_num is start_date + train_id
    unique_num = Column(String, primary_key=True)
    route_id = Column(String, nullable=False)
    is_assigned = Column(Boolean, nullable=True)
    first_seen_timestamp = Column(DateTime, nullable=False)
    is_in_system_now = Column(Boolean, nullable=False)

    def __init__(self, unique_num, route_id,
                 first_seen_timestamp, is_in_system_now,
                 is_assigned=None):
        self.unique_num = unique_num
        self.route_id = route_id
        self.is_assigned = is_assigned
        self.first_seen_timestamp = first_seen_timestamp
        self.is_in_system_now = is_in_system_now

    def __repr__(self):
        return self.route_id + self.direction + "_" + self.unique_num


class Trip_update(Base):
    __tablename__ = 'Trip_update'

    id = Column(Integer, primary_key=True)
    trip_id = Column(String, nullable=False)
    train_unique_num = Column(String, ForeignKey('Train.unique_num'),
                              nullable=False)
    origin_date = Column(Date, nullable=False)
    origin_time = Column(Time, nullable=False)
    line_id = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    effective_timestamp = Column(DateTime, nullable=False)
    path = Column(String, nullable=True)

    def __init__(self, trip_id, train_unique_num, origin_date,
                 origin_time, line_id,
                 direction, effective_timestamp, path=None):
        self.trip_id = trip_id
        self.train_unique_num = train_unique_num
        self.origin_date = origin_date
        self.origin_time = origin_time
        self.line_id = line_id
        self.direction = direction
        self.effective_timestamp = effective_timestamp
        self.path = path


class Stop_time_update(Base):
    __tablename__ = 'Stop_time_update'

    id = Column(Integer, primary_key=True)
    trip_update_id = Column(Integer,
                            ForeignKey('Trip_update.id'),
                            nullable=False)
    stop_id = Column(String, ForeignKey('Stop.id'),
                     nullable=False)
    arrival_time = Column(DateTime, nullable=True)
    departure_time = Column(DateTime, nullable=True)
    scheduled_track = Column(String, nullable=True)
    actual_track = Column(String, nullable=True)

    def __init__(self, trip_update_id, stop_id, arrival_time=None,
                 departure_time=None, scheduled_track=None,
                 actual_track=None):
        self.trip_update_id = trip_update_id
        self.stop_id = stop_id
        self.arrival_time = arrival_time
        self.departure_time = departure_time
        self.scheduled_track = scheduled_track
        self.actual_track = actual_track


class Stop(Base):
    __tablename__ = 'Stop'

    id = Column(String, primary_key=True)
    stop_code = Column(String, nullable=True)
    name = Column(String, nullable=False)
    desc = Column(String, nullable=True)
    stop_lat = Column(Float, nullable=True)
    stop_lon = Column(Float, nullable=True)
    zone_id = Column(String, nullable=True)
    stop_url = Column(String, nullable=True)
    location_type = Column(Integer, nullable=False)
    parent_station = Column(String, nullable=True)

    def __init__(self, stop_id, name, stop_code=None,
                 desc=None, stop_lat=None, stop_lon=None,
                 zone_id=None, stop_url=None,
                 location_type=0, parent_station=None):
        self.id = stop_id
        self.stop_code = stop_code
        self.name = name
        self.desc = desc
        self.stop_lat = stop_lat
        self.stop_lon = stop_lon
        self.zone_id = zone_id
        self.stop_url = stop_url
        self.location_type = location_type
        self.parent_station = parent_station

    def __repr__(self):
        return self.name + ' (' + self.id + ')'


class Trains_stopped(Base):
    '''junction table. Which trains stopped at what stops?'''
    __tablename__ = 'Trains_stopped'

    id = Column(Integer, primary_key=True)
    stop = Column(String, ForeignKey('Stop.id'),
                  nullable=False)
    train_id = Column(String, ForeignKey('Train.unique_num'), nullable=False)
    stop_time = Column(DateTime, nullable=False)

    def __init__(self, stop_id, train_id, stop_time):
        self.stop = stop_id
        self.train_id = train_id
        self.stop_time = stop_time


class SubwaySystem:
    """A subway system consists of stations, lines, and trains.
    These objects are stored in a database and accessed by SQLAlchemy."""

    def __init__(self, session):
        '''Create a SubwaySystem
        Args:
            session: SQLAlchemy session bound to database.
        '''

        self.session = session
        # we need to make sure we do not have unreasonably
        # long gaps in between files during tracking:
        self.last_attached_file_timestamp = np.nan

        # keep the Stops table in memory so that we can check whether
        # a stop is in the database without performing a query:
        self.stop_ids = [s.id for s in session.query(Stop).all()]

    def attach_tracking_data(self, data):
        """Process the protocol buffer feed and populate our
        subway model with its data.

        Args:
            data: List of protocol buffer messages containing
                  trip_update, vehicle, or alert feed entities
                  (presumably downloaded from the MTA realtime stream).
                  One message per requested feed.
        """
        # get the trains that are currently in the system:

        for message in data:
            current_time = message.header.timestamp
            for FeedEntity in message.entity:
                if len(FeedEntity.trip_update.trip.trip_id) > 0:
                    # entity type "trip_update"
                    self._processTripUpdate(FeedEntity, current_time)
                if len(FeedEntity.vehicle.trip.trip_id) > 0:
                    # entity type "vehicle"
                    # self._processVehicleMessage(FeedEntity, current_time)
                    pass
                if len(FeedEntity.alert.header_text.translation) > 0:
                    # alert message
                    # self._processAlertMessage(FeedEntity, current_time)
                    pass
        self.session.commit()

    def _processTripUpdate(self, FeedEntity, current_time):
        """Add data contained in the Protobuffer's Trip Update FeedEntity
        to the subway system.

        Args:
            FeedEntity: TripUpdate FeedEntity (from protobuffer).
            current_time (timestamp): Timestamp in seconds since 1970
        """
        # make DateTime object from current_time
        current_time_dt = ddatetime.fromtimestamp(current_time)
        current_time_dt = timezone('US/Eastern').localize(current_time_dt)

        # Get the trains currently in the subway system:
        # current_trains = self.session.query(Train).filter_by(is_in_system_now=True)

        # Add current train to database
        train_id = FeedEntity.trip_update.trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor].train_id
        origin_date = FeedEntity.trip_update.trip.start_date
        unique_num = origin_date + ": " + train_id
        origin_date = datetime.datetime.strptime(origin_date, "%Y%m%d").date()
        route_id = FeedEntity.trip_update.trip.route_id
        is_assigned = FeedEntity.trip_update.trip.Extensions[nyct_subway_pb2.nyct_trip_descriptor].is_assigned

        print('working on train: ' + unique_num)
        this_train = Train(unique_num=unique_num, route_id=route_id,
                           first_seen_timestamp=current_time_dt,
                           is_in_system_now=True,
                           is_assigned=is_assigned)

        self.session.merge(this_train)
        self.session.flush()

        # Add current trip to database
        trip_id = FeedEntity.trip_update.trip.trip_id
        origin_time, line, direction, path_id = self.parse_trip_id(trip_id)
        # origin time to Time object:
        origin_time = (datetime.datetime.min +
                       timedelta(minutes=origin_time)).time()
        route_id = FeedEntity.trip_update.trip.route_id
        direction = self.direction_to_str(FeedEntity.trip_update.trip.
                                          Extensions[nyct_subway_pb2.
                                                     nyct_trip_descriptor].
                                          direction)
        this_trip = Trip_update(trip_id=trip_id, train_unique_num=unique_num,
                                origin_date=origin_date,
                                origin_time=origin_time,
                                line_id=route_id,
                                direction=direction,
                                effective_timestamp=current_time_dt,
                                path=path_id)

        self.session.add(this_trip)

        # need to call this auto-populate the Trip_update id:
        self.session.flush()

        # Add stop time updates to database

        for stu in FeedEntity.trip_update.stop_time_update:
            stop_id = stu.stop_id
            # check whether this stop is in our table of stops.
            # If it isn't, add it.
            if stop_id not in self.stop_ids:
                stop = Stop(stop_id, name='Unknown')
                self.session.add(stop)
                self.session.flush()
                self.stop_ids.append(stop_id)

            arrival_time = stu.arrival.time
            arrival_time_dt = ddatetime.fromtimestamp(arrival_time)
            arrival_time_dt = timezone('US/Eastern').localize(arrival_time_dt)

            departure_time = stu.departure.time
            departure_time_dt = ddatetime.fromtimestamp(departure_time)
            departure_time_dt = timezone('US/Eastern').localize(departure_time_dt)

            scheduled_track = stu.Extensions[nyct_subway_pb2.\
                                             nyct_stop_time_update].scheduled_track
            actual_track = stu.Extensions[nyct_subway_pb2.\
                                          nyct_stop_time_update].actual_track
            this_stu = Stop_time_update(trip_update_id=this_trip.id,
                                        stop_id=stop_id,
                                        arrival_time=arrival_time_dt,
                                        departure_time=departure_time_dt,
                                        scheduled_track=scheduled_track,
                                        actual_track=actual_track)
            self.session.add(this_stu)

    def direction_to_str(self, direction):
        """convert a direction number (1, 2, 3, 4) to a string (N, E, S, W)
        """
        dirs = ['N', 'E', 'S', 'W']
        return dirs[direction-1]

    def parse_trip_id(self, trip_id):
        """Decode the trip id and find trip origin time, line number,
        and direction

        Returns:
            Tuple of (origin time, line, direction, path id)
        """
        # origin time of the trip in seconds past midnight.
        origin_time = int(trip_id.split('_')[0])/100
        trip_path = trip_id.split('_')[1]
        line = trip_path.split('.')[0]
        path_id = trip_path.split('.')[-1]
        direction = path_id[0]
        path_id = path_id[1:]
        return (origin_time, line, direction, path_id)
