from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.types import DateTime, Boolean, Time, Date, Float
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Train(Base):
    __tablename__ = 'Train'

    id = Column(Integer, primary_key=True)
    unique_num = Column(String, nullable=False)
    route_id = Column(String, nullable=False)
    is_assigned = Column(Boolean, nullable=True)
    first_seen_timestamp = Column(DateTime, nullable=False)
    is_in_system_now = Column(Boolean, nullable=False)
    __table_args__ = (UniqueConstraint('unique_num'),)

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


class Trip_id(Base):
    __tablename__ = 'Trip_id'

    id = Column(Integer, primary_key=True)
    train_id = Column(Integer, ForeignKey('Train.id'), nullable=False)
    origin_date = Column(Date, nullable=False)
    origin_time = Column(Time, nullable=False)
    line_id = Column(Integer, ForeignKey('Line.id'), nullable=False)
    direction = Column(String, nullable=False)
    effective_timestamp = Column(DateTime, nullable=False)
    path = Column(String, nullable=True)

    def __init__(self, train_id, origin_date, origin_time, line_id,
                 direction, effective_timestamp, path=None):
        self.train_id = train_id
        self.origin_date = origin_date
        self.origin_time = origin_time
        self.line_id = line_id
        self.direction = direction
        self.effective_timestamp = effective_timestamp
        self.path = path


# TODO consider whether this is strictly necessary. could we not
# just stick these attributes into the stop_time_update table?
class Remaining_stops(Base):
    __tablename__ = 'Remaining_stops'

    id = Column(Integer, primary_key=True)
    train_id = Column(Integer, ForeignKey('Train.id'), nullable=False)
    effective_timestamp = Column(DateTime, nullable=False)

    def __init__(self, train_id, effective_timestamp):
        self.train_id = train_id
        self.effective_timestamp = effective_timestamp


class Stop_time_update(Base):
    __tablename__ = 'Stop_time_update'

    id = Column(Integer, primary_key=True)
    remaining_stops_id = Column(Integer,
                                ForeignKey('Remaining_stops.id'),
                                nullable=False)
    stop_id = Column(String, ForeignKey('Stop.id'),
                     nullable=False)
    arrival_time = Column(DateTime, nullable=True)
    departure_time = Column(DateTime, nullable=True)
    scheduled_track = Column(String, nullable=True)
    actual_track = Column(String, nullable=True)

    def __init__(self, remaining_stops_id, stop_id, arrival_time=None,
                 departure_time=None, scheduled_track=None,
                 actual_track=None):
        self.remaining_stops_id = remaining_stops_id
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
    train_id = Column(Integer, ForeignKey('Train.id'), nullable=False)
    stop_time = Column(DateTime, nullable=False)

    def __init__(self, stop_id, train_id, stop_time):
        self.stop = stop_id
        self.train_id = train_id
        self.stop_time = stop_time


class Line(Base):
    __tablename__ = 'Line'

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)

    def __init__(self, name):
        self.name = name
