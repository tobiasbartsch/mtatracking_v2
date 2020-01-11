from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import DateTime, Boolean, Time, Date
from sqlalchemy.schema import UniqueConstraint

Base = declarative_base()


class train(Base):
    __tablename__ = 'train'

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


class trip_id(Base):
    __tablename__ = 'trip_id'

    id = Column(Integer, primary_key=True)
    train_id = Column(Integer, ForeignKey('train.id'), nullable=False)
    origin_date = Column(Date, nullable=False)
    origin_time = Column(Time, nullable=False)
    line_id = Column(String, ForeignKey('line.id'), nullable=False)
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
class remaining_stops(Base):
    __tablename__ = 'remaining_stops'

    id = Column(Integer, primary_key=True)
    train_id = Column(Integer, ForeignKey('train.id'), nullable=False)
    effective_timestamp = Column(DateTime, nullable=False)

    def __init__(self, train_id, effective_timestamp):
        self.train_id = train_id
        self.effective_timestamp = effective_timestamp


class stop_time_update(Base):
    __tablename__ = 'stop_time_update'

    id = Column(Integer, primary_key=True)
    remaining_stops_id = Column(Integer,
                                ForeignKey('remaining_stops.id'),
                                nullable=False)
    stop_id = Column(Integer, ForeignKey('stop.id'),
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


class stop(Base):
    __tablename__ = 'stop'

    id = Column(Integer, primary_key=True)
    code = Column(String, nullable=False)
    name = Column(String, nullable=False)

    def __init__(self, code, name):
        self.code = code
        self.name = name

    def __repr__(self):
        return self.name + ' (' + self.code + ')'


class trains_stopped(Base):
    '''junction table. Which trains stopped at what stops?'''
    __tablename__ = 'trains_stopped'

    id = Column(Integer, primary_key=True)
    stop = Column(Integer, ForeignKey('stop.id'),
                  nullable=False)
    train_id = Column(Integer, ForeignKey('train.id'), nullable=False)
    stop_time = Column(DateTime, nullable=False)

    def __init__(self, stop_id, train_id, stop_time):
        self.stop = stop_id
        self.train_id = train_id
        self.stop_time = stop_time


class line(Base):
    __tablename__ = 'line'

    id = Column(Integer, primary_key=True)
    name = Column(String, mullable=False)

    def __init__(self, name):
        self.name = name
