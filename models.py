from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.types import DateTime, Boolean, Time, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship


Base = declarative_base()


class Stop_time_update(Base):
    __tablename__ = 'Stop_time_update'

    id = Column(Integer, primary_key=True)
    trip_update_id = Column(String,
                            ForeignKey('Trip_update.id'),
                            nullable=False)
    stop_id = Column(String, ForeignKey('Stop.id'),
                     nullable=False)
    arrival_time = Column(DateTime, nullable=True)
    departure_time = Column(DateTime, nullable=True)
    scheduled_track = Column(String, nullable=True)
    actual_track = Column(String, nullable=True)

    trip_update = relationship('Trip_update',
                               back_populates='stop_time_updates')

    def __init__(self, id, trip_update_id, stop_id, arrival_time=None,
                 departure_time=None, scheduled_track=None,
                 actual_track=None):
        self.id = id
        self.trip_update_id = trip_update_id
        self.stop_id = stop_id
        self.arrival_time = arrival_time
        self.departure_time = departure_time
        self.scheduled_track = scheduled_track
        self.actual_track = actual_track


class Trip_update(Base):
    __tablename__ = 'Trip_update'

    id = Column(String, primary_key=True)
    trip_id = Column(String, nullable=False)
    train_unique_num = Column(String, ForeignKey('Train.unique_num'),
                              nullable=False)
    origin_date = Column(Date, nullable=False)
    origin_time = Column(Time, nullable=False)
    line_id = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    effective_timestamp = Column(DateTime, nullable=False)
    path = Column(String, nullable=True)

    train = relationship('Train', back_populates='trip_updates')

    stop_time_updates = relationship('Stop_time_update',
                                     order_by='Stop_time_update.id',
                                     back_populates='trip_update')

    alerts = relationship('Alert_message',
                          order_by='desc(Alert_message.id)',
                          back_populates='trip_update')

    def __init__(self, trip_id, train_unique_num, origin_date,
                 origin_time, line_id,
                 direction, effective_timestamp, path=None):
        self.id = train_unique_num + ": " + trip_id
        self.trip_id = trip_id
        self.train_unique_num = train_unique_num
        self.origin_date = origin_date
        self.origin_time = origin_time
        self.line_id = line_id
        self.direction = direction
        self.effective_timestamp = effective_timestamp
        self.path = path


class Train(Base):
    __tablename__ = 'Train'

    # unique_num is start_date + train_id
    unique_num = Column(String, primary_key=True)
    route_id = Column(String, nullable=False)
    is_assigned = Column(Boolean, nullable=True)
    first_seen_timestamp = Column(DateTime, nullable=False)
    is_in_system_now = Column(Boolean, nullable=False)
    next_station = Column(String, nullable=True)
    is_delayed = Column(Boolean, nullable=True)

    trip_updates = relationship('Trip_update',
                                order_by='desc(Trip_update.id)',
                                back_populates='train')

    stopped_at = relationship('Trains_stopped',
                              order_by='desc(Trains_stopped.id)',
                              back_populates='train')

    vehicle_messages = relationship('Vehicle_message',
                                    order_by='desc(Vehicle_message.id)',
                                    back_populates='train')

    def __init__(self, unique_num, route_id,
                 first_seen_timestamp, is_in_system_now,
                 is_assigned=None, next_station=None,
                 is_delayed=None):
        self.unique_num = unique_num
        self.route_id = route_id
        self.is_assigned = is_assigned
        self.first_seen_timestamp = first_seen_timestamp
        self.is_in_system_now = is_in_system_now
        self.next_station = next_station
        self.is_delayed = is_delayed

    def __repr__(self):
        return self.unique_num


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

    trains_stopped_here = relationship(
        'Trains_stopped',
        order_by='desc(Trains_stopped.id)',
        back_populates='stop'
    )

    transit_times_from_here = relationship(
        'Transit_time_fit',
        order_by='desc(Transit_time_fit.id)',
        back_populates='stop_origin',
        foreign_keys=lambda: Transit_time_fit.stop_id_origin
    )

    transit_times_to_here = relationship(
        'Transit_time_fit',
        order_by='desc(Transit_time_fit.id)',
        back_populates='stop_destination',
        foreign_keys=lambda: Transit_time_fit.stop_id_destination
    )

    vehicle_messages = relationship(
        'Vehicle_message',
        order_by='desc(Vehicle_message.id)',
        back_populates='stop'
    )

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
    '''junction table. Which trains stopped at what stops?
    When? Were they delayed?'''
    __tablename__ = 'Trains_stopped'

    id = Column(Integer, primary_key=True)
    stop_id = Column(String, ForeignKey('Stop.id'),
                     nullable=False)
    train_unique_num = Column(String,
                              ForeignKey('Train.unique_num'),
                              nullable=False)
    stop_time = Column(DateTime, nullable=False)
    delayed = Column(Boolean, nullable=False)
    delayed_MTA = Column(Boolean, nullable=False)

    train = relationship('Train', back_populates='stopped_at')
    stop = relationship('Stop', back_populates='trains_stopped_here')

    def __init__(self, id, stop_id, train_unique_num, stop_time,
                 delayed, delayed_MTA):
        self.id = id
        self.stop_id = stop_id
        self.train_unique_num = train_unique_num
        self.stop_time = stop_time
        self.delayed = delayed
        self.delayed_MTA = delayed_MTA

    def __repr__(self):
        return f'{self.train_unique_num}'\
            f' stopped at {self.stop_id} at {self.stop_time}'


class Transit_time_fit(Base):
    __tablename__ = 'Transit_time_fit'

    id = Column(Integer, primary_key=True)
    stop_id_origin = Column(String,
                            ForeignKey('Stop.id'),
                            nullable=False)
    stop_id_destination = Column(String, ForeignKey('Stop.id'),
                                 nullable=False)
    line_id = Column(String, nullable=False)
    direction = Column(String, nullable=False)
    fit_start_datetime = Column(DateTime, nullable=False)
    fit_end_datetime = Column(DateTime, nullable=False)

    stop_origin = relationship(
        'Stop',
        back_populates='transit_times_from_here',
        foreign_keys=[stop_id_origin]
    )

    stop_destination = relationship(
        'Stop',
        back_populates='transit_times_to_here',
        foreign_keys=[stop_id_destination]
    )

    means = relationship(
        'Mean_transit_time',
        order_by='desc(Mean_transit_time.id)',
        back_populates='fit'
    )

    def __init__(self, stop_id_origin, stop_id_destination,
                 line_id, direction, fit_start_datetime, fit_end_datetime):
        self.stop_id_origin = stop_id_origin
        self.stop_id_destination = stop_id_destination
        self.line_id = line_id
        self.direction = direction
        self.fit_start_datetime = fit_start_datetime
        self.fit_end_datetime = fit_end_datetime


class Mean_transit_time(Base):
    __tablename__ = 'Mean_transit_time'

    id = Column(Integer, primary_key=True)
    fit_id = Column(Integer, ForeignKey('Transit_time_fit.id'),
                    nullable=False)
    seg_start_datetime = Column(DateTime, nullable=False)
    seg_end_datetime = Column(DateTime, nullable=False)
    mean = Column(Integer, nullable=False)
    state = Column(Integer, nullable=False)

    fit = relationship('Transit_time_fit',
                       back_populates='means')

    def __init__(self, fit_id, seg_start_datetime, seg_end_datetime,
                 mean, state):
        self.fit_id = fit_id
        self.seg_start_datetime = seg_start_datetime
        self.seg_end_datetime = seg_end_datetime
        self.mean = mean
        self.state = state


class Alert_message(Base):
    __tablename__ = 'Alert_message'

    id = Column(Integer, primary_key=True)
    trip_id = Column(String, ForeignKey('Trip_update.id'),
                     nullable=False)
    header = Column(String, nullable=False)
    effective_timestamp = Column(DateTime, nullable=False)

    trip_update = relationship('Trip_update',
                               back_populates='alerts')

    def __init__(self, trip_id, header, effective_timestamp):
        self.trip_id = trip_id
        self.header = header
        self.effective_timestamp = effective_timestamp


class Vehicle_message(Base):
    __tablename__ = 'Vehicle_message'

    id = Column(Integer, primary_key=True)

    train_unique_num = Column(String, ForeignKey('Train.unique_num'),
                              nullable=False)
    effective_timestamp = Column(DateTime, nullable=False)
    current_status = Column(String, nullable=True)
    stop_id = Column(String, ForeignKey('Stop.id'), nullable=True)
    last_moved_at = Column(DateTime, nullable=True)
    current_stop_sequence = Column(Integer, nullable=True)

    train = relationship('Train',
                         back_populates='vehicle_messages')
    stop = relationship('Stop',
                        back_populates='vehicle_messages')

    def __init__(self, train_unique_num, current_status, stop_id,
                 last_moved_at, current_stop_sequence):
        self.train_unique_num = train_unique_num
        self.current_status = current_status
        self.stop_id = stop_id
        self.last_moved_at = last_moved_at
        self.current_stop_sequence = current_stop_sequence
