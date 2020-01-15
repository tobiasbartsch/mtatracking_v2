import numpy as np
import datetime
from datetime import datetime as ddatetime
from datetime import timedelta

import mtatracking_v2.nyct_subway_pb2 as nyct_subway_pb2

from pytz import timezone

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

    def __init__(self, trip_update_id, stop_id, arrival_time=None,
                 departure_time=None, scheduled_track=None,
                 actual_track=None):
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

    trip_updates = relationship('Trip_update',
                                order_by='desc(Trip_update.id)',
                                back_populates='train')

    stopped_at = relationship('Trains_stopped',
                              order_by='desc(Trains_stopped.id)',
                              back_populates='train')

    def __init__(self, unique_num, route_id,
                 first_seen_timestamp, is_in_system_now,
                 is_assigned=None):
        self.unique_num = unique_num
        self.route_id = route_id
        self.is_assigned = is_assigned
        self.first_seen_timestamp = first_seen_timestamp
        self.is_in_system_now = is_in_system_now

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
                              back_populates='stop')

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
    stop_id = Column(String, ForeignKey('Stop.id'),
                     nullable=False)
    train_unique_num = Column(String,
                              ForeignKey('Train.unique_num'),
                              nullable=False)
    stop_time = Column(DateTime, nullable=False)

    train = relationship('Train', back_populates='stopped_at')
    stop = relationship('Stop', back_populates='trains_stopped_here')

    def __init__(self, stop_id, train_unique_num, stop_time):
        self.stop_id = stop_id
        self.train_unique_num = train_unique_num
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

        # keep a dictionary of trains currently in the system
        # (and their arr stations). This will allow us to determine
        # whether a train stopped at a station without querying the
        # database
        current_trains = self.session.query(Train).\
            filter_by(is_in_system_now=True)
        if current_trains:
            self.curr_trains_arr_st_dict = {
                train.unique_num: train.trip_updates[0].
                stop_time_updates[0].stop_id
                if train.trip_updates[0].stop_time_updates
                else 'Unknown'
                for train in current_trains}
        else:
            self.curr_trains_arr_st_dict = {}

    def attach_tracking_data(self, data):
        """Process the protocol buffer feed and populate our
        subway model with its data.

        Args:
            data: List of protocol buffer messages containing
                  trip_update, vehicle, or alert feed entities
                  (presumably downloaded from the MTA realtime stream).
                  One message per requested feed.
                  The data MUST contain all tracked trains across the
                  entire subway system. If a train
                  is no longer in this feed, we assume that it
                  arrived at the last station it had been
                  traveling to and is longer in service.
        """
        # get the trains that are currently in the system:
        # we will remove entries from this list while processing FeedEntities.
        # The trains left in this list are the ones that are no longer in
        # the feed.
        leftover_train_uniques = set(list(self.curr_trains_arr_st_dict.keys()))
        current_time = None

        for message in data:
            current_time = message.header.timestamp
            for FeedEntity in message.entity:
                if len(FeedEntity.trip_update.trip.trip_id) > 0:
                    # entity type "trip_update"
                    leftover_train_uniques = self._processTripUpdate(
                                            FeedEntity,
                                            current_time,
                                            leftover_train_uniques)
                if len(FeedEntity.vehicle.trip.trip_id) > 0:
                    # entity type "vehicle"
                    # leftover_train_uniques = self\
                    #     ._processVehicleMessage(FeedEntity, current_time,
                    #                             leftover_train_uniques)
                    pass
                if len(FeedEntity.alert.header_text.translation) > 0:
                    # alert message
                    # leftover_train_uniques = self\
                    #     ._processAlertMessage(FeedEntity, current_time,
                    #                           leftover_train_uniques)
                    pass
        # any leftover trains have stopped at their last known stations
        # register their arrival, set their 'is_in_system_now=False'
        self._performCleanup(current_time, leftover_train_uniques)
        self.session.commit()

    def _performCleanup(self, current_time, leftover_train_uniques):
        """Set the is_in_system_now attribute of the leftover trains to False.
        Register the arrival of these trains at their last known stations.
        """
        if leftover_train_uniques:
            # make DateTime object from current_time
            current_time_dt = ddatetime.fromtimestamp(current_time)
            current_time_dt = timezone('US/Eastern').localize(current_time_dt)

            leftover_trains = self.session.query(Train).filter(
                Train.unique_num.in_(leftover_train_uniques)).all()
            for train in leftover_trains:
                train.is_in_system_now = False
                stopped_at = self.curr_trains_arr_st_dict[train.unique_num]
                this_train_stopped = Trains_stopped(stopped_at,
                                                    train.unique_num,
                                                    current_time_dt)
                self.session.add(this_train_stopped)

    def _processTripUpdate(self, FeedEntity, current_time,
                           leftover_train_uniques):
        """Add data contained in the Protobuffer's Trip Update FeedEntity
        to the subway system.

        Args:
            FeedEntity: TripUpdate FeedEntity (from protobuffer).
            current_time (timestamp): Timestamp in seconds since 1970
            leftover_train_uniques (list of strings): Unique numbers of
                                            trains that had been in the system
                                            before we processed messages.
        """
        # make DateTime object from current_time
        current_time_dt = ddatetime.fromtimestamp(current_time)
        current_time_dt = timezone('US/Eastern').localize(current_time_dt)

        # Add current train to database
        train_id = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].train_id
        origin_date = FeedEntity.trip_update.trip.start_date
        unique_num = origin_date + ": " + train_id
        origin_date = datetime.datetime.strptime(origin_date, "%Y%m%d").date()
        route_id = FeedEntity.trip_update.trip.route_id
        is_assigned = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].is_assigned

        this_train = Train(unique_num=unique_num, route_id=route_id,
                           first_seen_timestamp=current_time_dt,
                           is_in_system_now=True,
                           is_assigned=is_assigned)

        self.session.merge(this_train)
        # self.session.flush()

        # Add current trip to database
        trip_id = FeedEntity.trip_update.trip.trip_id
        origin_time, _, direction, path_id = self.parse_trip_id(trip_id)
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

        self.session.merge(this_trip)

        # need to call this to auto-populate the Trip_update id:
        # self.session.flush()

        # Add stop time updates to database

        for stu in FeedEntity.trip_update.stop_time_update:
            stop_id = stu.stop_id
            # check whether this stop is in our table of stops.
            # If it isn't, add it.
            if stop_id not in self.stop_ids:
                stop = Stop(stop_id, name='Unknown')
                self.session.add(stop)
                # self.session.flush()
                self.stop_ids.append(stop_id)

            arrival_time = stu.arrival.time
            arrival_time_dt = ddatetime.fromtimestamp(arrival_time)
            arrival_time_dt = timezone('US/Eastern').localize(arrival_time_dt)

            departure_time = stu.departure.time
            departure_time_dt = ddatetime.fromtimestamp(departure_time)
            departure_time_dt = timezone('US/Eastern').\
                localize(departure_time_dt)

            scheduled_track = stu.\
                Extensions[nyct_subway_pb2.
                           nyct_stop_time_update].scheduled_track
            actual_track = stu.\
                Extensions[nyct_subway_pb2.
                           nyct_stop_time_update].actual_track
            this_stu = Stop_time_update(trip_update_id=this_trip.id,
                                        stop_id=stop_id,
                                        arrival_time=arrival_time_dt,
                                        departure_time=departure_time_dt,
                                        scheduled_track=scheduled_track,
                                        actual_track=actual_track)
            self.session.add(this_stu)

        # determine whether our train has just stopped at a station:
        stopped_at = None
        if this_train.unique_num in self.curr_trains_arr_st_dict:
            # we processed this train:
            leftover_train_uniques.remove(this_train.unique_num)
            if FeedEntity.trip_update.stop_time_update[0].stop_id is not\
                    self.curr_trains_arr_st_dict[this_train.unique_num]:
                # we just stopped at
                # curr_trains_arr_st_dict[this_train.unique_num]
                stopped_at = self.curr_trains_arr_st_dict[
                    this_train.unique_num]
                # set new arrival station for our train:
                self.curr_trains_arr_st_dict[
                    this_train.unique_num] = FeedEntity\
                    .trip_update.stop_time_update[0].stop_id
        else:
            # register this train with our dictionary
            if FeedEntity.trip_update.stop_time_update:
                self.curr_trains_arr_st_dict[
                    this_train.unique_num] = FeedEntity\
                        .trip_update.stop_time_update[0].stop_id
            else:
                self.curr_trains_arr_st_dict[this_train.unique_num] = 'Unknown'
        # TODO figure out which trains vanished from the feed. Remove them from
        # the dictionary and set their last arrival station

        if stopped_at:
            this_train_stopped = Trains_stopped(stopped_at,
                                                this_train.unique_num,
                                                current_time_dt)
            self.session.add(this_train_stopped)

        return leftover_train_uniques

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


class SubwaySystem_bulk_updater:
    """A subway system consists of stations, lines, and trains.
    These objects are stored in a database and accessed by SQLAlchemy.

    THIS OBJECT CONTAINS HELPER FUNCTIONS TO LOAD HISTORICAL DATA IN BULK.
    WE WILL TRY TO AVOID PERFORMING ANY ORM OPERATIONS UNTIL ALL TRAINS,
    STOPS, ETC ARE IN MEMORY.

    WE ASSUME THAT WE ARE STARTING WITH AN EMPTY DATABASE (EXCEPT FOR STOPS)
    """

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
        self.stops_dict = {s.id: s for s in session.query(Stop).all()}

        # keep a dictionary of trains currently in the system
        # (and their arr stations). This will allow us to determine
        # whether a train stopped at a station without querying the
        # database

        # keys: uniquenums, vals: arr stations
        self.curr_trains_arr_st_dict = {}

        # keys: primary_keys, vals: ORM objects
        self.trains_dict = {}
        self.trip_update_dict = {}
        self.stop_time_update_dict = {}
        self.trains_stopped_dict = {}

        # increment this every time we want to add a
        # stoptimeupdate and use it as primary key
        self.stoptimeupdate_counter = 1
        # increment this every time we want to add a
        # Trains_stopped and use it as primary key
        self.trainsstopped_counter = 1

    def performBulkUpdate(self):
        # BULK UPDATE DATABASE
        # first deal with any stops we still may need to add.
        dbstop_list = [stop.id for stop in self.session.query(Stop).all()]
        new_stops = set(list(self.stops_dict.keys())) - set(dbstop_list)
        for stop in new_stops:
            self.session.add(self.stops_dict[stop])
        self.session.commit()

        # now perform bulk update of everything at once. I wonder whether I
        # can just make one long list of stuff.

        objs = list(self.trains_dict.values())\
            + list(self.trip_update_dict.values())\
            + list(self.stop_time_update_dict.values())\
            + list(self.trains_stopped_dict.values())
        self.session.bulk_save_objects(objs)
        self.session.commit()

    def attach_tracking_data(self, data):
        """Process the protocol buffer feed and populate our
        subway model with its data.

        Args:
            data: List of protocol buffer messages containing
                  trip_update, vehicle, or alert feed entities
                  (presumably downloaded from the MTA realtime stream).
                  One message per requested feed.
                  The data MUST contain all tracked trains across the
                  entire subway system. If a train
                  is no longer in this feed, we assume that it
                  arrived at the last station it had been
                  traveling to and is longer in service.
        """
        # get the trains that are currently in the system:
        # we will remove entries from this list while processing FeedEntities.
        # The trains left in this list are the ones that are no longer in
        # the feed.
        leftover_train_uniques = set(list(self.curr_trains_arr_st_dict.keys()))
        current_time = None

        for message in data:
            current_time = message.header.timestamp
            for FeedEntity in message.entity:
                if len(FeedEntity.trip_update.trip.trip_id) > 0:
                    # entity type "trip_update"
                    leftover_train_uniques = self._processTripUpdate(
                                            FeedEntity,
                                            current_time,
                                            leftover_train_uniques)
                if len(FeedEntity.vehicle.trip.trip_id) > 0:
                    # entity type "vehicle"
                    # leftover_train_uniques = self\
                    #     ._processVehicleMessage(FeedEntity, current_time,
                    #                             leftover_train_uniques)
                    pass
                if len(FeedEntity.alert.header_text.translation) > 0:
                    # alert message
                    # leftover_train_uniques = self\
                    #     ._processAlertMessage(FeedEntity, current_time,
                    #                           leftover_train_uniques)
                    pass
        # any leftover trains have stopped at their last known stations
        # register their arrival, set their 'is_in_system_now=False'
        self._performCleanup(current_time, leftover_train_uniques)

    def _performCleanup(self, current_time, leftover_train_uniques):
        """Set the is_in_system_now attribute of the leftover trains to False.
        Register the arrival of these trains at their last known stations.
        """
        if leftover_train_uniques:
            # make DateTime object from current_time
            current_time_dt = ddatetime.fromtimestamp(current_time)
            current_time_dt = timezone('US/Eastern').localize(current_time_dt)

            # print('performing clean up on the following trains: '
            #      + ' '.join(list(leftover_train_uniques)))
            leftover_trains = self.session.query(Train).filter(
                Train.unique_num.in_(leftover_train_uniques)).all()
            leftover_trains = [
                self.trains_dict[t] for t in leftover_train_uniques]
            for train in leftover_trains:
                train.is_in_system_now = False
                stopped_at = self.curr_trains_arr_st_dict[train.unique_num]
                this_train_stopped = Trains_stopped(stopped_at,
                                                    train.unique_num,
                                                    current_time_dt)
                self.trains_stopped_dict[
                    self.trainsstopped_counter] = this_train_stopped
                self.trainsstopped_counter += 1

    def _processTripUpdate(self, FeedEntity, current_time,
                           leftover_train_uniques):
        """Add data contained in the Protobuffer's Trip Update FeedEntity
        to the subway system.

        Args:
            FeedEntity: TripUpdate FeedEntity (from protobuffer).
            current_time (timestamp): Timestamp in seconds since 1970
            leftover_train_uniques (list of strings): Unique numbers of
                                            trains that had been in the system
                                            before we processed messages.
        """
        # make DateTime object from current_time
        current_time_dt = ddatetime.fromtimestamp(current_time)
        current_time_dt = timezone('US/Eastern').localize(current_time_dt)

        # Add current train to dict
        train_id = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].train_id
        origin_date = FeedEntity.trip_update.trip.start_date
        unique_num = origin_date + ": " + train_id
        origin_date = datetime.datetime.strptime(origin_date, "%Y%m%d").date()
        route_id = FeedEntity.trip_update.trip.route_id
        is_assigned = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].is_assigned

        this_train = Train(unique_num=unique_num, route_id=route_id,
                           first_seen_timestamp=current_time_dt,
                           is_in_system_now=True,
                           is_assigned=is_assigned)

        self.trains_dict[unique_num] = this_train

        # Add current trip to dict
        trip_id = FeedEntity.trip_update.trip.trip_id
        origin_time, _, direction, path_id = self.parse_trip_id(trip_id)
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

        self.trip_update_dict[this_trip.id] = this_trip

        # Add stop time updates to dict

        for stu in FeedEntity.trip_update.stop_time_update:
            stop_id = stu.stop_id
            # check whether this stop is in our table of stops.
            # If it isn't, add it.
            if stop_id not in self.stop_ids:
                stop = Stop(stop_id, name='Unknown')
                self.stops_dict[stop_id] = stop
                self.stop_ids.append(stop_id)

            arrival_time = stu.arrival.time
            arrival_time_dt = ddatetime.fromtimestamp(arrival_time)
            arrival_time_dt = timezone('US/Eastern').localize(arrival_time_dt)

            departure_time = stu.departure.time
            departure_time_dt = ddatetime.fromtimestamp(departure_time)
            departure_time_dt = timezone('US/Eastern').\
                localize(departure_time_dt)

            scheduled_track = stu.\
                Extensions[nyct_subway_pb2.
                           nyct_stop_time_update].scheduled_track
            actual_track = stu.\
                Extensions[nyct_subway_pb2.
                           nyct_stop_time_update].actual_track
            this_stu = Stop_time_update(trip_update_id=this_trip.id,
                                        stop_id=stop_id,
                                        arrival_time=arrival_time_dt,
                                        departure_time=departure_time_dt,
                                        scheduled_track=scheduled_track,
                                        actual_track=actual_track)
            self.stop_time_update_dict[self.stoptimeupdate_counter] = this_stu
            self.stoptimeupdate_counter += 1

        # determine whether our train has just stopped at a station:
        stopped_at = None
        if this_train.unique_num in self.curr_trains_arr_st_dict:
            # we processed this train:
            leftover_train_uniques.remove(this_train.unique_num)
            if FeedEntity.trip_update.stop_time_update[0].stop_id is not\
                    self.curr_trains_arr_st_dict[this_train.unique_num]:
                # we just stopped at
                # curr_trains_arr_st_dict[this_train.unique_num]
                stopped_at = self.curr_trains_arr_st_dict[
                    this_train.unique_num]
                # set new arrival station for our train:
                self.curr_trains_arr_st_dict[
                    this_train.unique_num] = FeedEntity\
                    .trip_update.stop_time_update[0].stop_id
        else:
            # register this train with our dictionary
            if FeedEntity.trip_update.stop_time_update:
                self.curr_trains_arr_st_dict[
                    this_train.unique_num] = FeedEntity\
                        .trip_update.stop_time_update[0].stop_id
            else:
                self.curr_trains_arr_st_dict[this_train.unique_num] = 'Unknown'

        if stopped_at:
            this_train_stopped = Trains_stopped(stopped_at,
                                                this_train.unique_num,
                                                current_time_dt)
            self.trains_stopped_dict[
                self.trainsstopped_counter] = this_train_stopped

        return leftover_train_uniques

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
