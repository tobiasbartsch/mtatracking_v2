import numpy as np
import datetime
from datetime import datetime as ddatetime
from datetime import timedelta
from sqlalchemy import desc
import mtatracking_v2.nyct_subway_pb2 as nyct_subway_pb2

from pytz import timezone

from mtatracking_v2.models import (Train,
                                   Stop,
                                   Stop_time_update,
                                   Trains_stopped,
                                   Trip_update,
                                   Alert_message,
                                   Vehicle_message
                                   )


class SubwaySystem_no_StopTimeUpdates:
    """A subway system consists of stations, lines, and trains.
    These objects are stored in a database and accessed by SQLAlchemy.
    Here we do not populate the Stop_time_update table which hopefully
    makes populating the database way faster.
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
                    # entity type alert message
                    leftover_train_uniques = self\
                        ._processAlertMessage(FeedEntity, current_time)
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
        if FeedEntity.trip_update and FeedEntity.trip_update.stop_time_update:
            next_station = FeedEntity.trip_update.stop_time_update[0].stop_id
        else:
            next_station = 'Unknown'

        this_train = Train(unique_num=unique_num, route_id=route_id,
                           first_seen_timestamp=current_time_dt,
                           is_in_system_now=True,
                           is_assigned=is_assigned, next_station=next_station)

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

        # determine whether our train has just stopped at a station:
        stopped_at = None
        if this_train.unique_num in self.curr_trains_arr_st_dict:
            # we processed this train:
            leftover_train_uniques.remove(this_train.unique_num)
            if next_station !=\
                    self.curr_trains_arr_st_dict[this_train.unique_num]:
                # we just stopped at
                # curr_trains_arr_st_dict[this_train.unique_num]
                stopped_at = self.curr_trains_arr_st_dict[
                    this_train.unique_num]
                # set new arrival station for our train:
                self.curr_trains_arr_st_dict[
                    this_train.unique_num] = next_station
        else:
            # register this train with our dictionary
            self.curr_trains_arr_st_dict[
                this_train.unique_num] = next_station

        if stopped_at:
            this_train_stopped = Trains_stopped(stopped_at,
                                                this_train.unique_num,
                                                current_time_dt)
            if stopped_at not in self.stop_ids:
                this_stop = Stop(stopped_at, 'Unknown')
                self.session.add(this_stop)
                self.stop_ids.append(stopped_at)
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


class SubwaySystem_bulk_updater_noStopTimeUpdate:
    """A subway system consists of stations, lines, and trains.
    These objects are stored in a database and accessed by SQLAlchemy.

    THIS OBJECT CONTAINS HELPER FUNCTIONS TO LOAD HISTORICAL DATA IN BULK.
    WE WILL TRY TO AVOID PERFORMING ANY ORM OPERATIONS UNTIL ALL TRAINS,
    STOPS, ETC ARE IN MEMORY.
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

        self.resetSystem(session)

    def setStartingPrimaryKeys(self):
        # increment this every time we want to add a
        # stoptimeupdate and use it as primary key
        session = self.session
        stoptimeupdate_last = session.query(
            Stop_time_update).order_by(
                desc(Stop_time_update.id)).limit(1).one_or_none()
        if stoptimeupdate_last:
            self.stoptimeupdate_counter = stoptimeupdate_last.id + 1
        else:
            self.stoptimeupdate_counter = 1
        # increment this every time we want to add a
        # Trains_stopped and use it as primary key
        trainsstopped_last = session.query(
            Trains_stopped).order_by(desc(
                Trains_stopped.id)).limit(1).one_or_none()
        if trainsstopped_last:
            self.trainsstopped_counter = trainsstopped_last.id + 1
        else:
            self.trainsstopped_counter = 1

    def performBulkUpdate(self):
        # BULK UPDATE DATABASE
        # first deal with any stops we still may need to add.
        dbstop_list = [stop.id for stop in self.session.query(Stop).all()]
        vehicle_msg_stops = [
            m.stop_id for m in self.vmessage_list if m.stop_id is not None]
        trains_stopped_stops = [
            t.stop_id for t in self.trains_stopped_dict.values()
            if t.stop_id is not None]
        new_stops = set(list(
            self.stops_dict.keys()) + vehicle_msg_stops +
                trains_stopped_stops) - set(dbstop_list)
        for stop in new_stops:
            if stop in self.stops_dict.keys():
                self.session.add(self.stops_dict[stop])
            else:
                unknown_new_stop = Stop(stop, 'Unknown')
                self.session.add(unknown_new_stop)
        self.session.commit()

        # now perform bulk update of everything at once. I wonder whether I
        # can just make one long list of stuff.

        # first check train and trip_updates for items that are already in the
        # database. We cannot add duplicates, so we have to merge for which
        # there is no bulk operation
        # see here: https://stackoverflow.com/questions/25955200/
        # sqlalchemy-performing-a-bulk-upsert-if-exists-update-else-insert-in-postgr
        for each in self.session.query(Train)\
                .filter(Train.unique_num.in_(self.trains_dict.keys())).all():
            self.session.merge(self.trains_dict.pop(each.unique_num))
        for each in self.session.query(Trip_update)\
                .filter(Trip_update.id.in_(
                    self.trip_update_dict.keys())).all():
            self.session.merge(self.trip_update_dict.pop(each.id))

        # finally do our bulk update
        objs = list(self.trains_dict.values())\
            + list(self.trip_update_dict.values())\
            + list(self.stop_time_update_dict.values())\
            + list(self.trains_stopped_dict.values())\
            + self.alerts_list\
            + self.vmessage_list
        self.session.bulk_save_objects(objs)
        self.session.commit()

        self.resetSystem(self.session)

    def resetSystem(self, session):
        # keep the Stops table in memory so that we can check whether
        # a stop is in the database without performing a query:
        self.stop_ids = [s.id for s in session.query(Stop).all()]
        self.stops_dict = {s.id: s for s in session.query(Stop).all()}

        # keep a dictionary of trains currently in the system
        # (and their arr stations). This will allow us to determine
        # whether a train stopped at a station without querying the
        # database

        curr_trains = session.query(Train).filter(
            Train.is_in_system_now == True).all()
        if curr_trains:
            self.curr_trains_arr_st_dict = {t.unique_num: t.next_station
                                            for t in curr_trains}
            self.trains_dict = {t.unique_num: t for t in curr_trains}
        else:
            # keys: uniquenums, vals: arr stations
            self.curr_trains_arr_st_dict = {}
            # keys: primary_keys, vals: ORM objects
            self.trains_dict = {}

        self.trip_update_dict = {}
        self.stop_time_update_dict = {}
        self.trains_stopped_dict = {}
        self.alerts_list = []
        self.vmessage_list = []

        # dict of trip origin dates.
        # keys are trip_id from GTFS, NOT our keys in the DB.
        self.trip_origin_date_dict = {}

        self.setStartingPrimaryKeys()

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
            # make DateTime object from current_time
            current_time_dt = ddatetime.fromtimestamp(current_time)
            current_time_dt = timezone('US/Eastern').localize(current_time_dt)

            for FeedEntity in message.entity:
                if len(FeedEntity.trip_update.trip.trip_id) > 0:
                    # entity type "trip_update"
                    leftover_train_uniques = self._processTripUpdate(
                                            FeedEntity,
                                            current_time_dt,
                                            leftover_train_uniques)
                if len(FeedEntity.vehicle.trip.trip_id) > 0:
                    # entity type "vehicle"
                    self._processVehicleMessage(FeedEntity, current_time_dt)
                if len(FeedEntity.alert.header_text.translation) > 0:
                    # alert message
                    self._processAlertMessage(FeedEntity, current_time_dt)

        # any leftover trains have stopped at their last known stations
        # register their arrival, set their 'is_in_system_now=False'
        self._performCleanup(current_time_dt, leftover_train_uniques)

    def _performCleanup(self, current_time_dt, leftover_train_uniques):
        """Set the is_in_system_now attribute of the leftover trains to False.
        Register the arrival of these trains at their last known stations.
        """
        if leftover_train_uniques:

            leftover_trains = [
                self.trains_dict[t] for t in leftover_train_uniques]
            for train in leftover_trains:
                train.is_in_system_now = False
                stopped_at = self.curr_trains_arr_st_dict[train.unique_num]
                this_train_stopped = Trains_stopped(self.trainsstopped_counter,
                                                    stopped_at,
                                                    train.unique_num,
                                                    current_time_dt,
                                                    delayed=False,
                                                    delayed_MTA=False)
                self.trains_stopped_dict[
                    self.trainsstopped_counter] = this_train_stopped
                self.trainsstopped_counter += 1
                self.curr_trains_arr_st_dict.pop(train.unique_num)

    def _processTripUpdate(self, FeedEntity, current_time_dt,
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

        # Add current train to dict
        train_id = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].train_id
        origin_date = FeedEntity.trip_update.trip.start_date
        unique_num = origin_date + ": " + train_id
        origin_date = datetime.datetime.strptime(origin_date, "%Y%m%d").date()
        route_id = FeedEntity.trip_update.trip.route_id
        is_assigned = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].is_assigned
        if FeedEntity.trip_update and FeedEntity.trip_update.stop_time_update:
            next_station = FeedEntity.trip_update.stop_time_update[0].stop_id
        else:
            next_station = 'Unknown'

        if unique_num not in self.trains_dict.keys():
            this_train = Train(unique_num=unique_num, route_id=route_id,
                               first_seen_timestamp=current_time_dt,
                               is_in_system_now=True,
                               is_assigned=is_assigned,
                               next_station=next_station)
            self.trains_dict[unique_num] = this_train
        else:
            self.trains_dict[unique_num].is_assigned = is_assigned
            self.trains_dict[unique_num].next_station = next_station
            this_train = self.trains_dict[unique_num]

        # Add current trip to dict
        trip_id = FeedEntity.trip_update.trip.trip_id
        # We need this later for alert messages:
        self.trip_origin_date_dict[trip_id] = origin_date
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

        # determine whether our train has just stopped at a station:
        stopped_at = None
        if this_train.unique_num in self.curr_trains_arr_st_dict:
            # we processed this train:
            if this_train.unique_num in leftover_train_uniques:
                leftover_train_uniques.remove(this_train.unique_num)
            else:
                print("warning: processed train that was not in set")
            if next_station !=\
                    self.curr_trains_arr_st_dict[this_train.unique_num]:
                # we just stopped at
                # curr_trains_arr_st_dict[this_train.unique_num]
                stopped_at = self.curr_trains_arr_st_dict[
                    this_train.unique_num]
                if(this_train.unique_num == '20190707: 1Q 2010+ STL/962'):
                    # print(this_train)
                    # print(FeedEntity.trip_update.stop_time_update[0])
                    print("************")
                    print('stopped at: ', stopped_at)
                    print('next station: ', next_station)
                # set new arrival station for our train:
                self.curr_trains_arr_st_dict[
                    this_train.unique_num] = next_station
        else:
            # register this train with our dictionary
            self.curr_trains_arr_st_dict[
                this_train.unique_num] = next_station

        if stopped_at:
            this_train_stopped = Trains_stopped(self.trainsstopped_counter,
                                                stopped_at,
                                                this_train.unique_num,
                                                current_time_dt,
                                                delayed=False,
                                                delayed_MTA=False)
            if stopped_at not in self.stop_ids:
                this_stop = Stop(stopped_at, 'Unknown')
                self.stops_dict[stopped_at] = this_stop
                self.stop_ids.append(stopped_at)
            self.trains_stopped_dict[
                self.trainsstopped_counter] = this_train_stopped
            self.trainsstopped_counter += 1

        return leftover_train_uniques

    def _processAlertMessage(self, FeedEntity, current_time_dt):
        '''process any alert messages in the feed. These are always delay messages
        and should always refer to a delayed train.

        Args:
            FeedEntity: AlertMessage FeedEntity (from protobuffer).
            current_time (timestamp): timestamp in seconds since 1970
        '''
        # sometimes there are alert messages without reference to trains.
        # We can't really use those so we will for now ignore them.
        if len(FeedEntity.alert.informed_entity) > 0:
            for tr in FeedEntity.alert.informed_entity:
                # we need to construct the DB ID of the trip update that this
                # alert message belongs to.
                tr_id = tr.trip.trip_id
                train_id = tr.trip.Extensions[
                    nyct_subway_pb2.nyct_trip_descriptor].train_id
                if tr_id in self.trip_origin_date_dict.keys():
                    origin_date = self.trip_origin_date_dict[tr_id]
                else:
                    # fallback to hoping that the current date is right
                    origin_date = current_time_dt
                unique_num = origin_date.strftime('%Y%m%d') + ": " + train_id
                # tr_id is ID in GTFS; trip_id is ID in DB:
                trip_id = unique_num + ": " + tr_id
                if trip_id in self.trip_update_dict.keys():
                    if len(FeedEntity.alert.header_text.translation) > 0:
                        for h in FeedEntity.alert.header_text.translation:
                            header = h.text
                            thisalert = Alert_message(
                                trip_id, header, current_time_dt)
                            self.alerts_list.append(thisalert)
                else:
                    print('warning: alert message refers '
                          'to non-existent trip update')

    def _processVehicleMessage(self, FeedEntity, current_time_dt):
        train_id = FeedEntity.vehicle.trip.Extensions[
            nyct_subway_pb2.nyct_trip_descriptor].train_id
        origin_date = FeedEntity.vehicle.trip.start_date
        unique_num = origin_date + ": " + train_id
        current_status = FeedEntity.vehicle.current_status
        stop_id = FeedEntity.vehicle.stop_id
        if stop_id == '':
            stop_id = None
        last_moved_at = FeedEntity.vehicle.timestamp
        last_moved_at = ddatetime.fromtimestamp(last_moved_at)
        last_moved_at = timezone('US/Eastern').localize(last_moved_at)

        current_stop_sequence = FeedEntity.vehicle.current_stop_sequence
        effective_timestamp = current_time_dt
        vmessage = Vehicle_message(unique_num, current_status,
                                   stop_id, last_moved_at,
                                   current_stop_sequence,
                                   effective_timestamp)
        self.vmessage_list.append(vmessage)

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

        self.resetSystem(session)

    def resetSystem(self, session):
        # keep the Stops table in memory so that we can check whether
        # a stop is in the database without performing a query:
        self.stop_ids = [s.id for s in session.query(Stop).all()]
        self.stops_dict = {s.id: s for s in session.query(Stop).all()}

        # keep a dictionary of trains currently in the system
        # (and their arr stations). This will allow us to determine
        # whether a train stopped at a station without querying the
        # database

        curr_trains = session.query(Train).filter(
            Train.is_in_system_now == True).all()
        if curr_trains:
            self.curr_trains_arr_st_dict = {t.unique_num: t.next_station
                                            for t in curr_trains}
            self.trains_dict = {t.unique_num: t for t in curr_trains}
        else:
            # keys: uniquenums, vals: arr stations
            self.curr_trains_arr_st_dict = {}
            # keys: primary_keys, vals: ORM objects
            self.trains_dict = {}

        # self.trip_update_dict = {}
        # self.stop_time_update_dict = {}
        # self.trains_stopped_dict = {}
        self.trip_update_list = []
        self.alerts_list = []
        self.vmessage_list = []

        # dict of trip origin dates.
        # keys are trip_id from GTFS, NOT our keys in the DB.
        self.trip_origin_date_dict = {}

        self.setStartingPrimaryKeys()

    def setStartingPrimaryKeys(self):
        # increment this every time we want to add a
        # stoptimeupdate and use it as primary key
        session = self.session
        stoptimeupdate_last = session.query(
            Stop_time_update).order_by(
                desc(Stop_time_update.id)).limit(1).one_or_none()
        if stoptimeupdate_last:
            self.stoptimeupdate_counter = stoptimeupdate_last.id + 1
        else:
            self.stoptimeupdate_counter = 1
        # increment this every time we want to add a
        # Trains_stopped and use it as primary key
        trainsstopped_last = session.query(
            Trains_stopped).order_by(desc(
                Trains_stopped.id)).limit(1).one_or_none()
        if trainsstopped_last:
            self.trainsstopped_counter = trainsstopped_last.id + 1
        else:
            self.trainsstopped_counter = 1
        # increment this every time we want to add a
        # Stop_time_update and use it as primary key
        stop_time_update_last = session.query(
            Stop_time_update).order_by(desc(
                Stop_time_update.id)).limit(1).one_or_none()
        if stop_time_update_last:
            self.stu_counter = stop_time_update_last.id + 1
        else:
            self.stu_counter = 1

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
            # make DateTime object from current_time
            current_time_dt = ddatetime.fromtimestamp(current_time)
            current_time_dt = timezone('US/Eastern').localize(current_time_dt)

            for FeedEntity in message.entity:
                if len(FeedEntity.trip_update.trip.trip_id) > 0:
                    # entity type "trip_update"
                    leftover_train_uniques = self._processTripUpdate(
                                            FeedEntity,
                                            current_time_dt,
                                            leftover_train_uniques)
                if len(FeedEntity.vehicle.trip.trip_id) > 0:
                    # entity type "vehicle"
                    self._processVehicleMessage(FeedEntity, current_time_dt)
                if len(FeedEntity.alert.header_text.translation) > 0:
                    # alert message
                    self._processAlertMessage(FeedEntity, current_time_dt)

        # any leftover trains have stopped at their last known stations
        # register their arrival, set their 'is_in_system_now=False'
        self._performCleanup(current_time_dt, leftover_train_uniques)
        self.session.commit()
        self.resetSystem(self.session)

    def _performCleanup(self, current_time_dt, leftover_train_uniques):
        """Set the is_in_system_now attribute of the leftover trains to False.
        Register the arrival of these trains at their last known stations.
        """
        if leftover_train_uniques:

            leftover_trains = self.session.query(Train).filter(
                Train.unique_num.in_(leftover_train_uniques)).all()
            for train in leftover_trains:
                train.is_in_system_now = False
                stopped_at = self.curr_trains_arr_st_dict[train.unique_num]
                this_train_stopped = Trains_stopped(self.trainsstopped_counter,
                                                    stopped_at,
                                                    train.unique_num,
                                                    current_time_dt,
                                                    delayed=False,
                                                    delayed_MTA=False)
                self.trainsstopped_counter += 1
                self.session.add(this_train_stopped)

    def _processTripUpdate(self, FeedEntity, current_time_dt,
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

        # Add current train to database
        train_id = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].train_id
        origin_date = FeedEntity.trip_update.trip.start_date
        unique_num = origin_date + ": " + train_id
        origin_date = datetime.datetime.strptime(origin_date, "%Y%m%d").date()
        route_id = FeedEntity.trip_update.trip.route_id
        is_assigned = FeedEntity.trip_update.trip\
            .Extensions[nyct_subway_pb2.nyct_trip_descriptor].is_assigned
        if FeedEntity.trip_update and FeedEntity.trip_update.stop_time_update:
            next_station = FeedEntity.trip_update.stop_time_update[0].stop_id
        else:
            next_station = 'Unknown'

        this_train = Train(unique_num=unique_num, route_id=route_id,
                           first_seen_timestamp=current_time_dt,
                           is_in_system_now=True,
                           is_assigned=is_assigned,
                           next_station=next_station)

        self.session.merge(this_train)

        # Add current trip to database
        trip_id = FeedEntity.trip_update.trip.trip_id
        # We need this later for alert messages:
        self.trip_origin_date_dict[trip_id] = origin_date
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
        self.trip_update_list.append(trip_id)

        # need to call this to auto-populate the Trip_update id:
        # self.session.flush()

        # determine whether our train has just stopped at a station:
        stopped_at = None
        if this_train.unique_num in self.curr_trains_arr_st_dict:
            # we processed this train:
            if this_train.unique_num in leftover_train_uniques:
                leftover_train_uniques.remove(this_train.unique_num)
            else:
                print("warning: processed train that was not in set")
            if next_station !=\
                    self.curr_trains_arr_st_dict[this_train.unique_num]:
                # we just stopped at
                # curr_trains_arr_st_dict[this_train.unique_num]
                stopped_at = self.curr_trains_arr_st_dict[
                    this_train.unique_num]
                if(this_train.unique_num == '20190707: 1Q 2010+ STL/962'):
                    # print(this_train)
                    # print(FeedEntity.trip_update.stop_time_update[0])
                    print("************")
                    print('stopped at: ', stopped_at)
                    print('next station: ', next_station)
                # set new arrival station for our train:
                self.curr_trains_arr_st_dict[
                    this_train.unique_num] = next_station
        else:
            # register this train with our dictionary
            self.curr_trains_arr_st_dict[
                this_train.unique_num] = next_station

        if stopped_at:
            this_train_stopped = Trains_stopped(self.trainsstopped_counter,
                                                stopped_at,
                                                this_train.unique_num,
                                                current_time_dt,
                                                delayed=False,
                                                delayed_MTA=False)
            if stopped_at not in self.stop_ids:
                this_stop = Stop(stopped_at, 'Unknown')
                self.session.merge(this_stop)
                self.stop_ids.append(stopped_at)
            self.session.merge(this_train_stopped)
            self.trainsstopped_counter += 1

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
            this_stu = Stop_time_update(id=self.stu_counter,
                                        trip_update_id=this_trip.id,
                                        stop_id=stop_id,
                                        arrival_time=arrival_time_dt,
                                        departure_time=departure_time_dt,
                                        scheduled_track=scheduled_track,
                                        actual_track=actual_track,
                                        effective_timestamp=current_time_dt)
            self.session.add(this_stu)
            self.stu_counter += 1

        return leftover_train_uniques

    def _processAlertMessage(self, FeedEntity, current_time_dt):
        '''process any alert messages in the feed. These are always delay messages
        and should always refer to a delayed train.

        Args:
            FeedEntity: AlertMessage FeedEntity (from protobuffer).
            current_time (timestamp): timestamp in seconds since 1970
        '''
        # sometimes there are alert messages without reference to trains.
        # We can't really use those so we will for now ignore them.
        print('found alert message')
        if len(FeedEntity.alert.informed_entity) > 0:
            for tr in FeedEntity.alert.informed_entity:
                # we need to construct the DB ID of the trip update that this
                # alert message belongs to.
                tr_id = tr.trip.trip_id
                train_id = tr.trip.Extensions[
                    nyct_subway_pb2.nyct_trip_descriptor].train_id
                if tr_id in self.trip_origin_date_dict.keys():
                    origin_date = self.trip_origin_date_dict[tr_id]
                else:
                    # fallback to hoping that the current date is right
                    origin_date = current_time_dt
                unique_num = origin_date.strftime('%Y%m%d') + ": " + train_id
                # tr_id is ID in GTFS; trip_id is ID in DB:
                trip_id = unique_num + ": " + tr_id
                print('message refers to trip id: ', trip_id)
                thisupdate = self.session.query(Trip_update)\
                    .filter(Trip_update.id == trip_id).one_or_none()
                if thisupdate:
                    if len(FeedEntity.alert.header_text.translation) > 0:
                        for h in FeedEntity.alert.header_text.translation:
                            header = h.text
                            thisalert = Alert_message(
                                trip_id, header, current_time_dt)
                            print('adding to session...')
                            self.session.add(thisalert)
                else:
                    print('warning: alert message refers '
                          'to non-existent trip update')

    def _processVehicleMessage(self, FeedEntity, current_time_dt):
        train_id = FeedEntity.vehicle.trip.Extensions[
            nyct_subway_pb2.nyct_trip_descriptor].train_id
        origin_date = FeedEntity.vehicle.trip.start_date
        unique_num = origin_date + ": " + train_id
        current_status = FeedEntity.vehicle.current_status
        stop_id = FeedEntity.vehicle.stop_id
        if stop_id == '':
            stop_id = ''
        last_moved_at = FeedEntity.vehicle.timestamp
        last_moved_at = ddatetime.fromtimestamp(last_moved_at)
        last_moved_at = timezone('US/Eastern').localize(last_moved_at)

        current_stop_sequence = FeedEntity.vehicle.current_stop_sequence
        effective_timestamp = current_time_dt
        if stop_id not in self.stop_ids:
            this_stop = Stop(stop_id, 'Unknown')
            self.session.add(this_stop)
            self.stop_ids.append(stop_id)
        vmessage = Vehicle_message(unique_num, current_status,
                                   stop_id, last_moved_at,
                                   current_stop_sequence,
                                   effective_timestamp)
        self.session.add(vmessage)

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
