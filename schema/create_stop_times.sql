CREATE TABLE IF NOT EXISTS stop_times
  (
	 trip_id						TEXT NOT NULL,
	 arrival_time					TEXT,
	 departure_time					TEXT,
	 stop_id						TEXT,
	 location_group_id				TEXT,
	 location_id					TEXT,
	 stop_sequence					INTEGER NOT NULL,
	 stop_headsign					TEXT,
	 start_pickup_drop_off_window 	TEXT,
	 end_pickup_drop_off_window 	TEXT,
	 pickup_type					TEXT,
	 drop_off_type					TEXT,
	 continuous_pickup				INTEGER,
	 continuous_drop_off			INTEGER,
	 shape_dist_traveled			FLOAT,
	 timepoint						INTEGER,
	 pickup_booking_rule_id			INTEGER,
	 drop_off_booking_rule_id		INTEGER,
	 PRIMARY KEY ( trip_id, stop_id, stop_sequence )
  ) 
