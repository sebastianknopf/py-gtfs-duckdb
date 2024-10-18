CREATE TABLE IF NOT EXISTS trips
  (
	 route_id				TEXT NOT NULL,
	 service_id				TEXT NOT NULL,
	 trip_id				TEXT NOT NULL,
	 trip_headsign			TEXT,
	 trip_short_name		TEXT,
	 direction_id			TEXT,
	 block_id				TEXT,
	 shape_id				TEXT,
	 wheelchair_accessible	TEXT,
	 bikes_allowed			TEXT,
	 PRIMARY KEY ( trip_id )
  ) 
