CREATE TABLE IF NOT EXISTS stops
  (
	 stop_id						TEXT NOT NULL,
	 stop_code						TEXT,
	 stop_name						TEXT,
	 tts_stop_name					TEXT,
	 stop_desc						TEXT,
	 stop_lat						FLOAT,
	 stop_lon						FLOAT,
	 zone_id						TEXT,
	 stop_url 						TEXT,
	 location_type 					TEXT,
	 parent_station					TEXT,
	 stop_timezone					TEXT,
	 wheelchair_boarding			TEXT,
	 level_id						TEXT,
	 platform_code					TEXT,
	 PRIMARY KEY ( stop_id )
  ) 
