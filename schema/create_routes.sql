CREATE TABLE IF NOT EXISTS routes
  (
     agency_id 				TEXT NOT NULL,
	 route_id				TEXT,
	 route_short_name		TEXT,
	 route_long_name		TEXT,
	 route_desc				TEXT,
	 route_type				INTEGER NOT NULL,
	 route_url				TEXT,
	 route_color			TEXT,
	 route_text_color		TEXT,
	 route_sort_order		INTEGER,
	 continuous_pickup		INTEGER,
	 continuous_drop_off	INTEGER,
	 network_id				TEXT,
	 PRIMARY KEY ( route_id )
  ) 
