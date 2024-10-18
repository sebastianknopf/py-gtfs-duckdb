CREATE TABLE IF NOT EXISTS transfers
  (
	 from_stop_id				TEXT,
	 to_stop_id					TEXT,
	 from_route_id				TEXT,
	 to_route_id				TEXT,
	 from_trip_id				TEXT,
	 to_trip_id					TEXT,
	 transfer_type				TEXT NOT NULL,
	 min_transfer_time			TEXT
  )
