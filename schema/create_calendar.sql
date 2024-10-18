CREATE TABLE IF NOT EXISTS calendar
  (
	 service_id					TEXT NOT NULL,
	 monday						INTEGER NOT NULL,
	 tuesday					INTEGER NOT NULL,
	 wednesday					INTEGER NOT NULL,
	 thursday					INTEGER NOT NULL,
	 friday						INTEGER NOT NULL,
	 saturday					INTEGER NOT NULL,
	 sunday						INTEGER NOT NULL,
	 start_date 				INTEGER NOT NULL,
	 end_date 					INTEGER NOT NULL,
	 PRIMARY KEY ( service_id )
  )
