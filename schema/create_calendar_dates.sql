CREATE TABLE IF NOT EXISTS calendar_dates
  (
	 service_id					TEXT NOT NULL,
	 date						INTEGER NOT NULL,
	 exception_type				INTEGER NOT NULL,
	 PRIMARY KEY ( service_id, date )
  )
