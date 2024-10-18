CREATE TABLE IF NOT EXISTS agency
  (
     agency_id                  TEXT NOT NULL,
         agency_name            TEXT NOT NULL,
         agency_url             TEXT NOT NULL,
         agency_timezone        TEXT NOT NULL,
         agency_lang            TEXT,
         agency_phone           TEXT,
         agency_fare_url        TEXT,
         agency_email           TEXT,
         PRIMARY KEY ( agency_id )
  )
