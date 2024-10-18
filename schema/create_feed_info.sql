CREATE TABLE IF NOT EXISTS feed_info
  (
     feed_publisher_name TEXT NOT NULL,
     feed_publisher_url  TEXT NOT NULL,
     feed_lang           TEXT NOT NULL,
     default_lang        TEXT,
     feed_start_date     INTEGER,
     feed_end_date       INTEGER,
     feed_version        TEXT,
     feed_contact_email  TEXT,
     feed_contact_url    TEXT
  )
