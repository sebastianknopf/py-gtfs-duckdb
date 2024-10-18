CREATE TABLE IF NOT EXISTS shapes
  (
     shape_id            TEXT NOT NULL,
     shape_pt_lat        FLOAT NOT NULL,
     shape_pt_lon        FLOAT NOT NULL,
     shape_pt_sequence   INTEGER NOT NULL,
     shape_dist_traveled FLOAT
  )
