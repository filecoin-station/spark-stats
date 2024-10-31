CREATE TABLE daily_retrieval_result_status (
  day DATE NOT NULL,
  status TEXT NOT NULL,
  rate NUMERIC NOT NULL,
  PRIMARY KEY (day, status)
);
