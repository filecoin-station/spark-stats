CREATE TABLE daily_retrieval_result_codes (
  day DATE NOT NULL,
  code TEXT NOT NULL,
  rate NUMERIC NOT NULL,
  PRIMARY KEY (day, code)
);
