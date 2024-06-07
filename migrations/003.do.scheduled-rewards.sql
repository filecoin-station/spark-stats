CREATE TABLE daily_scheduled_rewards (
  day DATE NOT NULL,
  address TEXT NOT NULL,
  scheduled_rewards NUMERIC NOT NULL,
  PRIMARY KEY (day, address)
);
