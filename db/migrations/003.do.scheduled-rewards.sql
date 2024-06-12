CREATE TABLE daily_scheduled_rewards (
  day DATE NOT NULL,
  participant_address TEXT NOT NULL,
  scheduled_rewards NUMERIC NOT NULL,
  PRIMARY KEY (day, participant_address)
);
