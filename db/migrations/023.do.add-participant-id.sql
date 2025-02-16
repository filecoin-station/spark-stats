-- Add participant_id columns to both tables
ALTER TABLE daily_scheduled_rewards ADD COLUMN participant_id INTEGER;
ALTER TABLE daily_reward_transfers ADD COLUMN participant_id INTEGER;

-- Backfill participant_id in daily_scheduled_rewards based on participant_address
WITH address_mapping AS (
  SELECT DISTINCT participant_address AS address,
         ROW_NUMBER() OVER (ORDER BY participant_address) AS generated_id
  FROM daily_scheduled_rewards
)
UPDATE daily_scheduled_rewards dsr
SET participant_id = am.generated_id
FROM address_mapping am
WHERE dsr.participant_address = am.address;

-- Backfill participant_id in daily_reward_transfers using the same mapping
WITH address_mapping AS (
  SELECT DISTINCT participant_address AS address,
         ROW_NUMBER() OVER (ORDER BY participant_address) AS generated_id
  FROM daily_scheduled_rewards
)
UPDATE daily_reward_transfers drt
SET participant_id = am.generated_id
FROM address_mapping am
WHERE drt.to_address = am.address;

-- Create indexes for better performance
CREATE INDEX idx_daily_scheduled_rewards_pid_day ON daily_scheduled_rewards (participant_id, day DESC);
CREATE INDEX idx_daily_reward_transfers_pid_day ON daily_reward_transfers (participant_id, day);

