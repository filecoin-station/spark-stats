CREATE TABLE daily_reward_transfers (
    day DATE NOT NULL,
    to_address TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    last_checked_block INTEGER NOT NULL,
    PRIMARY KEY (day, to_address)
);

CREATE INDEX daily_reward_transfers_day ON daily_reward_transfers (day);
CREATE INDEX daily_reward_transfers_last_block ON daily_reward_transfers (last_checked_block DESC);
