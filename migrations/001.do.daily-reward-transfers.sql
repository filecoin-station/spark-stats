CREATE TABLE daily_reward_transfers (
    day DATE NOT NULL,
    to_address TEXT NOT NULL,
    amount NUMERIC NOT NULL,
    PRIMARY KEY (day, to_address)
);

CREATE TABLE reward_transfer_last_block (
    last_block INTEGER NOT NULL
);
INSERT INTO reward_transfer_last_block (last_block) VALUES (0);
