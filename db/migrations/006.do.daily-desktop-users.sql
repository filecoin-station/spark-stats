CREATE TABLE daily_desktop_users (
    day DATE NOT NULL,
    platform TEXT NOT NULL,
    user_count INT NOT NULL,
    PRIMARY KEY (day, platform)
);

CREATE INDEX daily_desktop_users_to_day ON daily_desktop_users (day);
