CREATE KEYSPACE IF NOT EXISTS wikimedia
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wikimedia;

CREATE TABLE IF NOT EXISTS recent_changes (
    id UUID PRIMARY KEY,
    title TEXT,
    user_name TEXT,
    type TEXT,
    wiki TEXT,
    bot BOOLEAN,
    server_name TEXT,
    comment TEXT,
    timestamp_event TIMESTAMP,
    parsed_date TEXT
);