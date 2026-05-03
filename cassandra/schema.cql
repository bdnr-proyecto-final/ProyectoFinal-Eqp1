CREATE KEYSPACE IF NOT EXISTS wikimedia
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE wikimedia;

-- Tabla legacy temporal para no romper los consumers actuales.
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

-- Tabla raw orientada al stream de Wikimedia.
-- Diseno pensado para consultas por fecha + wiki + hora.
CREATE TABLE IF NOT EXISTS recent_changes_raw (
    event_date DATE,
    wiki TEXT,
    event_hour INT,
    timestamp_event TIMESTAMP,
    source_event_id TEXT,
    title TEXT,
    user_name TEXT,
    change_type TEXT,
    bot BOOLEAN,
    server_name TEXT,
    comment TEXT,
    raw_json TEXT,
    PRIMARY KEY ((event_date, wiki, event_hour), timestamp_event, source_event_id)
) WITH CLUSTERING ORDER BY (timestamp_event DESC, source_event_id ASC);

-- Tabla analitica agregada por wiki y hora.
CREATE TABLE IF NOT EXISTS changes_by_wiki_hour (
    event_date DATE,
    wiki TEXT,
    event_hour INT,
    change_type TEXT,
    total_events BIGINT,
    bot_events BIGINT,
    PRIMARY KEY ((event_date, wiki), event_hour, change_type)
) WITH CLUSTERING ORDER BY (event_hour ASC, change_type ASC);