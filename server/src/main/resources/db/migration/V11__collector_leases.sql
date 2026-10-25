CREATE TABLE collector_plan (
    singleton smallint PRIMARY KEY CHECK (singleton=1),
    version varchar(64) NOT NULL,
    source_mode varchar(32) NOT NULL CHECK (source_mode IN ('SYNTHETIC','UNOFFICIAL_POC')),
    discovered integer NOT NULL CHECK (discovered>=0),
    eligible integer NOT NULL CHECK (eligible>=0),
    selected integer NOT NULL CHECK (selected BETWEEN 0 AND 20),
    valid_until timestamptz NOT NULL
);
INSERT INTO collector_plan VALUES (1,'UNCONFIGURED','SYNTHETIC',0,0,0,'epoch');
CREATE TABLE collector_worker (
    worker_id uuid PRIMARY KEY,
    capacity integer NOT NULL CHECK (capacity BETWEEN 1 AND 20),
    valid_until timestamptz NOT NULL,
    state varchar(32) NOT NULL CHECK (state IN ('ACTIVE','STOPPED'))
);
CREATE TABLE collector_lease (
    channel_id varchar(32) PRIMARY KEY CHECK (channel_id ~ '^[0-9a-f]{32}$'),
    policy_version varchar(64) NOT NULL,
    selected boolean NOT NULL,
    owner uuid,
    epoch bigint NOT NULL DEFAULT 0 CHECK (epoch>=0),
    valid_until timestamptz NOT NULL DEFAULT 'epoch',
    connected boolean NOT NULL DEFAULT false,
    retry_after timestamptz NOT NULL DEFAULT 'epoch'
);
CREATE INDEX collector_lease_owner ON collector_lease(owner);
