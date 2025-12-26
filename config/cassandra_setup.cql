-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS healthinsight WITH replication = {
  'class': 'NetworkTopologyStrategy',
  'datacenter1': 3
} AND durable_writes = true;

USE healthinsight;

-- Patient monitoring table
CREATE TABLE IF NOT EXISTS patient_monitoring (
    patient_id UUID,
    reading_timestamp TIMESTAMP,
    metric_type TEXT,
    value DOUBLE,
    unit TEXT,
    device_id TEXT,
    is_abnormal BOOLEAN,
    threshold_min DOUBLE,
    threshold_max DOUBLE,
    PRIMARY KEY (patient_id, reading_timestamp, metric_type)
) WITH CLUSTERING ORDER BY (reading_timestamp DESC, metric_type ASC)
  AND gc_grace_seconds = 864000
  AND default_time_to_live = 7776000;

-- Alerts table
CREATE TABLE IF NOT EXISTS alerts (
    alert_id UUID,
    patient_id UUID,
    timestamp TIMESTAMP,
    metric_type TEXT,
    severity TEXT,
    value DOUBLE,
    threshold_violated TEXT,
    acknowledged BOOLEAN,
    PRIMARY KEY (patient_id, timestamp, alert_id)
) WITH CLUSTERING ORDER BY (timestamp DESC);
