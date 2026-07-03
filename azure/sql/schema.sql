CREATE TABLE machine_status (
    machine_id VARCHAR(20),
    window_end DATETIME2,
    avg_temp FLOAT,
    max_temp FLOAT,
    avg_vibration FLOAT,
    max_vibration FLOAT,
    health_status VARCHAR(10)
);

CREATE TABLE active_alerts (
    alert_id INT IDENTITY PRIMARY KEY,
    machine_id VARCHAR(20),
    triggered_at DATETIME2,
    reason VARCHAR(200),
    resolved BIT DEFAULT 0
);
