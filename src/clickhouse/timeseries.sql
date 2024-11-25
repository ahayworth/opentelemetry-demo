CREATE DATABASE IF NOT EXISTS otel;
SET allow_experimental_time_series_table = 1;
CREATE TABLE IF NOT EXISTS otel.metrics ENGINE=TimeSeries;
