CREATE TABLE IF NOT EXISTS analytical_agg.data_mart_device (
    load_date Date,
    device String,
    users_amount Int32
) ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, device);

CREATE TABLE IF NOT EXISTS analytical_agg.data_mart_traffic (
    load_date Date,
    traffic_source String,
    users_amount Int32
) ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, traffic_source);

CREATE TABLE IF NOT EXISTS analytical_agg.data_mart_acton (
    load_date Date,
    action_type String,
    users_amount Int32
) ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, action_type);

CREATE TABLE IF NOT EXISTS analytical_agg.data_mart_purchases (
    load_date Date,
    name_product String,
    product_count Int32,
    product_amount Float64
) ENGINE = MergeTree
PARTITION BY toYYYYMM(load_date)
ORDER BY (load_date, name_product);