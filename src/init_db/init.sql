CREATE SCHEMA IF NOT EXISTS stg_analytical;


CREATE TABLE IF NOT EXISTS stg_analytical.action_users (
    user_id BIGINT NOT NULL,
    session_id VARCHAR(255) NOT NULL,
    traffic_source VARCHAR(100),
    session_start TIMESTAMP WITH TIME ZONE NOT NULL,
    session_end TIMESTAMP WITH TIME ZONE NOT NULL,
    device VARCHAR(100),
    name_link TEXT, 
    action_type VARCHAR(50),
    purchase_amount DECIMAL,
    CONSTRAINT pk_action_users PRIMARY KEY (user_id, session_id)
);


-- Для пользователя postgres (dev-режим):
GRANT USAGE ON SCHEMA stg_analytical TO postgres;
GRANT SELECT ON ALL TABLES IN SCHEMA stg_analytical TO postgres;

ALTER TABLE stg_analytical.action_users REPLICA IDENTITY FULL;
CREATE PUBLICATION dbz_publication FOR TABLE stg_analytical.action_users;