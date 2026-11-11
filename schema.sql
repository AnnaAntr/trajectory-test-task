BEGIN;

CREATE TABLE IF NOT EXISTS customer (
    id BIGSERIAL NOT NULL,
    crm_customer_id BIGINT NOT NULL,
    login TEXT NOT NULL,
    name TEXT,
    jsonb_data JSONB,
    send_methods JSONB,
    last_seen_run_id UUID,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT pk_customer PRIMARY KEY (id),
    CONSTRAINT uq_customer_crm_customer_id UNIQUE (crm_customer_id)
);

CREATE TABLE IF NOT EXISTS lead (
    id BIGSERIAL NOT NULL,
    crm_lead_id BIGINT NOT NULL,
    login TEXT,
    name TEXT,
    customer_id BIGINT,
    jsonb_data JSONB,
    last_seen_run_id UUID,
    created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT now(),
    updated_at TIMESTAMP WITHOUT TIME ZONE,
    CONSTRAINT pk_lead PRIMARY KEY (id),
    CONSTRAINT uq_lead_crm_lead_id UNIQUE (crm_lead_id),
    CONSTRAINT fk_lead_customer_id_customer FOREIGN KEY (customer_id)
        REFERENCES customer (id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_customer_login ON customer (login);
CREATE INDEX IF NOT EXISTS ix_customer_name ON customer (name);
CREATE INDEX IF NOT EXISTS ix_customer_last_seen_run_id ON customer (last_seen_run_id);
CREATE INDEX IF NOT EXISTS ix_lead_login ON lead (login);
CREATE INDEX IF NOT EXISTS ix_lead_name ON lead (name);
CREATE INDEX IF NOT EXISTS ix_lead_customer_id ON lead (customer_id);
CREATE INDEX IF NOT EXISTS ix_lead_last_seen_run_id ON lead (last_seen_run_id);

DROP TABLE IF EXISTS abstract_customer;

COMMIT;
