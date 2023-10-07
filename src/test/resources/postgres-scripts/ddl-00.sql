CREATE TABLE status (
  "trace_id"               TEXT              NOT NULL,
  "parent_trace_id"        TEXT              NOT NULL,
  "trace_id_failed_chunks" TEXT ARRAY        NOT NULL DEFAULT '{}',
  "status"                 TEXT              NOT NULL,
  "updated_at"             TIMESTAMPTZ       NOT NULL,
  "created_at"             TIMESTAMPTZ               ,
  "entity_version"         TEXT                      ,
  "entity_operation"       TEXT                      ,
  "entity_name"            TEXT                      ,
  "vendor_id"              TEXT                      ,
  CONSTRAINT status_pkey PRIMARY KEY ("trace_id", "created_at")
);

CREATE INDEX status_created_at_idx ON public.status USING btree (created_at DESC);
CREATE INDEX status_status_idx ON public.status USING btree (status) WHERE status IS NOT NULL;
CREATE INDEX status_vendor_id_idx ON public.status USING btree (vendor_id);

-- CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;
-- SELECT create_hypertable('status','created_at', migrate_data => true, chunk_time_interval => INTERVAL '1 day');
