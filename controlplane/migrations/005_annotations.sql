-- Labels for changes someone made on purpose, so a chart can say which ones.
--
-- One kind only, and the CHECK is the point. The drift demo makes the dev mock
-- answer as a different model, and the dashboard marks that change simulated by
-- matching it to a row here. If a row could say anything, it could relabel a
-- change a provider really made as one somebody did deliberately -- the mistake a
-- drift view exists to prevent. So the only thing this table can hold is "this
-- change was simulated", and a real change is only ever what telemetry shows with
-- no row to match.
CREATE TABLE IF NOT EXISTS annotations (
  tenant_id    text NOT NULL REFERENCES tenants(id),
  id           bigint GENERATED ALWAYS AS IDENTITY,
  -- Set by the database, never by the caller: when the label was recorded.
  at           timestamptz NOT NULL DEFAULT now(),
  kind         text NOT NULL CHECK (kind IN ('simulated_served_model_change')),
  detail       jsonb NOT NULL,
  principal_id uuid NOT NULL,
  PRIMARY KEY (tenant_id, id)
);

ALTER TABLE annotations ENABLE ROW LEVEL SECURITY;
ALTER TABLE annotations FORCE ROW LEVEL SECURITY;
CREATE POLICY annotations_tenant ON annotations
  USING (tenant_id=current_setting('app.tenant',true))
  WITH CHECK (tenant_id=current_setting('app.tenant',true));

-- Serves the only read: one tenant's annotations inside a window.
CREATE INDEX IF NOT EXISTS annotations_at ON annotations (tenant_id, at);

-- Append-only, like audit: an annotation records something that happened.
GRANT SELECT,INSERT ON annotations TO switchboard_app;
GRANT USAGE ON SEQUENCE annotations_id_seq TO switchboard_app;
