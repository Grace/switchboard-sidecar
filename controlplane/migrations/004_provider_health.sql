-- Circuit state, shared across a tenant's fleet.
--
-- A gateway learns a provider is unhealthy by paying for it: three failures
-- before the breaker opens, per process. Run four gateways and each pays the
-- same three failures to learn the same outage, and a tenant with an autoscaled
-- fleet pays it again on every new instance for as long as the outage lasts.
--
-- What is shared is deliberately narrow, because the fault classes do not mean
-- the same thing. An account fault -- no credits, quota withdrawn -- is true for
-- every gateway holding that tenant's key, and is precisely what an external
-- status page cannot see. A degraded fault is plausibly global but might be one
-- host's network, so it needs corroboration. Refused and terminal are never
-- shared at all: a 401 is one deployment's wrong key and a 400 is one caller's
-- bad request, and broadcasting either would take down a provider that is
-- working fine for everyone else.
--
-- Rows are advisory and short-lived. A gateway that cannot reach the control
-- plane simply stops hearing, its hints expire within one poll interval, and it
-- is back to deciding alone -- which is the behaviour it had before this table
-- existed. Nothing here is on the path of a request.
CREATE TABLE IF NOT EXISTS provider_health (
  tenant_id   text NOT NULL REFERENCES tenants(id),
  -- Which gateway said so. Random per process and never persisted: it exists
  -- only to count how many distinct instances agree inside one window, so that
  -- a single sick host cannot speak for the fleet.
  instance_id text NOT NULL,
  provider    text NOT NULL,
  -- account or degraded. Constrained here rather than in the application alone,
  -- because the value of this table is entirely in what it refuses to carry.
  fault       text NOT NULL CHECK (fault IN ('account','degraded')),
  reported_at timestamptz NOT NULL DEFAULT now(),
  -- Set by the reporter from its own cooldown, and bounded by the reader.
  expires_at  timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, instance_id, provider)
);

ALTER TABLE provider_health ENABLE ROW LEVEL SECURITY;
ALTER TABLE provider_health FORCE ROW LEVEL SECURITY;
CREATE POLICY provider_health_tenant ON provider_health
  USING (tenant_id=current_setting('app.tenant',true))
  WITH CHECK (tenant_id=current_setting('app.tenant',true));

-- Serves the only read: live reports for one tenant, counted by provider.
CREATE INDEX IF NOT EXISTS provider_health_live
  ON provider_health (tenant_id, provider, expires_at DESC);

-- UPDATE and DELETE, unlike the append-only tables above. A report is a current
-- statement rather than a record: the same instance overwrites its own row each
-- poll, and expired rows are removed rather than accumulating. There is nothing
-- here worth keeping once it has expired, and an audit trail of transient
-- circuit state would be a retention problem in exchange for nothing.
GRANT SELECT,INSERT,UPDATE,DELETE ON provider_health TO switchboard_app;
