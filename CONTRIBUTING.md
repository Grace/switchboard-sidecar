# Contributing

**Contributions are not currently accepted.** This document exists so the
position is settled before that changes, not because there is a queue.

When it does change, the terms below apply.

## Sign-off

Every commit must carry a `Signed-off-by` trailer:

```sh
git commit -s
```

That appends `Signed-off-by: Your Name <you@example.com>` using your git
`user.name` and `user.email`. Use a real name and an address that reaches you.
CI rejects any commit in a pull request without the trailer.

Signing off certifies the [Developer Certificate of Origin 1.1](DCO), reproduced
verbatim in this repository.

The DCO refers to "the open source license indicated in the file," which for
Switchboard is the [Apache License 2.0](LICENSE). The text is reproduced
unmodified: an edited DCO is worth less than the recognised one, and there is
nothing here to reconcile.

That was not always true. Between 2026-09-07 and 2026-09-09 the project was
under the Elastic License 2.0, which is source available rather than
OSI-approved, and this section explained the mismatch. Recorded because a
contributor reading old commits will find sign-offs made under those terms.

## Grant

You keep the copyright in your contribution.

By submitting a contribution, you grant the copyright holder identified in [LICENSE](LICENSE) a perpetual,
worldwide, non-exclusive, irrevocable, royalty-free, sublicensable and
transferable license to use, reproduce, modify, prepare derivative works of,
publicly display, distribute and relicense that contribution, in whole or in
part, **under any license terms and as part of any product**.

That last clause is doing real work and is stated plainly rather than buried:

- Switchboard Sidecar is Apache 2.0 today. This grant permits relicensing it
  later, including under different terms — it has already carried the project
  through MIT, the Elastic License 2.0 and back to Apache.
- Your contribution may be used in the other Switchboard repositories, which is
  what the grant is chiefly for. Every one of them is Apache 2.0 today,
  including Switchboard Recordkeeper.
- **Today's licence is not a promise about tomorrow's.** The grant permits
  relicensing under any terms, and the history above is evidence that it gets
  exercised. If a future product built on this code were not published, this
  grant is what would permit that.

If either is unacceptable to you, do not contribute. That is a reasonable
position and no argument will be made against it.

## Practical rules

- **No real credentials, private signing keys, or Terraform state** ever enter
  this repository. The `.env.example` names variables and their sources with
  every value empty, and the fixture public key in `testdata` is test-only.
- **Your own secret tooling is not a product dependency.** A wrapper that keeps
  values out of your shell or your agent's context is yours; naming it in
  `docs/` tells a reader to run something they do not have. `asm-exec` reached
  `DEPLOYMENT.md` and `SECURITY.md` that way and had to be removed.
- `go test -race ./...` must pass. Postgres tests need a disposable dedicated
  cluster and `TEST_DATABASE_URL`; they create schema and roles.
- CI runs `govulncheck` and `pip-audit`, and pins every GitHub Action by full
  commit SHA. Keep it that way.
- **New dependencies are close to unwelcome.** The module graph is 4 direct and
  12 indirect, every one AWS-published, and the Python requirements are pinned
  direct and transitive. That is a load-bearing product claim, not an accident —
  a PR adding a convenience library will be declined on those grounds alone.
  Prometheus exposition, OTLP export, Ed25519 verification and SSE parsing are
  hand-written against the standard library for this reason.
- Unknown request fields are rejected on purpose. Widening the API surface is a
  design decision, not a patch.

## Not legal advice

This document was drafted without a lawyer. Before the first external
contribution is accepted, it — and the Apache 2.0 relicense it sits on top of —
should be reviewed by one.
