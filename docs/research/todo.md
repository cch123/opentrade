# Research TODO

- [x] Confirm who is responsible for generating, triggering, and storing snapshots for the dual-output matching service architecture.
  - **Resolved 2026-04-26** in [ADR-0066](../adr/0066-trade-dump-projection-platform-admission.md): Match snapshot stays inside Match (self-produced, periodic + on symbol unload, ADR-0030). Rejected as a trade-dump shadow candidate because `order-event` is a command stream — replaying it would require duplicating the matching engine in trade-dump with no outsourcing benefit, and would couple every matching-rule upgrade to two binaries. The admission rule (state journal vs command stream) makes this a categorical decision, not a per-case judgment. Storage migration to S3/EFS for HA remains possible but is independent of producer ownership.
