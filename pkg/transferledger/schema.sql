-- ADR-0057: asset-service saga state authority (transfer_ledger).
--
-- This schema is owned by asset-service; no other service writes it.
-- trade-dump projects a denormalised `transfers` read model into the
-- trade-dump MySQL instance from asset-journal (see ADR-0057 M5) — that
-- projection is a separate table, not this one.
--
-- Deployment note: the DB lives in its own logical database
-- (`opentrade_asset`) so DBA tooling / backup policy can be scoped
-- separately from trade-dump's `opentrade` database.

CREATE DATABASE IF NOT EXISTS opentrade_asset CHARACTER SET utf8mb4;
USE opentrade_asset;

CREATE TABLE IF NOT EXISTS transfer_ledger (
    transfer_id    VARCHAR(64)  PRIMARY KEY,
    user_id        VARCHAR(64)  NOT NULL,
    from_biz       VARCHAR(32)  NOT NULL,
    to_biz         VARCHAR(32)  NOT NULL,
    asset          VARCHAR(16)  NOT NULL,
    amount         VARCHAR(64)  NOT NULL,        -- decimal string
    state          VARCHAR(32)  NOT NULL,        -- saga state machine; values in transferledger.State
    reject_reason  VARCHAR(256) NOT NULL DEFAULT '',
    created_at_ms  BIGINT       NOT NULL,        -- wall clock, ms
    updated_at_ms  BIGINT       NOT NULL,
    INDEX idx_user_created (user_id, created_at_ms),
    INDEX idx_state_updated (state, updated_at_ms)
) ENGINE=InnoDB;
