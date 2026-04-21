-- OpenTrade MySQL schema (MVP-0 skeleton — full tables land in MVP-7)
--
-- Naming convention for monotonic sequence columns: each column carries the
-- name of the *producer* that assigns the value, so a bare column name in a
-- query plan is unambiguous about who owns the monotonicity.
--
--   counter_seq_id     - counter shard-scoped, assigned by counter UserSequencer
--   match_seq_id       - per-symbol, assigned by match SymbolWorker
--   conditional_seq_id - conditional service global, used as upsert guard
--
-- All monetary values are stored as DECIMAL(36, 18) (string at the API boundary).

-- account_version / balance_version are the ADR-0048 backlog "双层 version"
-- counters (方案 B): account-level bumps on any of the user's balance
-- mutations, balance-level bumps only on this (user, asset) mutation.
-- All three counters are guarded by counter_seq_id on upsert so replays
-- don't rewind them.
CREATE TABLE IF NOT EXISTS accounts (
    user_id          VARCHAR(64)     NOT NULL,
    asset            VARCHAR(32)     NOT NULL,
    available        DECIMAL(36, 18) NOT NULL DEFAULT 0,
    frozen           DECIMAL(36, 18) NOT NULL DEFAULT 0,
    counter_seq_id   BIGINT UNSIGNED NOT NULL,
    account_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    balance_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at       DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, asset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS orders (
    order_id        BIGINT UNSIGNED NOT NULL,
    client_order_id VARCHAR(64)     NOT NULL DEFAULT '',
    user_id         VARCHAR(64)     NOT NULL,
    symbol          VARCHAR(32)     NOT NULL,
    side            TINYINT         NOT NULL,
    order_type      TINYINT         NOT NULL,
    tif             TINYINT         NOT NULL,
    price           DECIMAL(36, 18) NOT NULL DEFAULT 0,
    qty             DECIMAL(36, 18) NOT NULL,
    filled_qty      DECIMAL(36, 18) NOT NULL DEFAULT 0,
    frozen_amt      DECIMAL(36, 18) NOT NULL DEFAULT 0,
    status          TINYINT         NOT NULL,
    reject_reason   TINYINT         NOT NULL DEFAULT 0,
    created_at      DATETIME(3)     NOT NULL,
    updated_at      DATETIME(3)     NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_user_ctime (user_id, created_at),
    KEY idx_symbol_ctime (symbol, created_at)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS trades (
    trade_id        VARCHAR(64)     NOT NULL,
    symbol          VARCHAR(32)     NOT NULL,
    price           DECIMAL(36, 18) NOT NULL,
    qty             DECIMAL(36, 18) NOT NULL,
    maker_user_id   VARCHAR(64)     NOT NULL,
    maker_order_id  BIGINT UNSIGNED NOT NULL,
    taker_user_id   VARCHAR(64)     NOT NULL,
    taker_order_id  BIGINT UNSIGNED NOT NULL,
    taker_side      TINYINT         NOT NULL,
    ts              BIGINT          NOT NULL,
    match_seq_id    BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (trade_id),
    UNIQUE KEY uk_symbol_match_seq (symbol, match_seq_id),
    KEY idx_symbol_ts (symbol, ts),
    KEY idx_user (maker_user_id, ts),
    KEY idx_user2 (taker_user_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- account_logs — per-asset journal mirror. One CounterJournalEvent may
-- generate multiple rows (SettlementEvent touches both base and quote), so
-- the PK includes `asset` to keep the (shard, counter_seq_id) → multi-row
-- expansion idempotent.
CREATE TABLE IF NOT EXISTS account_logs (
    shard_id       INT             NOT NULL,
    counter_seq_id BIGINT UNSIGNED NOT NULL,
    asset          VARCHAR(32)     NOT NULL,
    user_id        VARCHAR(64)     NOT NULL,
    delta_avail    DECIMAL(36, 18) NOT NULL,
    delta_frozen   DECIMAL(36, 18) NOT NULL,
    avail_after    DECIMAL(36, 18) NOT NULL,
    frozen_after   DECIMAL(36, 18) NOT NULL,
    biz_type       VARCHAR(32)     NOT NULL,
    biz_ref_id     VARCHAR(128)    NOT NULL DEFAULT '',
    ts             BIGINT          NOT NULL,
    PRIMARY KEY (shard_id, counter_seq_id, asset),
    KEY idx_user_ts (user_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- conditionals — last-state-wins projection of conditional-event (ADR-0047).
-- trade-dump upserts by PK (id) and guards with last_update_ms (wall-clock
-- emit timestamp; conditional_seq_id is also carried on-wire, but the wall
-- clock is the authoritative ordering for this projection — see below).
-- Type / status stored as TINYINT mirroring rpc/conditional enum values.
CREATE TABLE IF NOT EXISTS conditionals (
    id                    BIGINT UNSIGNED NOT NULL,
    client_conditional_id VARCHAR(64)     NOT NULL DEFAULT '',
    user_id               VARCHAR(64)     NOT NULL,
    symbol                VARCHAR(32)     NOT NULL,
    side                  TINYINT         NOT NULL,
    type                  TINYINT         NOT NULL,
    stop_price            DECIMAL(36, 18) NOT NULL DEFAULT 0,
    limit_price           DECIMAL(36, 18) NOT NULL DEFAULT 0,
    qty                   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    quote_qty             DECIMAL(36, 18) NOT NULL DEFAULT 0,
    tif                   TINYINT         NOT NULL DEFAULT 0,
    status                TINYINT         NOT NULL,
    triggered_order_id    BIGINT UNSIGNED NOT NULL DEFAULT 0,
    reject_reason         VARCHAR(128)    NOT NULL DEFAULT '',
    expires_at_unix_ms    BIGINT          NOT NULL DEFAULT 0,
    oco_group_id          VARCHAR(64)     NOT NULL DEFAULT '',
    trailing_delta_bps    INT             NOT NULL DEFAULT 0,
    activation_price      DECIMAL(36, 18) NOT NULL DEFAULT 0,
    trailing_watermark    DECIMAL(36, 18) NOT NULL DEFAULT 0,
    trailing_active       TINYINT         NOT NULL DEFAULT 0,
    created_at_unix_ms    BIGINT          NOT NULL,
    triggered_at_unix_ms  BIGINT          NOT NULL DEFAULT 0,
    -- last_update_ms is the wall-clock ms stamped at event emission; used by
    -- trade-dump as the out-of-order / HA-duplicate guard so an older event
    -- never overwrites newer state (see ADR-0047 §Guard).
    last_update_ms        BIGINT          NOT NULL,
    updated_at            DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id),
    KEY idx_user_ctime (user_id, created_at_unix_ms),
    KEY idx_user_symbol_ctime (user_id, symbol, created_at_unix_ms),
    KEY idx_oco_group (oco_group_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- funding_accounts — current funding-wallet balances projected from
-- asset-journal FundingTransferOut / FundingTransferIn / FundingCompensate
-- events (ADR-0057 M5). trade-dump upserts by (user_id, asset) with
-- last-write-wins guarded by asset_seq_id so older events can't
-- regress fresher balances during HA / replay. Structurally mirrors
-- `accounts`, but scoped to biz_line=funding.
--
-- Authoritative live read: asset-service.QueryFundingBalance RPC. This
-- projection is the historical / BFF-cache view and may lag by at most
-- one trade-dump batch.
CREATE TABLE IF NOT EXISTS funding_accounts (
    user_id         VARCHAR(64)     NOT NULL,
    asset           VARCHAR(16)     NOT NULL,
    available       DECIMAL(36, 18) NOT NULL DEFAULT 0,
    frozen          DECIMAL(36, 18) NOT NULL DEFAULT 0,
    -- asset_seq_id is asset-service's typed monotonic sequence
    -- (ADR-0051). Used as the out-of-order / duplicate guard during
    -- upsert.
    asset_seq_id    BIGINT UNSIGNED NOT NULL,
    -- funding_version is the user-level monotonic bumper (mirrors
    -- `accounts.account_version`; ADR-0057 §6).
    funding_version BIGINT UNSIGNED NOT NULL,
    -- balance_version is the per-(user, asset) monotonic bumper
    -- (mirrors `accounts.balance_version`; ADR-0048).
    balance_version BIGINT UNSIGNED NOT NULL,
    updated_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, asset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- funding_account_logs — per-event journal of funding-wallet balance
-- movements. One row per FundingTransferOut / FundingTransferIn /
-- FundingCompensate event. Append-only (ON DUPLICATE KEY UPDATE no-op
-- so replays are idempotent).
--
-- asset_seq_id alone is unique across funding events (asset-service is
-- single-instance); the composite (asset_seq_id, asset) keeps room for
-- future multi-asset-per-event payloads if needed.
CREATE TABLE IF NOT EXISTS funding_account_logs (
    asset_seq_id  BIGINT UNSIGNED NOT NULL,
    asset         VARCHAR(16)     NOT NULL,
    user_id       VARCHAR(64)     NOT NULL,
    delta_avail   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    delta_frozen  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    avail_after   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    frozen_after  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    -- biz_type values: "transfer_out" / "transfer_in" / "compensate".
    biz_type      VARCHAR(32)     NOT NULL,
    -- biz_ref_id carries the saga transfer_id for cross-referencing
    -- with the transfers projection.
    biz_ref_id    VARCHAR(64)     NOT NULL DEFAULT '',
    -- peer_biz is the counterparty biz_line ("spot" / "futures" / ...).
    peer_biz      VARCHAR(32)     NOT NULL DEFAULT '',
    ts            BIGINT          NOT NULL,
    PRIMARY KEY (asset_seq_id, asset),
    KEY idx_user_ts (user_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- transfers — last-state-wins projection of asset-journal's
-- SagaStateChange events (ADR-0057 M5). trade-dump upserts by
-- (transfer_id) with last-write-wins guarded by asset_seq_id so older
-- events can't overwrite newer state during HA handover.
--
-- Live / authoritative read path for in-flight sagas: asset-service
-- QueryTransfer RPC (→ opentrade_asset.transfer_ledger, managed by
-- pkg/transferledger/schema.sql). This table is the historical browsing
-- view behind `GET /v1/transfers`.
CREATE TABLE IF NOT EXISTS transfers (
    transfer_id     VARCHAR(64)     NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    from_biz        VARCHAR(32)     NOT NULL,
    to_biz          VARCHAR(32)     NOT NULL,
    asset           VARCHAR(16)     NOT NULL,
    amount          DECIMAL(36, 18) NOT NULL,
    -- state is the raw transferledger.State string ("INIT" / "DEBITED" /
    -- "COMPLETED" / "FAILED" / "COMPENSATING" / "COMPENSATED" /
    -- "COMPENSATE_STUCK"). See pkg/transferledger/ledger.go for the
    -- authoritative values.
    state           VARCHAR(32)     NOT NULL,
    reject_reason   VARCHAR(256)    NOT NULL DEFAULT '',
    -- asset_seq_id is the asset-service's typed monotonic sequence
    -- (ADR-0051) carried on every asset-journal envelope. trade-dump
    -- uses it as the out-of-order / HA-duplicate guard during upsert.
    asset_seq_id    BIGINT UNSIGNED NOT NULL,
    created_at_ms   BIGINT          NOT NULL,
    updated_at_ms   BIGINT          NOT NULL,
    updated_at      DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (transfer_id),
    KEY idx_user_ctime (user_id, created_at_ms),
    KEY idx_user_state_ctime (user_id, state, created_at_ms),
    KEY idx_user_asset_ctime (user_id, asset, created_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
