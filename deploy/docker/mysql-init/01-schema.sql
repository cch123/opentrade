-- OpenTrade MySQL schema (MVP-0 skeleton — full tables land in MVP-7)
--
-- Naming convention for monotonic sequence columns: each column carries the
-- name of the *producer* that assigns the value, so a bare column name in a
-- query plan is unambiguous about who owns the monotonicity.
--
--   counter_seq_id     - counter shard-scoped, assigned by counter UserSequencer
--   match_seq_id       - per-symbol, assigned by match SymbolWorker
--   trigger_seq_id - trigger service global, used as upsert guard
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
-- the PK includes `asset` to keep the (vshard, counter_seq_id) → multi-row
-- expansion idempotent.
--
-- ADR-0058 renamed shard_id → vshard_id: Counter now routes by 256
-- virtual shards instead of 10 physical ones. The column is parsed
-- out of EventMeta.producer_id which is "counter-vshard-NNN".
-- writer_node / writer_epoch are audit-only: trade-dump copies them
-- from the Kafka record headers that counter's TxnProducer attaches
-- (writer-node / writer-epoch per ADR-0058 §4). They identify which
-- counter node + which assignment epoch produced the event, which is
-- useful for forensic queries after a failover / migration ("which
-- owner wrote this settlement?") but not part of any client-facing API.
CREATE TABLE IF NOT EXISTS account_logs (
    vshard_id      INT             NOT NULL,
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
    writer_node    VARCHAR(64)     NOT NULL DEFAULT '',
    writer_epoch   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    PRIMARY KEY (vshard_id, counter_seq_id, asset),
    KEY idx_user_ts (user_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- triggers — last-state-wins projection of trigger-event (ADR-0047).
-- trade-dump upserts by PK (id) and guards with last_update_ms (wall-clock
-- emit timestamp; trigger_seq_id is also carried on-wire, but the wall
-- clock is the authoritative ordering for this projection — see below).
-- Type / status stored as TINYINT mirroring rpc/trigger enum values.
CREATE TABLE IF NOT EXISTS triggers (
    id                    BIGINT UNSIGNED NOT NULL,
    client_trigger_id     VARCHAR(64)     NOT NULL DEFAULT '',
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

-- Funding wallet balances + transfer history live in asset-service's
-- own schema (opentrade_asset.funding_accounts / funding_mutations /
-- transfer_ledger, per ADR-0065). trade-dump no longer projects
-- asset-journal, so no funding_accounts / funding_account_logs /
-- transfers tables are kept here.
