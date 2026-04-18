-- OpenTrade MySQL schema (MVP-0 skeleton — full tables land in MVP-7)
--
-- Naming:
--   seq_id = monotonic event id attached to the source Kafka event
--   All monetary values stored as DECIMAL(36, 18) (string at the API boundary)

-- account_version / balance_version are the ADR-0048 backlog "双层 version"
-- counters (方案 B): account-level bumps on any of the user's balance
-- mutations, balance-level bumps only on this (user, asset) mutation.
-- Both are guarded by seq_id on upsert so replays don't rewind them.
CREATE TABLE IF NOT EXISTS accounts (
    user_id          VARCHAR(64)     NOT NULL,
    asset            VARCHAR(32)     NOT NULL,
    available        DECIMAL(36, 18) NOT NULL DEFAULT 0,
    frozen           DECIMAL(36, 18) NOT NULL DEFAULT 0,
    seq_id           BIGINT UNSIGNED NOT NULL,
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
    symbol_seq_id   BIGINT UNSIGNED NOT NULL,
    PRIMARY KEY (trade_id),
    UNIQUE KEY uk_symbol_seq (symbol, symbol_seq_id),
    KEY idx_symbol_ts (symbol, ts),
    KEY idx_user (maker_user_id, ts),
    KEY idx_user2 (taker_user_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- account_logs — per-asset journal mirror. One CounterJournalEvent may
-- generate multiple rows (SettlementEvent touches both base and quote), so
-- the PK includes `asset` to keep the (shard, seq) → multi-row expansion
-- idempotent.
CREATE TABLE IF NOT EXISTS account_logs (
    shard_id     INT             NOT NULL,
    seq_id       BIGINT UNSIGNED NOT NULL,
    asset        VARCHAR(32)     NOT NULL,
    user_id      VARCHAR(64)     NOT NULL,
    delta_avail  DECIMAL(36, 18) NOT NULL,
    delta_frozen DECIMAL(36, 18) NOT NULL,
    avail_after  DECIMAL(36, 18) NOT NULL,
    frozen_after DECIMAL(36, 18) NOT NULL,
    biz_type     VARCHAR(32)     NOT NULL,
    biz_ref_id   VARCHAR(128)    NOT NULL DEFAULT '',
    ts           BIGINT          NOT NULL,
    PRIMARY KEY (shard_id, seq_id, asset),
    KEY idx_user_ts (user_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- conditionals — last-state-wins projection of conditional-event (ADR-0047).
-- trade-dump upserts by PK (id) and guards with seq_id so out-of-order /
-- duplicate replays from HA handover never overwrite fresher state.
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
