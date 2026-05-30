-- OpenTrade perp (USDT-margined linear perpetual) projections — ADR-0068 M7.
--
-- trade-dump projects the perp-journal (perp-counter's WAL) into these tables.
-- perp_seq_id is the perp-counter-shard-scoped monotonic id (ADR-0051), used as
-- the per-row idempotency guard: append-only ledgers INSERT IGNORE on it; state
-- tables upsert latest-wins guarded by perp_seq_id so replays never regress.
--
-- All monetary values are DECIMAL(36, 18) (string at the API boundary).

-- Current position per (user, symbol). Upserted from the PerpPositionSnapshot
-- embedded in settlement / funding / liquidation events. A flat position keeps
-- size 0 (its watermarks live in the perp-counter snapshot, not here).
CREATE TABLE IF NOT EXISTS perp_positions (
    user_id       VARCHAR(64)     NOT NULL,
    symbol        VARCHAR(32)     NOT NULL,
    side          TINYINT         NOT NULL DEFAULT 0,
    size          DECIMAL(36, 18) NOT NULL DEFAULT 0,
    entry_price   DECIMAL(36, 18) NOT NULL DEFAULT 0,
    margin        DECIMAL(36, 18) NOT NULL DEFAULT 0,
    leverage      DECIMAL(36, 18) NOT NULL DEFAULT 0,
    realized_pnl  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    version       BIGINT UNSIGNED NOT NULL DEFAULT 0,
    perp_seq_id   BIGINT UNSIGNED NOT NULL,
    updated_at    DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Per-user futures (USDT) margin wallet. Upserted from PerpMarginEvent
-- after-values.
CREATE TABLE IF NOT EXISTS perp_wallets (
    user_id      VARCHAR(64)     NOT NULL,
    asset        VARCHAR(32)     NOT NULL,
    available    DECIMAL(36, 18) NOT NULL DEFAULT 0,
    reserved     DECIMAL(36, 18) NOT NULL DEFAULT 0,
    perp_seq_id  BIGINT UNSIGNED NOT NULL,
    updated_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (user_id, asset)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Perp order lifecycle, from PerpOrderStatusEvent.
CREATE TABLE IF NOT EXISTS perp_orders (
    order_id      BIGINT UNSIGNED NOT NULL,
    user_id       VARCHAR(64)     NOT NULL,
    symbol        VARCHAR(32)     NOT NULL,
    status        TINYINT         NOT NULL,
    filled_qty    DECIMAL(36, 18) NOT NULL DEFAULT 0,
    reduce_only   TINYINT(1)      NOT NULL DEFAULT 0,
    reject_reason TINYINT         NOT NULL DEFAULT 0,
    updated_at    DATETIME(3)     NOT NULL,
    PRIMARY KEY (order_id),
    KEY idx_user_symbol (user_id, symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Append-only fill ledger (one row per PerpSettlementEvent).
CREATE TABLE IF NOT EXISTS perp_settlements (
    perp_seq_id     BIGINT UNSIGNED NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    order_id        BIGINT UNSIGNED NOT NULL,
    trade_id        VARCHAR(64)     NOT NULL DEFAULT '',
    symbol          VARCHAR(32)     NOT NULL,
    fill_side       TINYINT         NOT NULL,
    price           DECIMAL(36, 18) NOT NULL,
    qty             DECIMAL(36, 18) NOT NULL,
    realized_pnl    DECIMAL(36, 18) NOT NULL DEFAULT 0,
    fee             DECIMAL(36, 18) NOT NULL DEFAULT 0,
    margin_added    DECIMAL(36, 18) NOT NULL DEFAULT 0,
    margin_released DECIMAL(36, 18) NOT NULL DEFAULT 0,
    ts_unix_ms      BIGINT          NOT NULL,
    PRIMARY KEY (user_id, perp_seq_id),
    KEY idx_user_symbol_ts (user_id, symbol, ts_unix_ms),
    KEY idx_trade (trade_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Append-only funding payment ledger (one row per PerpFundingEvent).
CREATE TABLE IF NOT EXISTS perp_funding (
    perp_seq_id      BIGINT UNSIGNED NOT NULL,
    user_id          VARCHAR(64)     NOT NULL,
    symbol           VARCHAR(32)     NOT NULL,
    funding_round_id VARCHAR(96)     NOT NULL,
    funding_rate     DECIMAL(36, 18) NOT NULL DEFAULT 0,
    mark_price       DECIMAL(36, 18) NOT NULL DEFAULT 0,
    payment          DECIMAL(36, 18) NOT NULL DEFAULT 0,
    ts_unix_ms       BIGINT          NOT NULL,
    PRIMARY KEY (user_id, perp_seq_id),
    KEY idx_user_symbol_ts (user_id, symbol, ts_unix_ms),
    KEY idx_round (funding_round_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Append-only liquidation ledger (one row per PerpLiquidationEvent fill).
CREATE TABLE IF NOT EXISTS perp_liquidations (
    perp_seq_id      BIGINT UNSIGNED NOT NULL,
    user_id          VARCHAR(64)     NOT NULL,
    symbol           VARCHAR(32)     NOT NULL,
    liq_order_id     BIGINT UNSIGNED NOT NULL,
    bankruptcy_price DECIMAL(36, 18) NOT NULL DEFAULT 0,
    mark_price       DECIMAL(36, 18) NOT NULL DEFAULT 0,
    closed_qty       DECIMAL(36, 18) NOT NULL DEFAULT 0,
    realized_pnl     DECIMAL(36, 18) NOT NULL DEFAULT 0,
    insurance_delta  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    adl_queued       TINYINT(1)      NOT NULL DEFAULT 0,
    ts_unix_ms       BIGINT          NOT NULL,
    PRIMARY KEY (user_id, perp_seq_id),
    KEY idx_user_symbol_ts (user_id, symbol, ts_unix_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Append-only futures-wallet balance change ledger (one row per
-- PerpMarginEvent: transfer in/out, IM reserve/release).
CREATE TABLE IF NOT EXISTS perp_margin_logs (
    perp_seq_id     BIGINT UNSIGNED NOT NULL,
    user_id         VARCHAR(64)     NOT NULL,
    kind            TINYINT         NOT NULL,
    asset           VARCHAR(32)     NOT NULL,
    amount          DECIMAL(36, 18) NOT NULL DEFAULT 0,
    available_after DECIMAL(36, 18) NOT NULL DEFAULT 0,
    reserved_after  DECIMAL(36, 18) NOT NULL DEFAULT 0,
    ref_id          VARCHAR(96)     NOT NULL DEFAULT '',
    ts_unix_ms      BIGINT          NOT NULL,
    PRIMARY KEY (user_id, perp_seq_id),
    KEY idx_ref (ref_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
