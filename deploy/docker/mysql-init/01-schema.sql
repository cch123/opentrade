-- OpenTrade MySQL schema (MVP-0 skeleton — full tables land in MVP-7)
--
-- Naming:
--   seq_id = monotonic event id attached to the source Kafka event
--   All monetary values stored as DECIMAL(36, 18) (string at the API boundary)

CREATE TABLE IF NOT EXISTS accounts (
    user_id      VARCHAR(64)     NOT NULL,
    asset        VARCHAR(32)     NOT NULL,
    available    DECIMAL(36, 18) NOT NULL DEFAULT 0,
    frozen       DECIMAL(36, 18) NOT NULL DEFAULT 0,
    seq_id       BIGINT UNSIGNED NOT NULL,
    updated_at   DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
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

CREATE TABLE IF NOT EXISTS account_logs (
    shard_id     INT             NOT NULL,
    seq_id       BIGINT UNSIGNED NOT NULL,
    user_id      VARCHAR(64)     NOT NULL,
    asset        VARCHAR(32)     NOT NULL,
    delta_avail  DECIMAL(36, 18) NOT NULL,
    delta_frozen DECIMAL(36, 18) NOT NULL,
    avail_after  DECIMAL(36, 18) NOT NULL,
    frozen_after DECIMAL(36, 18) NOT NULL,
    biz_type     VARCHAR(32)     NOT NULL,
    biz_ref_id   VARCHAR(128)    NOT NULL DEFAULT '',
    ts           BIGINT          NOT NULL,
    PRIMARY KEY (shard_id, seq_id),
    KEY idx_user_ts (user_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
