-- ADR-0057 / ADR-0065: asset-service authoritative MySQL schema.
--
-- This logical database is owned by asset-service. It is separate from the
-- trade-dump/read-model `opentrade` database created in 01-schema.sql.

CREATE DATABASE IF NOT EXISTS opentrade_asset CHARACTER SET utf8mb4;
USE opentrade_asset;

CREATE TABLE IF NOT EXISTS transfer_ledger (
    transfer_id    VARCHAR(64)  PRIMARY KEY,
    user_id        VARCHAR(64)  NOT NULL,
    from_biz       VARCHAR(32)  NOT NULL,
    to_biz         VARCHAR(32)  NOT NULL,
    asset          VARCHAR(16)  NOT NULL,
    amount         VARCHAR(64)  NOT NULL,
    state          VARCHAR(32)  NOT NULL,
    reject_reason  VARCHAR(256) NOT NULL DEFAULT '',
    created_at_ms  BIGINT       NOT NULL,
    updated_at_ms  BIGINT       NOT NULL,
    INDEX idx_user_created (user_id, created_at_ms),
    INDEX idx_state_updated (state, updated_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS funding_users (
    user_id          VARCHAR(64)     PRIMARY KEY,
    funding_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at_ms    BIGINT          NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS funding_accounts (
    user_id          VARCHAR(64)     NOT NULL,
    asset            VARCHAR(16)     NOT NULL,
    available        DECIMAL(36,18)  NOT NULL DEFAULT 0,
    frozen           DECIMAL(36,18)  NOT NULL DEFAULT 0,
    balance_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at_ms    BIGINT          NOT NULL,
    PRIMARY KEY (user_id, asset),
    KEY idx_user (user_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS funding_mutations (
    mutation_id       BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
    transfer_id       VARCHAR(64)     NOT NULL,
    op_type           VARCHAR(32)     NOT NULL,
    user_id           VARCHAR(64)     NOT NULL,
    asset             VARCHAR(16)     NOT NULL,
    amount            DECIMAL(36,18)  NOT NULL,
    peer_biz          VARCHAR(32)     NOT NULL DEFAULT '',
    memo              VARCHAR(256)    NOT NULL DEFAULT '',
    status            VARCHAR(32)     NOT NULL,
    reject_reason     VARCHAR(256)    NOT NULL DEFAULT '',
    available_after   DECIMAL(36,18)  NOT NULL DEFAULT 0,
    frozen_after      DECIMAL(36,18)  NOT NULL DEFAULT 0,
    funding_version   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    balance_version   BIGINT UNSIGNED NOT NULL DEFAULT 0,
    created_at_ms     BIGINT          NOT NULL,
    PRIMARY KEY (mutation_id),
    UNIQUE KEY uk_transfer_op (transfer_id, op_type),
    KEY idx_user_created (user_id, created_at_ms),
    KEY idx_transfer (transfer_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
