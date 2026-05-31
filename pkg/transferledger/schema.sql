-- ADR-0057 / ADR-0065: asset-service MySQL authority.
--
-- This schema is owned by asset-service; no other service writes it.
-- transfer_ledger is the cross-biz_line saga state table; funding_users,
-- funding_accounts, and funding_mutations are the funding wallet authority.
--
-- Deployment note: the DB lives in its own logical database
-- (`opentrade_asset`) so DBA tooling / backup policy can be scoped
-- separately from trade-dump's `opentrade` database.

CREATE DATABASE IF NOT EXISTS opentrade_asset CHARACTER SET utf8mb4;
USE opentrade_asset;

CREATE TABLE IF NOT EXISTS transfer_ledger (
    transfer_id    VARCHAR(64)  PRIMARY KEY,
    user_id        BIGINT UNSIGNED NOT NULL,
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

CREATE TABLE IF NOT EXISTS funding_users (
    user_id          BIGINT UNSIGNED NOT NULL PRIMARY KEY,
    funding_version  BIGINT UNSIGNED NOT NULL DEFAULT 0,
    updated_at_ms    BIGINT          NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS funding_accounts (
    user_id          BIGINT UNSIGNED NOT NULL,
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
    user_id           BIGINT UNSIGNED NOT NULL,
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
