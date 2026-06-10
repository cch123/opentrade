-- OpenTrade perp symbol catalog — ADR-0075.
--
-- MySQL is the authoritative store for perp contract specs and their
-- versioned trading parameters. Services (perp-counter / match / perp-pricing
-- / bff) never write here; they poll the perp_catalog_version anchor (~1s,
-- ADR-0056 pattern) and reload into a local read-only cache, swapping each
-- symbol atomically. admin-gateway is the only writer; every publish bumps
-- the anchor inside the same transaction.
--
-- config_version is symbol-scoped and strictly monotonic. A published row is
-- immutable: parameter changes, status transitions, and rollbacks all publish
-- a NEW version (rollback rows record source_version). effective_from_ms
-- schedules future activation — services derive the ACTIVE version as the
-- highest version with effective_from_ms <= now, so the switch needs no
-- writer at the boundary (ADR-0053 ScheduledChange generalized).

-- Stable contract spec, plus the latest-PUBLISHED status / version pointers.
-- The pointers are an ops listing convenience only: with a future-dated
-- publish they run ahead of what is active, so services must never read them
-- for admission — admission state comes from perp_symbol_configs.
CREATE TABLE IF NOT EXISTS perp_symbols (
    symbol         VARCHAR(32)     NOT NULL,
    contract_type  VARCHAR(16)     NOT NULL,            -- LINEAR_PERP (others reserved, ADR-0076)
    base_asset     VARCHAR(16)     NOT NULL,
    quote_asset    VARCHAR(16)     NOT NULL,
    settle_asset   VARCHAR(16)     NOT NULL,
    contract_size  DECIMAL(36, 18) NOT NULL DEFAULT 1,
    price_scale    INT             NOT NULL DEFAULT 2,
    qty_scale      INT             NOT NULL DEFAULT 3,
    alias          VARCHAR(64)     NOT NULL DEFAULT '',
    status         VARCHAR(24)     NOT NULL,            -- latest published status (ops view)
    config_version BIGINT UNSIGNED NOT NULL,            -- latest published version (ops view)
    created_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    updated_at     DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (symbol)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- One row per published version. Append-only; rows are never updated or
-- deleted (audit, staged-risk pinned lookups, and historical replay all
-- depend on version history staying intact).
CREATE TABLE IF NOT EXISTS perp_symbol_configs (
    symbol                VARCHAR(32)     NOT NULL,
    config_version        BIGINT UNSIGNED NOT NULL,
    status                VARCHAR(24)     NOT NULL,
    precision_json        JSON            NOT NULL,
    order_limits_json     JSON            NOT NULL,
    risk_tiers_json       JSON            NOT NULL,
    funding_json          JSON            NOT NULL,
    pricing_json          JSON            NOT NULL,
    fees_json             JSON            NOT NULL,
    price_protection_json JSON            NOT NULL,
    risk_apply            VARCHAR(12)     NOT NULL DEFAULT 'STAGED', -- STAGED | IMMEDIATE (ADR-0075 §3)
    reprice_policy_json   JSON            NULL,                      -- required for IMMEDIATE tightening
    effective_from_ms     BIGINT          NOT NULL DEFAULT 0,
    created_by            VARCHAR(64)     NOT NULL DEFAULT '',
    reason                VARCHAR(256)    NOT NULL DEFAULT '',
    source_version        BIGINT UNSIGNED NOT NULL DEFAULT 0,        -- non-zero on rollback copies
    created_at            DATETIME(3)     NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
    PRIMARY KEY (symbol, config_version)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- Single-row poll anchor (ADR-0056 pattern): bumped in the same transaction
-- as every catalog write, so a reader's 1-row poll detects any change.
CREATE TABLE IF NOT EXISTS perp_catalog_version (
    id         TINYINT UNSIGNED NOT NULL,
    version    BIGINT UNSIGNED  NOT NULL DEFAULT 0,
    updated_at DATETIME(3)      NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

INSERT INTO perp_catalog_version (id, version) VALUES (1, 0)
ON DUPLICATE KEY UPDATE id = id;
