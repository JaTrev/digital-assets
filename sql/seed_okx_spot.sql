-- Tickers are stored in OKX API format (with hyphen: BTC-USDT)

INSERT INTO instruments (exchange, ticker, base_asset, quote_asset, settle_asset, kind, margin_mode, is_active, delisted_at)
VALUES
    -- Top tier coins
    ('okx', 'BTC-USDT', 'BTC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'ETH-USDT', 'ETH', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'BNB-USDT', 'BNB', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'SOL-USDT', 'SOL', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'XRP-USDT', 'XRP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- Large cap altcoins
    ('okx', 'ADA-USDT', 'ADA', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'DOGE-USDT', 'DOGE', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'MATIC-USDT', 'MATIC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'DOT-USDT', 'DOT', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'LTC-USDT', 'LTC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- DeFi & Layer 1/2
    ('okx', 'AVAX-USDT', 'AVAX', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'LINK-USDT', 'LINK', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'ATOM-USDT', 'ATOM', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'UNI-USDT', 'UNI', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'APT-USDT', 'APT', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- Additional top projects
    ('okx', 'NEAR-USDT', 'NEAR', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'ARB-USDT', 'ARB', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'OP-USDT', 'OP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'ICP-USDT', 'ICP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'FIL-USDT', 'FIL', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'TRX-USDT', 'TRX', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'TON-USDT', 'TON', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('okx', 'SUI-USDT', 'SUI', 'USDT', NULL, 'spot', NULL, TRUE, NULL)
ON CONFLICT (exchange, ticker) DO NOTHING;
