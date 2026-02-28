-- Insert Bybit spot trading pairs that have kline data available

INSERT INTO instruments (exchange, ticker, base_asset, quote_asset, settle_asset, kind, margin_mode, is_active, delisted_at)
VALUES
    -- Top tier coins
    ('bybit', 'BTCUSDT', 'BTC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'ETHUSDT', 'ETH', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'BNBUSDT', 'BNB', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'SOLUSDT', 'SOL', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'XRPUSDT', 'XRP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- Large cap
    ('bybit', 'ADAUSDT', 'ADA', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'DOGEUSDT', 'DOGE', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'MATICUSDT', 'MATIC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'DOTUSDT', 'DOT', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'LTCUSDT', 'LTC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- DeFi & Layer 1/2
    ('bybit', 'AVAXUSDT', 'AVAX', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'LINKUSDT', 'LINK', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'ATOMUSDT', 'ATOM', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'UNIUSDT', 'UNI', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'APTUSDT', 'APT', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'NEARUSDT', 'NEAR', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'ARBUSDT', 'ARB', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'OPUSDT', 'OP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'ICPUSDT', 'ICP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'FILUSDT', 'FIL', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'TONUSDT', 'TON', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'WLDUSDT', 'WLD', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('bybit', 'SUIUSDT', 'SUI', 'USDT', NULL, 'spot', NULL, TRUE, NULL)
ON CONFLICT (exchange, ticker) DO NOTHING;