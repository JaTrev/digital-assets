-- Insert top 20 Binance spot trading pairs (by market cap/volume)
-- All are USDT pairs for consistency

INSERT INTO instruments (exchange, ticker, base_asset, quote_asset, settle_asset, kind, margin_mode, is_active, delisted_at)
VALUES
    -- Top tier coins
    ('binance', 'BTCUSDT', 'BTC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'ETHUSDT', 'ETH', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'BNBUSDT', 'BNB', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'SOLUSDT', 'SOL', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'XRPUSDT', 'XRP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- Large cap altcoins
    ('binance', 'ADAUSDT', 'ADA', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'DOGEUSDT', 'DOGE', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'MATICUSDT', 'MATIC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'DOTUSDT', 'DOT', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'LTCUSDT', 'LTC', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- DeFi & Layer 1/2
    ('binance', 'AVAXUSDT', 'AVAX', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'LINKUSDT', 'LINK', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'ATOMUSDT', 'ATOM', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'UNIUSDT', 'UNI', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'APTUSDT', 'APT', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    
    -- Additional top projects
    ('binance', 'NEARUSDT', 'NEAR', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'ARBUSDT', 'ARB', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'OPUSDT', 'OP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'ICPUSDT', 'ICP', 'USDT', NULL, 'spot', NULL, TRUE, NULL),
    ('binance', 'FILUSDT', 'FIL', 'USDT', NULL, 'spot', NULL, TRUE, NULL)
ON CONFLICT (exchange, ticker) DO NOTHING;
