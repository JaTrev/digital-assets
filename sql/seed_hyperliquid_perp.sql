
-- All perpetuals are linear (USDC-margined), quoted in USD, settled in USDC

INSERT INTO instruments (exchange, ticker, base_asset, quote_asset, settle_asset, kind, margin_mode, is_active)
VALUES
    -- ========================================
    -- CRYPTO PERPETUALS (20)
    -- ========================================
    ('hyperliquid', 'BTC', 'BTC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'ETH', 'ETH', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'SOL', 'SOL', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'BNB', 'BNB', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'ARB', 'ARB', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'AVAX', 'AVAX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'ATOM', 'ATOM', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'DOGE', 'DOGE', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'OP', 'OP', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'LTC', 'LTC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'SUI', 'SUI', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'INJ', 'INJ', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'APE', 'APE', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'DYDX', 'DYDX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'XRP', 'XRP', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'ADA', 'ADA', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'DOT', 'DOT', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'LINK', 'LINK', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'UNI', 'UNI', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'MATIC', 'MATIC', 'USD', 'USDC', 'perp', 'linear', TRUE),
        
    -- ========================================
    -- INDEX PERPETUALS (1) - no prefix
    -- ========================================
    ('hyperliquid', 'SPX', 'SPX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    
    -- ========================================
    -- HIP-3 COMMUNITY MARKETS
    -- ========================================
    
    -- xyz DEX Markets
    ('hyperliquid', 'xyz:XYZ100', 'XYZ100', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:GOLD', 'XAU', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:SILVER', 'XAG', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:EUR', 'EUR', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:JPY', 'JPY', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:NVDA', 'NVDA', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:TSLA', 'TSLA', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:AAPL', 'AAPL', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:MSFT', 'MSFT', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:GOOGL', 'GOOGL', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:AMZN', 'AMZN', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:META', 'META', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:NFLX', 'NFLX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:AMD', 'AMD', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:INTC', 'INTC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:COIN', 'COIN', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:MSTR', 'MSTR', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:PLTR', 'PLTR', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'xyz:MU', 'MU', 'USD', 'USDC', 'perp', 'linear', TRUE),
    
    -- cash DEX Markets
    ('hyperliquid', 'cash:USA500', 'SPX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'cash:GOLD', 'XAU', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'cash:SILVER', 'XAG', 'USD', 'USDC', 'perp', 'linear', TRUE),
    
    -- hyna DEX Markets
    ('hyperliquid', 'hyna:BTC', 'BTC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('hyperliquid', 'hyna:ETH', 'ETH', 'USD', 'USDC', 'perp', 'linear', TRUE)
ON CONFLICT (exchange, ticker) DO NOTHING;

