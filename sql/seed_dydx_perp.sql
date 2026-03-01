-- Seed dYdX perpetual futures contracts

INSERT INTO instruments (exchange, ticker, base_asset, quote_asset, settle_asset, kind, margin_mode, is_active)
VALUES
    -- Major Cryptocurrencies
    ('dydx', 'BTC-USD', 'BTC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'ETH-USD', 'ETH', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'SOL-USD', 'SOL', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'AVAX-USD', 'AVAX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'ATOM-USD', 'ATOM', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'MATIC-USD', 'MATIC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'DOT-USD', 'DOT', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'LINK-USD', 'LINK', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'UNI-USD', 'UNI', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'AAVE-USD', 'AAVE', 'USD', 'USDC', 'perp', 'linear', TRUE),
    
    -- Layer 1 / Layer 2
    ('dydx', 'NEAR-USD', 'NEAR', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'FTM-USD', 'FTM', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'ALGO-USD', 'ALGO', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'ADA-USD', 'ADA', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'XLM-USD', 'XLM', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'ARB-USD', 'ARB', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'OP-USD', 'OP', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'SUI-USD', 'SUI', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'APT-USD', 'APT', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'SEI-USD', 'SEI', 'USD', 'USDC', 'perp', 'linear', TRUE),
    
    -- DeFi Tokens
    ('dydx', 'MKR-USD', 'MKR', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'SUSHI-USD', 'SUSHI', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'CRV-USD', 'CRV', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'LDO-USD', 'LDO', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'DYDX-USD', 'DYDX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    
    -- Popular Altcoins
    ('dydx', 'DOGE-USD', 'DOGE', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'SHIB-USD', 'SHIB', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'PEPE-USD', 'PEPE', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'WLD-USD', 'WLD', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'BLUR-USD', 'BLUR', 'USD', 'USDC', 'perp', 'linear', TRUE),
    
    -- Additional High-Volume Markets
    ('dydx', 'BCH-USD', 'BCH', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'LTC-USD', 'LTC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'XRP-USD', 'XRP', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'TRX-USD', 'TRX', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'ETC-USD', 'ETC', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'FIL-USD', 'FIL', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'ICP-USD', 'ICP', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'INJ-USD', 'INJ', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'TIA-USD', 'TIA', 'USD', 'USDC', 'perp', 'linear', TRUE),
    ('dydx', 'WIF-USD', 'WIF', 'USD', 'USDC', 'perp', 'linear', TRUE)

ON CONFLICT (exchange, ticker) DO NOTHING;
