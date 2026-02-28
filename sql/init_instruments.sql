-- Create ENUM types for instrument classification
CREATE TYPE inst_kind AS ENUM ('spot', 'perp', 'future');
CREATE TYPE margin_mode AS ENUM ('linear', 'inverse');

-- Create instruments table
CREATE TABLE instruments (
    id SERIAL PRIMARY KEY,
    exchange VARCHAR(20) NOT NULL,  -- 'binance', 'bybit', 'okx', 'kraken'
    ticker VARCHAR(50) NOT NULL,    -- 'BTCUSDT', 'ETH-PERP', 'XBTUSD'
    
    -- Categorization
    base_asset VARCHAR(10) NOT NULL,   -- 'BTC', 'ETH', 'SOL'
    quote_asset VARCHAR(10) NOT NULL,  -- 'USDT', 'USD', 'BUSD'
    settle_asset VARCHAR(10),          -- 'USDT' (Linear) or 'BTC' (Inverse), NULL for spot
    kind inst_kind NOT NULL,           -- 'spot', 'perp', 'future'
    margin_mode margin_mode,           -- NULL for spot, 'linear'/'inverse' for derivatives
    
    -- Lifecycle & Delisting
    is_active BOOLEAN DEFAULT TRUE,
    delisted_at TIMESTAMPTZ,           -- Stores exactly when it was removed/delisted
    
    UNIQUE(exchange, ticker)
);

-- Create indexes for common queries
CREATE INDEX idx_instruments_exchange ON instruments(exchange);
CREATE INDEX idx_instruments_active ON instruments(is_active) WHERE is_active = TRUE;
CREATE INDEX idx_instruments_kind ON instruments(kind);
