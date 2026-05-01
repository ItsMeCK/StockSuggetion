-- Bitemporal Append-Only Ledger Schema
CREATE TYPE trade_status AS ENUM (
    'SIGNALED',       -- Agent approved, ready for AMO
    'AMO_PLACED',     -- Limit order sent to Zerodha
    'ACTIVE',         -- Order filled, position is live
    'STOP_HIT',       -- GTT stop-loss triggered
    'PROFIT_BOOKED',  -- Target reached / GTT trailing stop hit
    'EXPIRED_UNFILLED'-- AMO never triggered, gap up occurred
);

CREATE TABLE IF NOT EXISTS trade_events (
    event_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    trade_id UUID NOT NULL, 
    ticker VARCHAR(20) NOT NULL,
    status trade_status NOT NULL,
    price DECIMAL(10,2),
    quantity INT,
    market_time TIMESTAMP WITH TIME ZONE, 
    system_time TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP, 
    order_id VARCHAR(50), 
    notes TEXT
);
