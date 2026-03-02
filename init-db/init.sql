CREATE TABLE stock_quotes (
  id SERIAL PRIMARY KEY,
  symbol VARCHAR(10) NOT NULL,
  current_price NUMERIC(12, 4),
  high_price NUMERIC(12, 4),
  low_price NUMERIC(12, 4),
  open_price NUMERIC(12, 4),
  previous_close NUMERIC(12, 4),
  market_time TIMESTAMPTZ NOT NULL,
  fetch_time TIMESTAMPTZ NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW ()
);
