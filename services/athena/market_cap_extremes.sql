-- Check for any extreme values that might indicate transformation issues
SELECT 
    trading_date,
    ticker,
    market_cap
FROM market_data.company_details
WHERE market_cap > 5e12  -- More than $5 trillion
   OR market_cap < 1e11; -- Less than $100 billion