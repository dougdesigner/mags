-- Check for any extreme values that might indicate transformation issues
SELECT trading_date, ticker, market_cap 
FROM market_data.company_details 
WHERE market_cap > 5e12 OR market_cap < 1e11;