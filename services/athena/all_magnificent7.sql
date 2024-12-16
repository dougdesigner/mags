-- Verify we have data for all Magnificent 7 companies each trading day
SELECT 
    trading_date,
    COUNT(DISTINCT ticker) as company_count
FROM market_data.company_details
GROUP BY trading_date
HAVING COUNT(DISTINCT ticker) != 7
ORDER BY trading_date;