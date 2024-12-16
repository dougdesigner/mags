SELECT
    trading_date,
    CAST(NULL as STRING) as ticker,
    CASE 
        WHEN sp500_details.failed_count = 0 AND concentration_metrics.total_mag7_market_cap IS NULL 
        THEN 'Market Holiday'
        WHEN sp500_details.failed_count = 0 
        THEN 'No collection failures'
        ELSE 'Failed collections present'
    END as reason
FROM myDataSource