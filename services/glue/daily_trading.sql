SELECT 
    -- Get trading date from the nested trading_data to match the actual trading day
    companies.AAPL.trading_data.date AS trading_date,
    -- Use stack to transform company trading data into rows
    stack(7,
        'AAPL', companies.AAPL.trading_data.open_price, companies.AAPL.trading_data.high_price, companies.AAPL.trading_data.low_price, companies.AAPL.trading_data.close_price, companies.AAPL.trading_data.volume, companies.AAPL.trading_data.vwap,
        'MSFT', companies.MSFT.trading_data.open_price, companies.MSFT.trading_data.high_price, companies.MSFT.trading_data.low_price, companies.MSFT.trading_data.close_price, companies.MSFT.trading_data.volume, companies.MSFT.trading_data.vwap,
        'GOOGL', companies.GOOGL.trading_data.open_price, companies.GOOGL.trading_data.high_price, companies.GOOGL.trading_data.low_price, companies.GOOGL.trading_data.close_price, companies.GOOGL.trading_data.volume, companies.GOOGL.trading_data.vwap,
        'AMZN', companies.AMZN.trading_data.open_price, companies.AMZN.trading_data.high_price, companies.AMZN.trading_data.low_price, companies.AMZN.trading_data.close_price, companies.AMZN.trading_data.volume, companies.AMZN.trading_data.vwap,
        'NVDA', companies.NVDA.trading_data.open_price, companies.NVDA.trading_data.high_price, companies.NVDA.trading_data.low_price, companies.NVDA.trading_data.close_price, companies.NVDA.trading_data.volume, companies.NVDA.trading_data.vwap,
        'META', companies.META.trading_data.open_price, companies.META.trading_data.high_price, companies.META.trading_data.low_price, companies.META.trading_data.close_price, companies.META.trading_data.volume, companies.META.trading_data.vwap,
        'TSLA', companies.TSLA.trading_data.open_price, companies.TSLA.trading_data.high_price, companies.TSLA.trading_data.low_price, companies.TSLA.trading_data.close_price, companies.TSLA.trading_data.volume, companies.TSLA.trading_data.vwap
    ) AS (ticker, open_price, high_price, low_price, close_price, volume, vwap)
FROM myDataSource
WHERE companies.AAPL.trading_data.close_price IS NOT NULL  -- Filter out market holidays