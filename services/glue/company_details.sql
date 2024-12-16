SELECT 
    trading_date,
    stack(7,
        'AAPL', companies.AAPL.name, 
        CAST(companies.AAPL.market_cap AS DOUBLE),
        CAST(companies.AAPL.shares_outstanding AS DOUBLE),
        companies.AAPL.currency, 
        companies.AAPL.description,

        'MSFT', companies.MSFT.name, 
        CAST(companies.MSFT.market_cap AS DOUBLE),
        CAST(companies.MSFT.shares_outstanding AS DOUBLE),
        companies.MSFT.currency, 
        companies.MSFT.description,

        'GOOGL', companies.GOOGL.name, 
        CAST(companies.GOOGL.market_cap AS DOUBLE),
        CAST(companies.GOOGL.shares_outstanding AS DOUBLE),
        companies.GOOGL.currency, 
        companies.GOOGL.description,

        'AMZN', companies.AMZN.name, 
        CAST(companies.AMZN.market_cap AS DOUBLE),
        CAST(companies.AMZN.shares_outstanding AS DOUBLE),
        companies.AMZN.currency, 
        companies.AMZN.description,

        'NVDA', companies.NVDA.name, 
        CAST(companies.NVDA.market_cap AS DOUBLE),
        CAST(companies.NVDA.shares_outstanding AS DOUBLE),
        companies.NVDA.currency, 
        companies.NVDA.description,

        'META', companies.META.name, 
        CAST(companies.META.market_cap AS DOUBLE),
        CAST(companies.META.shares_outstanding AS DOUBLE),
        companies.META.currency, 
        companies.META.description,

        'TSLA', companies.TSLA.name, 
        CAST(companies.TSLA.market_cap AS DOUBLE),
        CAST(companies.TSLA.shares_outstanding AS DOUBLE),
        companies.TSLA.currency, 
        companies.TSLA.description
    ) AS (ticker, company_name, market_cap, shares_outstanding, currency, description)
FROM myDataSource
WHERE companies.AAPL.market_cap IS NOT NULL