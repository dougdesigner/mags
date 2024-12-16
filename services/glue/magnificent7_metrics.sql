SELECT
    trading_date,
    -- Extract from the rankings.by_market_cap array which is already ordered
    stack(7,
        rankings.by_market_cap[0].ticker, rankings.by_market_cap[0].market_cap, rankings.by_market_cap[0].pct_of_mag7, rankings.by_market_cap[0].pct_of_sp500, 1,
        rankings.by_market_cap[1].ticker, rankings.by_market_cap[1].market_cap, rankings.by_market_cap[1].pct_of_mag7, rankings.by_market_cap[1].pct_of_sp500, 2,
        rankings.by_market_cap[2].ticker, rankings.by_market_cap[2].market_cap, rankings.by_market_cap[2].pct_of_mag7, rankings.by_market_cap[2].pct_of_sp500, 3,
        rankings.by_market_cap[3].ticker, rankings.by_market_cap[3].market_cap, rankings.by_market_cap[3].pct_of_mag7, rankings.by_market_cap[3].pct_of_sp500, 4,
        rankings.by_market_cap[4].ticker, rankings.by_market_cap[4].market_cap, rankings.by_market_cap[4].pct_of_mag7, rankings.by_market_cap[4].pct_of_sp500, 5,
        rankings.by_market_cap[5].ticker, rankings.by_market_cap[5].market_cap, rankings.by_market_cap[5].pct_of_mag7, rankings.by_market_cap[5].pct_of_sp500, 6,
        rankings.by_market_cap[6].ticker, rankings.by_market_cap[6].market_cap, rankings.by_market_cap[6].pct_of_mag7, rankings.by_market_cap[6].pct_of_sp500, 7
    ) AS (ticker, market_cap, pct_of_mag7, pct_of_sp500, ranking)
FROM myDataSource
WHERE rankings.by_market_cap[0].market_cap IS NOT NULL  -- Filter out market holidays