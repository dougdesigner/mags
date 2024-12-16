from polygon import RESTClient
import json
import os
import boto3
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import time

def get_previous_trading_date():
    """
    Determines the most recent trading day, accounting for weekends.
    This function ensures we don't request data for non-trading days.
    
    Returns:
        str: Date in YYYY-MM-DD format representing the last trading day
    """
    et_now = datetime.now(ZoneInfo("America/New_York"))
    date = et_now - timedelta(days=1)
    
    # Handle weekend cases by moving back to Friday
    if date.weekday() == 6:  # Sunday
        date = date - timedelta(days=2)
    elif date.weekday() == 5:  # Saturday
        date = date - timedelta(days=1)
    
    return date.date().isoformat()

def get_historical_ticker(ticker, trading_date):
    """
    Returns the historically accurate ticker symbol for a given date.
    
    Some companies have changed their ticker symbols over time. This function
    ensures we query using the correct historical symbol while maintaining
    consistent modern naming in our stored data.
    
    Args:
        ticker (str): Current ticker symbol
        trading_date (str): Date in YYYY-MM-DD format
        
    Returns:
        str: The historically accurate ticker symbol for that date
    """
    # Meta changed from FB to META on June 9, 2022 but API only shows proper data from June 10, 2022
    meta_change_date = "2022-06-09"
    
    if ticker == "META" and trading_date <= meta_change_date:
        return "FB"
    
    return ticker

def get_historical_ticker_aggs(ticker, trading_date):
    """
    Returns the historically accurate ticker symbol for a given date.
    
    Some companies have changed their ticker symbols over time. This function
    ensures we query using the correct historical symbol while maintaining
    consistent modern naming in our stored data.
    
    Args:
        ticker (str): Current ticker symbol
        trading_date (str): Date in YYYY-MM-DD format
        
    Returns:
        str: The historically accurate ticker symbol for that date
    """
    # Meta changed from FB to META on June 9, 2022
    meta_change_date = "2022-06-08"
    
    if ticker == "META" and trading_date <= meta_change_date:
        return "FB"
    
    return ticker

def get_stock_aggs(client, ticker, trading_date):
    """
    Retrieves daily aggregate data for a stock using the stocks endpoint.
    
    The stock aggregates endpoint returns data in this structure:
    {
        "adjusted": true,
        "queryCount": 2,
        "results": [
            {
                "c": close_price,
                "h": high_price,
                "l": low_price,
                "o": open_price,
                "t": timestamp,
                "v": volume,
                "vw": volume_weighted_price
            }
        ]
    }
    """
    try:
        time.sleep(0.5)  # Rate limit compliance

        # Get the historically accurate ticker for API queries
        historical_ticker = get_historical_ticker_aggs(ticker, trading_date)
        
        # Make the API call with proper parameters
        response = client.get_aggs(
            ticker=historical_ticker,
            multiplier=1,
            timespan="day",
            from_=trading_date,
            to=trading_date,
            adjusted=True
        )
        
        # Check if we have valid results
        if not response or len(response) == 0:
            print(f"No aggregate data available for stock {ticker} on {trading_date}")
            return None
            
        # Get the first (and should be only) result for our single day
        agg = response[0]
        
        # Create our return dictionary using the correct property names
        return {
            'date': trading_date,
            'close_price': float(agg.close),  # Using full property names
            'open_price': float(agg.open),
            'high_price': float(agg.high),
            'low_price': float(agg.low),
            'volume': int(agg.volume),
            'vwap': float(agg.vwap) if hasattr(agg, 'vwap') else None,
            'timestamp': agg.timestamp
        }
        
    except Exception as e:
        print(f"Error fetching stock aggregates for {ticker} on {trading_date}: {str(e)}")
        # Add more detailed error information
        if 'response' in locals():
            print(f"Response structure: {str(response)}")
        return None

def get_company_details(client, ticker, trading_date):
    """
    Retrieves company details using the Ticker Details v3 endpoint.

    Args:
        client: Polygon.io REST client
        ticker (str): Stock ticker symbol
        trading_date (str): Date in YYYY-MM-DD format to fetch historical data for
    
    Returns:
        dict: Company details including historical market cap data
    """
    try:
        time.sleep(1)  # Rate limiting

        # Get the historically accurate ticker for API queries
        historical_ticker = get_historical_ticker(ticker, trading_date)

        response = client.get_ticker_details(
            historical_ticker, 
            date=trading_date
        )
        
        if response:
            return {
                'name': getattr(response, 'name', None),
                'market_cap': getattr(response, 'market_cap', None),
                'shares_outstanding': getattr(response, 'weighted_shares_outstanding', None),
                'currency': getattr(response, 'currency_name', None),
                'description': getattr(response, 'description', None)
            }
        else:
            print(f"No response data for {ticker}")
            return None
            
    except Exception as e:
        print(f"Error fetching company details for {ticker}: {str(e)}")
        return None

def get_sp500_total_market_cap(client, sp500_tickers, trading_date):
    """
    Calculates the total market cap of the S&P 500 by fetching data for all constituents.
    """
    total_market_cap = 0
    processed_tickers = []
    failed_tickers = []
    
    print(f"Starting to process {len(sp500_tickers)} S&P 500 constituents...")
    
    for ticker in sp500_tickers:
        try:
            time.sleep(1)

            # Get the historically accurate ticker for API queries
            historical_ticker = get_historical_ticker(ticker, trading_date)

            response = client.get_ticker_details(historical_ticker, date=trading_date)
            
            if response and hasattr(response, 'market_cap') and response.market_cap:
                total_market_cap += response.market_cap
                processed_tickers.append({
                    'ticker': ticker,
                    'market_cap': response.market_cap
                })
                print(f"Successfully processed {ticker}: ${response.market_cap / 1e9:.2f}B")
            else:
                print(f"No market cap data available for {ticker}")
                failed_tickers.append({
                    'ticker': ticker,
                    'reason': 'No market cap data'
                })
                
        except Exception as e:
            print(f"Error processing {ticker}: {str(e)}")
            failed_tickers.append({
                'ticker': ticker,
                'reason': str(e)
            })
            continue
    
    success_rate = (len(processed_tickers) / len(sp500_tickers)) * 100
    print(f"\nProcessing complete:")
    print(f"Successfully processed: {len(processed_tickers)} tickers ({success_rate:.1f}%)")
    print(f"Failed to process: {len(failed_tickers)} tickers")
    print(f"Total S&P 500 market cap: ${total_market_cap / 1e12:.2f}T")
    
    return total_market_cap, processed_tickers, failed_tickers

def lambda_handler(event, context):
    # Initialize API and AWS clients
    client = RESTClient(os.environ['POLYGON_API_KEY'])
    s3 = boto3.client('s3')
    
    # Define the Magnificent 7 companies
    magnificent_7 = [
        "AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA"
    ]
    
    # S&P 500 tickers for market cap comparison
    sp500_tickers = ['AAPL', 'NVDA', 'MSFT', 'AMZN', 'META', 'TSLA', 'GOOGL', 'BRK.B', 'GOOG', 'AVGO', 'JPM', 'LLY', 'V', 'UNH', 'XOM', 'COST', 'MA', 'HD', 'WMT', 'PG', 'NFLX', 'JNJ', 'CRM', 'BAC', 'ABBV', 'ORCL', 'CVX', 'MRK', 'WFC', 'ADBE', 'KO', 'CSCO', 'NOW', 'ACN', 'AMD', 'IBM', 'PEP', 'LIN', 'MCD', 'DIS', 'PM', 'TMO', 'ABT', 'ISRG', 'CAT', 'GE', 'GS', 'INTU', 'VZ', 'BKNG', 'QCOM', 'TXN', 'T', 'AXP', 'CMCSA', 'SPGI', 'MS', 'RTX', 'LOW', 'NEE', 'PLTR', 'PGR', 'DHR', 'ETN', 'HON', 'AMGN', 'PFE', 'BLK', 'AMAT', 'TJX', 'UNP', 'UBER', 'C', 'BX', 'COP', 'BSX', 'SYK', 'PANW', 'ADP', 'SCHW', 'BMY', 'TMUS', 'FI', 'VRTX', 'GILD', 'DE', 'SBUX', 'BA', 'MU', 'ANET', 'MMC', 'LMT', 'ADI', 'MDT', 'KKR', 'CB', 'PLD', 'LRCX', 'MO', 'AMT', 'GEV', 'NKE', 'EQIX', 'TT', 'SO', 'UPS', 'PYPL', 'CMG', 'ICE', 'PH', 'APH', 'SHW', 'INTC', 'CI', 'ELV', 'KLAC', 'DUK', 'CME', 'CRWD', 'CDNS', 'MDLZ', 'PNC', 'REGN', 'AON', 'MSI', 'USB', 'WM', 'ZTS', 'CEG', 'SNPS', 'MCK', 'MCO', 'CL', 'CTAS', 'WELL', 'EMR', 'ITW', 'MMM', 'ORLY', 'EOG', 'TDG', 'COF', 'APD', 'GD', 'CVS', 'WMB', 'MAR', 'CSX', 'ADSK', 'NOC', 'AJG', 'HLT', 'OKE', 'BDX', 'ECL', 'TFC', 'FDX', 'FTNT', 'CARR', 'TGT', 'RCL', 'PCAR', 'FCX', 'ABNB', 'GM', 'TRV', 'BK', 'HCA', 'DLR', 'ROP', 'NSC', 'FICO', 'SLB', 'URI', 'SRE', 'AZO', 'SPG', 'JCI', 'NXPI', 'AMP', 'VST', 'CPRT', 'AFL', 'PSX', 'ALL', 'KMI', 'GWW', 'PSA', 'ROST', 'CMI', 'AEP', 'MPC', 'MET', 'AXON', 'PWR', 'O', 'AIG', 'MSCI', 'HWM', 'NEM', 'D', 'FIS', 'DHI', 'FAST', 'TEL', 'LULU', 'PAYX', 'KMB', 'PRU', 'DFS', 'PEG', 'LHX', 'PCG', 'AME', 'CCI', 'RSG', 'KVUE', 'EW', 'TRGP', 'COR', 'VLO', 'CBRE', 'DAL', 'IR', 'CTVA', 'F', 'BKR', 'A', 'VRSK', 'CTSH', 'EA', 'OTIS', 'IT', 'SYY', 'LEN', 'KR', 'HES', 'CHTR', 'XEL', 'YUM', 'ODFL', 'GLW', 'VMC', 'EXC', 'STZ', 'GEHC', 'MNST', 'KDP', 'ACGL', 'GIS', 'WAB', 'IDXX', 'MLM', 'DELL', 'RMD', 'HPQ', 'MTB', 'IRM', 'IQV', 'HIG', 'EXR', 'DD', 'HUM', 'NUE', 'GRMN', 'NDAQ', 'ROK', 'VICI', 'EFX', 'UAL', 'ED', 'WTW', 'EIX', 'ETR', 'AVB', 'OXY', 'FITB', 'MCHP', 'CSGP', 'FANG', 'DXCM', 'HPE', 'EBAY', 'TTWO', 'XYL', 'WEC', 'TSCO', 'DECK', 'RJF', 'ANSS', 'GPN', 'KEYS', 'CAH', 'CNC', 'DOW', 'STT', 'PPG', 'GDDY', 'MPWR', 'ON', 'NVR', 'DOV', 'FTV', 'TROW', 'BR', 'KHC', 'NTAP', 'SW', 'CCL', 'SYF', 'MTD', 'TYL', 'VLTO', 'PHM', 'CHD', 'BRO', 'HSY', 'AWK', 'EQT', 'HBAN', 'VTR', 'HAL', 'CPAY', 'TPL', 'EQR', 'DTE', 'HUBB', 'PPL', 'ADM', 'AEE', 'CINF', 'PTC', 'CDW', 'RF', 'WBD', 'EXPE', 'SBAC', 'WST', 'WDC', 'BIIB', 'WAT', 'WY', 'IFF', 'TDY', 'SMCI', 'ATO', 'ZBH', 'LDOS', 'DVN', 'NTRS', 'K', 'PKG', 'LYV', 'ES', 'CBOE', 'STE', 'ZBRA', 'CFG', 'FE', 'FSLR', 'STX', 'CLX', 'CNP', 'NRG', 'LUV', 'BLDR', 'ULTA', 'OMC', 'DRI', 'CMS', 'LYB', 'IP', 'COO', 'STLD', 'LH', 'MKC', 'TER', 'ESS', 'LVS', 'INVH', 'WRB', 'SNA', 'PODD', 'MAA', 'EL', 'CTRA', 'TRMB', 'FDS', 'PFG', 'DG', 'TSN', 'PNR', 'MAS', 'DGX', 'KEY', 'HOLX', 'IEX', 'BALL', 'BBY', 'MOH', 'J', 'GPC', 'KIM', 'GEN', 'EXPD', 'NI', 'ALGN', 'AVY', 'BAX', 'ARE', 'EG', 'DPZ', 'VRSN', 'CF', 'L', 'LNT', 'TXT', 'JBL', 'VTRS', 'APTV', 'DOC', 'MRNA', 'FFIV', 'AKAM', 'AMCR', 'JBHT', 'DLTR', 'EVRG', 'RVTY', 'TPR', 'POOL', 'SWKS', 'EPAM', 'ROL', 'NDSN', 'UDR', 'KMX', 'HST', 'CAG', 'SWK', 'CPT', 'JKHY', 'DAY', 'SJM', 'CHRW', 'ALB', 'ALLE', 'NCLH', 'INCY', 'REG', 'JNPR', 'BG', 'EMN', 'TECH', 'BXP', 'AIZ', 'UHS', 'PAYC', 'CTLT', 'LW', 'NWSA', 'IPG', 'GNRC', 'TAP', 'FOXA', 'PNW', 'ERIE', 'LKQ', 'CRL', 'ENPH', 'SOLV', 'HRL', 'GL', 'AES', 'HSIC', 'RL', 'MKTX', 'WYNN', 'AOS', 'TFX', 'HAS', 'FRT', 'MTCH', 'MGM', 'CPB', 'MOS', 'BF.B', 'CZR', 'IVZ', 'APA', 'CE', 'BWA', 'DVA', 'HII', 'FMC', 'MHK', 'BEN', 'PARA', 'QRVO', 'WBA', 'FOX', 'NWS', 'AMTM']
    
    try:
        trading_date = event if isinstance(event, str) else get_previous_trading_date()
        
        # Initialize our data structure
        market_data = {
            'trading_date': trading_date,
            'data_collection_time': datetime.now(ZoneInfo("America/New_York")).isoformat(),
            'companies': {},
            'rankings': {},
            'concentration_metrics': {}
        }
        
        # Get S&P 500 market cap data first
        sp500_total_market_cap, processed_sp500, failed_sp500 = get_sp500_total_market_cap(
            client, sp500_tickers, trading_date
        )
        
        # Store S&P 500 details
        market_data['sp500_details'] = {
            'total_market_cap': sp500_total_market_cap,
            'processed_count': len(processed_sp500),
            'failed_count': len(failed_sp500),
            'failed_tickers': failed_sp500
        }
        
        # Collect data for each Magnificent 7 company
        total_mag7_market_cap = 0
        company_data = []
        
        for ticker in magnificent_7:
            print(f"Processing {ticker}...")
            
            details = get_company_details(client, ticker, trading_date)
            stock_data = get_stock_aggs(client, ticker, trading_date)
            
            if details and stock_data and details.get('market_cap'):
                company_info = {
                    **details,
                    'trading_data': stock_data
                }
                
                market_data['companies'][ticker] = company_info
                total_mag7_market_cap += details['market_cap']
                
                company_data.append({
                    'ticker': ticker,
                    'name': details['name'],
                    'market_cap': details['market_cap'],
                    'pct_of_mag7': None
                })
                print(f"Successfully processed {ticker}")
            else:
                print(f"Skipping {ticker} due to missing data")
        
        # Calculate percentages and create rankings
        if company_data:
            sorted_companies = sorted(company_data, key=lambda x: x['market_cap'], reverse=True)
            
            for company in sorted_companies:
                company['pct_of_mag7'] = (company['market_cap'] / total_mag7_market_cap) * 100
                if sp500_total_market_cap > 0:
                    company['pct_of_sp500'] = (company['market_cap'] / sp500_total_market_cap) * 100
            
            market_data['rankings'] = {
                'by_market_cap': sorted_companies
            }
            
            # Store concentration metrics
            market_data['concentration_metrics'] = {
                'total_mag7_market_cap': total_mag7_market_cap,
                'sp500_total_market_cap': sp500_total_market_cap,
                'mag7_pct_of_sp500': (total_mag7_market_cap / sp500_total_market_cap * 100) if sp500_total_market_cap > 0 else 0,
                'collection_date': trading_date,
                'mag7_companies_count': len(market_data['companies'])
            }
        
        # Store in S3
        s3_key = f"raw/magnificent7/trading_date={trading_date}/market_data.json"
        
        s3.put_object(
            Bucket=os.environ['DATA_BUCKET'],
            Key=s3_key,
            Body=json.dumps(market_data, indent=2)
        )
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Data successfully collected',
                's3_key': s3_key,
                'summary': {
                    'trading_date': trading_date,
                    'companies_collected': len(market_data['companies']),
                    'total_mag7_market_cap': f"${total_mag7_market_cap / 1e12:.2f}T",
                    'sp500_total_market_cap': f"${sp500_total_market_cap / 1e12:.2f}T",
                    'mag7_pct_of_sp500': f"{(total_mag7_market_cap / sp500_total_market_cap * 100):.1f}%" if sp500_total_market_cap > 0 else "N/A",
                    'top_company': sorted_companies[0]['name'] if company_data else None
                }
            }, indent=2)
        }
        
    except Exception as e:
        print(f"Error in lambda_handler: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'trading_date': trading_date if 'trading_date' in locals() else None
            })
        }