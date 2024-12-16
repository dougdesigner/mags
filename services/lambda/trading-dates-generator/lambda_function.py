import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def generate_trading_dates():
    """
    Generate list of trading dates from today back to 4 years ago.
    
    The function creates a list of dates excluding weekends, working
    backward from today. For 4 years of daily data (excluding weekends),
    we expect around 1,040 dates (260 trading days per year × 4 years).
    
    Returns:
        list: ISO format dates (YYYY-MM-DD) representing trading days
    """
    dates = []
    current = datetime.now(ZoneInfo("America/New_York"))
    end_date = current - timedelta(days=4 * 365)
    
    while current > end_date:
        # Skip weekends (0-4 are Monday to Friday)
        if current.weekday() < 5:  # 0-4 are Monday to Friday
            dates.append(current.date().isoformat())
        current = current - timedelta(days=1)

    # Log metadata for monitoring without changing the return structure
    print(json.dumps({
        'metadata': {
            'date_count': len(dates),
            'date_range': {
                'start': dates[0] if dates else None,
                'end': dates[-1] if dates else None
            }
        }
    }))
    
    return dates

def lambda_handler(event, context):
    dates = generate_trading_dates()

    return {
        'dates': dates
    }