import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

def generate_trading_dates():
    """
    Generate list of trading dates from a range.
    
    The function creates a list of dates excluding weekends from a range based on inputs.
    
    Returns:
        list: ISO format dates (YYYY-MM-DD) representing trading days
    """
    start_date = datetime(2020, 11,29)
    end_date = datetime(2022, 6, 10)
    dates = []
    current = end_date
    end_date = start_date
    
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