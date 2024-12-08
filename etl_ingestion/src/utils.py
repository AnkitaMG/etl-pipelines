import re
from datetime import datetime, timedelta

def to_snake_case(column_name: str) -> str:
    # Convert camelCase or space-separated names to snake_case
    return re.sub(r'([a-z0-9])([A-Z])', r'\1_\2', column_name).lower().replace(' ', '_')


def find_date_ranges(
        latest_date: str
) -> dict:
    """
    Finds the date ranges for last month, last 6 months, and last 12 months
    from a given latest date.
    """
    latest_date_obj = datetime.strptime(latest_date, '%Y-%m-%d')
    last_month = latest_date_obj - timedelta(days=30)
    last_six_months = latest_date_obj - timedelta(days=180)
    last_twelve_months = latest_date_obj - timedelta(days=365)

    return {
        "last_month": last_month,
        "last_six_months": last_six_months,
        "last_twelve_months": last_twelve_months
    }
