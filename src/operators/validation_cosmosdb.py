import pandas as pd
import datetime
import uuid

def clean_and_validate_row(row):
    def safe_float(value):
        if pd.isna(value) or value == 'nan':
            return None
        try:
            return float(value)
        except (ValueError, TypeError):
            return None

    def safe_date(value):
        if isinstance(value, (datetime.date, datetime.datetime)):
            return value.isoformat()
        return str(value) if value else None

    return {
        "id": str(uuid.uuid4()),  # Generate a unique ID
        "date": safe_date(row['Date']),
        "buying_usd": safe_float(row['Buying_USD']),
        "selling_usd": safe_float(row['Selling_USD']),
        "buying_gbp": safe_float(row['Buying_GBP']),
        "selling_gbp": safe_float(row['Selling_GBP']),
        "buying_eur": safe_float(row['Buying_EUR']),
        "selling_eur": safe_float(row['Selling_EUR']),
        "buying_chf": safe_float(row['Buying_CHF']),
        "selling_chf": safe_float(row['Selling_CHF']),
        "buying_cad": safe_float(row['Buying_CAD']),
        "selling_cad": safe_float(row['Selling_CAD']),
        "buying_aud": safe_float(row['Buying_AUD']),
        "selling_aud": safe_float(row['Selling_AUD']),
        "buying_sgd": safe_float(row['Buying_SGD']),
        "selling_sgd": safe_float(row['Selling_SGD']),
        "buying_jpy": safe_float(row['Buying_JPY']),
        "selling_jpy": safe_float(row['Selling_JPY']),
        "buying_cny": safe_float(row['Buying_CNY']),
        "selling_cny": safe_float(row['Selling_CNY'])
    }