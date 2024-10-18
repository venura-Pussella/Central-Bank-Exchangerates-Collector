import pandas as pd

def process_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    # Drop columns named 'Unnamed: 0'
    df = df.loc[:, df.columns != 'Unnamed: 0']
    
    # Drop the first three rows
    df = df.drop(index=[0, 1, 2])
    
    # Reset the index after dropping rows
    df.reset_index(drop=True, inplace=True)
    
    # Fill NaN values with empty strings for both the first and second rows
    first_row_filled = df.iloc[0].fillna('')
    second_row_filled = df.iloc[1].fillna('')
    
    # Combine the values from the first and second rows for each column, ignoring NaNs
    combined_row = first_row_filled + '_' + second_row_filled
    
    # Update the second row with the combined values
    df.iloc[1] = combined_row
    
    return df

def creating_headers(df: pd.DataFrame) -> pd.DataFrame:

    # Drop the row at index 0
    df = df.drop(index=0)
    
    # Set the row at index 1 as the column headers
    df.columns = df.iloc[0]
    
    # Drop the row now serving as headers (previously index 1)
    df = df.drop(index=1)
    
    # Drop rows where all cells are NaN
    df = df.dropna(how='all')

    # Reset the index to keep it sequential
    df = df.reset_index(drop=True)

    # drop the columns which are all NaN
    df = df.dropna(axis=1, how='all')
    
    return df


def clean_column_names(df):
    # Ensure all column names are strings before replacing
    df.columns = df.columns.astype(str)
    
    # Clean up column names: remove extra spaces after underscores
    df.columns = [col.replace('_', '_').replace(' ', '') for col in df.columns]
    
    df.drop(index=4, inplace=True)
    df.reset_index(drop=True, inplace=True)
    
    # Rename the '_' column to 'Date', assuming this is the intent
    df.rename(columns={'_': 'Date'}, inplace=True)
    
    return df

def clean_dataframe(df1_cleaned):
    # Convert 'Date' column to datetime, coercing errors to NaT
    df1_cleaned['Date'] = pd.to_datetime(df1_cleaned['Date'].astype(str), errors='coerce')
    
    # Remove time and keep only the date part
    df1_cleaned['Date'] = df1_cleaned['Date'].dt.date
    
    # Drop rows where 'Date' is NaT (invalid dates)
    df1_cleaned = df1_cleaned.dropna(subset=['Date'])
    
    # Optionally, reset the index after dropping rows
    df1_cleaned = df1_cleaned.reset_index(drop=True)

    # df1_cleaned = df1_cleaned.fillna("NaN")

    return df1_cleaned

