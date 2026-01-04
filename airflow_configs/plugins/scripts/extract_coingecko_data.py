# airflow/scripts/extract_coingecko_data.py
import requests
import json
import pandas as pd
from datetime import datetime
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_coin_list():
    """Fetches a list of all supported coins from CoinGecko API."""
    url = "https://api.coingecko.com/api/v3/coins/list"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Raise HTTPError for bad responses (4xx or 5xx)
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching coin list: {e}")
        return None

def get_market_chart(coin_id, vs_currency='usd', days='365'):
    """Fetches market chart data (price, market cap, total volume) for a coin."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/market_chart"
    params = {
        'vs_currency': vs_currency,
        'days': days,
        'interval': 'daily' # Or 'hourly', 'minutely' depending on your needs
    }
    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Error fetching market chart for {coin_id}: {e}")
        return None

def transform_market_chart_data(data, coin_id):
    """Transforms raw CoinGecko market chart data into a flat DataFrame."""
    if not data:
        return pd.DataFrame()

    prices = data.get('prices', [])
    market_caps = data.get('market_caps', [])
    total_volumes = data.get('total_volumes', [])

    df_prices = pd.DataFrame(prices, columns=['timestamp', 'price'])
    df_market_caps = pd.DataFrame(market_caps, columns=['timestamp', 'market_cap'])
    df_total_volumes = pd.DataFrame(total_volumes, columns=['timestamp', 'total_volume'])

    # Convert timestamps to datetime and then to date for daily aggregation
    df_prices['date'] = pd.to_datetime(df_prices['timestamp'], unit='ms').dt.date
    df_market_caps['date'] = pd.to_datetime(df_market_caps['timestamp'], unit='ms').dt.date
    df_total_volumes['date'] = pd.to_datetime(df_total_volumes['timestamp'], unit='ms').dt.date

    # Group by date and take the last value for simplicity or average, depending on your analysis needs
    # For daily data, the last value of the day is often used.
    df_prices = df_prices.groupby('date')['price'].last().reset_index()
    df_market_caps = df_market_caps.groupby('date')['market_cap'].last().reset_index()
    df_total_volumes = df_total_volumes.groupby('date')['total_volume'].last().reset_index()

    # Merge dataframes
    df = pd.merge(df_prices, df_market_caps, on='date', how='outer')
    df = pd.merge(df, df_total_volumes, on='date', how='outer')

    df['coin_id'] = coin_id
    df['extraction_timestamp'] = datetime.utcnow()

    return df[['coin_id', 'date', 'price', 'market_cap', 'total_volume', 'extraction_timestamp']]

def extract_and_load_coingecko_data(**kwargs):
    """
    Airflow callable function to extract CoinGecko data and load into Snowflake.
    """
    coin_ids = kwargs.get('coin_ids', ['bitcoin', 'ethereum', 'ripple', 'litecoin', 'cardano'])
    vs_currency = kwargs.get('vs_currency', 'usd')

    snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake_default') # Ensure this matches your Airflow connection ID
    table_name = "COINGECKO_HISTORICAL_PRICES"
    schema_name = "RAW"
    database_name = "CRYPTO_DB"

    all_data_frames = []

    for coin_id in coin_ids:
        print(f"Fetching data for {coin_id}...")
        raw_data = get_market_chart(coin_id, vs_currency=vs_currency)
        df = transform_market_chart_data(raw_data, coin_id)
        if not df.empty:
            all_data_frames.append(df)
            print(f"Successfully processed {len(df)} records for {coin_id}.")
        else:
            print(f"No data processed for {coin_id}.")

    if not all_data_frames:
        print("No data collected across all coins. Exiting.")
        return

    final_df = pd.concat(all_data_frames, ignore_index=True)

    # Convert pandas DataFrame to a list of tuples for Snowflake insertion
    # Ensure column order matches your Snowflake table
    # Convert 'date' column to string for direct insertion, or to datetime if Snowflake handles it directly
    final_df['date'] = final_df['date'].astype(str)

    # Define the Snowflake table structure. This is crucial for consistency.
    # Note: Snowflake typically infers types well, but explicit definition helps.
    columns_to_load = ['coin_id', 'date', 'price', 'market_cap', 'total_volume', 'extraction_timestamp']
    data_to_load = [tuple(row) for row in final_df[columns_to_load].itertuples(index=False)]

    if data_to_load:
        print(f"Loading {len(data_to_load)} records into Snowflake table {database_name}.{schema_name}.{table_name}...")
        # Use a temporary stage for bulk loading, or insert directly if the volume is small.
        # For large data, consider using SnowflakeHook's `copy_into_table` method with a staged file.
        # For simplicity, let's use a direct insert for now.
        # Note: This is an OVERWRITE strategy for demonstration. For production, consider incremental loads or UPSERTs.

        # Create table if not exists (only if you want the hook to manage schema)
        # Or, create the table manually in Snowflake first.
        # Manual table creation is generally preferred for production.
        # CREATE TABLE CRYPTO_DB.RAW.COINGECKO_HISTORICAL_PRICES (
        #     coin_id VARCHAR,
        #     date DATE,
        #     price FLOAT,
        #     market_cap FLOAT,
        #     total_volume FLOAT,
        #     extraction_timestamp TIMESTAMP_LTZ
        # );

        # Load data using pandas_to_snowflake. The hook's run method might be more flexible.
        # Alternatively, use a custom SQL INSERT or COPY INTO from a Pandas DataFrame saved to a temporary file.

        # Let's use the execute method with a list of tuples and manage the table creation/truncation ourselves.
        # This gives more control over the schema and ensures atomicity.
        try:
            with snowflake_hook.get_conn() as conn:
                with conn.cursor() as cur:
                    # Truncate table before inserting (for full refresh)
                    cur.execute(f"TRUNCATE TABLE {database_name}.{schema_name}.{table_name}")

                    # Prepare the insert statement
                    placeholders = ', '.join(['%s'] * len(columns_to_load))
                    column_names = ', '.join(columns_to_load)
                    insert_sql = f"INSERT INTO {database_name}.{schema_name}.{table_name} ({column_names}) VALUES ({placeholders})"

                    # Execute batch insert
                    cur.executemany(insert_sql, data_to_load)
                    conn.commit()
            print(f"Successfully loaded {len(data_to_load)} records to Snowflake.")
        except Exception as e:
            print(f"Error loading data to Snowflake: {e}")
            raise # Re-raise to fail the Airflow task
    else:
        print("No data to load into Snowflake.")

if __name__ == "__main__":
    # Example local run
    # For actual Airflow execution, the function extract_and_load_coingecko_data will be called by PythonOperator
    print("Running local test extraction...")
    # Make sure your Snowflake connection details are set as environment variables for local testing
    # Or mock the SnowflakeHook for isolated testing
    # This block won't be run by Airflow, but useful for testing the script's core logic
    extract_and_load_coingecko_data(
        coin_ids=['bitcoin'],
        vs_currency='usd',
        # You might need to set up a dummy Snowflake connection or mock the hook for local script testing outside Airflow
    )
    print("Local test extraction complete.")