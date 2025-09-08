import requests
import snowflake.connector
from datetime import datetime

def fetch_prices():
    url = "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum&vs_currencies=usd"
    res = requests.get(url)
    data = res.json()

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.environ['SNOWFLAKE_USER'],
        password=os.environ['SNOWFLAKE_PASSWORD'],
        account=os.environ['SNOWFLAKE_ACCOUNT'],
        ...
    )
    cur = conn.cursor()
    timestamp = datetime.utcnow()

    for coin, value in data.items():
        price = value['usd']
        cur.execute("""
            INSERT INTO RAW.CRYPTO_PRICES (timestamp, coin, usd_price)
            VALUES (%s, %s, %s)
        """, (timestamp, coin, price))

    conn.commit()
# DAG code will go here
