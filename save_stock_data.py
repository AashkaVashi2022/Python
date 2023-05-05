import psycopg2
from psycopg2.extras import execute_values
import datetime


def save_minute_data(data):
    try:
        # Establish a connection to the database
        conn = psycopg2.connect(
            database="*********",
            user="*********",
            password="*********",
            host="*********",
            port="*********"
        )
    except psycopg2.Error as e:
        print("Unable to connect to the database")
        print(e)
        exit(1)

    # Use a context manager to automatically close the cursor and connection when execution is complete or an error occurs
    with conn.cursor() as cur:
        # SQL statement for inserting the data into the table
        sql_1 = '''INSERT INTO minute_data 
                (instrument_identifier, exchange, last_trade_time,traded_qty,
                 open_interest, open, high, low, close, token_number, symbol,
                 segment, expiry, strike_price, segment_type) 
                 VALUES %s ON CONFLICT DO NOTHING'''

        sql_2 = '''INSERT INTO pre_2_day_minute_data 
                (instrument_identifier, exchange, last_trade_time,traded_qty,
                 open_interest, open, high, low, close, token_number, symbol,
                 segment, expiry, strike_price, segment_type)
                 VALUES %s ON CONFLICT DO NOTHING'''

        # Define your UPDATE statement
        update_query = '''UPDATE last_update_time SET bucket = (SELECT max(last_trade_time) FROM public.pre_2_day_minute_data)'''

        # Convert the list of dictionaries into a list of tuples
        values = [(
            d["InstrumentIdentifier"], d["Exchange"],
            datetime.datetime.fromtimestamp(d["LastTradeTime"]).strftime(
                '%Y-%m-%d %H:%M:%S %z'), d["TradedQty"],
            d["OpenInterest"], d["Open"],
            d["High"], d["Low"],
            d["Close"], d["TokenNumber"] if d["TokenNumber"] != "" else 0,
            d["InstrumentIdentifier"].split("_")[1],
            d["InstrumentIdentifier"].split("_")[0],
            d["InstrumentIdentifier"].split("_")[2],
            d["InstrumentIdentifier"].split("_")[4],
            d["InstrumentIdentifier"].split("_")[3],
        ) for d in data]

        try:
            # Insert the data into the table using parameterized queries and the execute_values method
            execute_values(cur, sql_1, values)
            execute_values(cur, sql_2, values)

            # Execute the UPDATE statement using the same cursor object
            cur.execute(update_query)

            conn.commit()
            conn.close()

        except psycopg2.Error as e:
            print("Unable to insert data into the table")
            print(e)
            conn.rollback()
            exit(1)

def generate_unix_timestamps(date):
    timestamps = []
    start = datetime.datetime(date.year, date.month, date.day, 9, 0)
    end = datetime.datetime(date.year, date.month, date.day, 15, 30)
    current = start
    while current <= end:
        timestamps.append(int(current.timestamp()))
        current += datetime.timedelta(minutes=5)
    return timestamps

