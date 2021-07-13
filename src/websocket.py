import time
import os
from queue import Queue
from threading import Thread

from alpaca_trade_api import Stream
from alpaca_trade_api.common import URL
import configparser

async def print_quote(quote):
    print('quote', quote)

def setup_connection():
    #needs to be run twice in order to move up a directory
    dirname = os.path.dirname(os.path.dirname(__file__))
    propfile = os.path.join(dirname, 'config.properties')

    config = configparser.RawConfigParser()
    config.read(propfile)
    public_key = config.get('API', 'alpaca_public_key')
    secret_key = config.get('API', 'alpaca_secret_key')

    conn = Stream(
        public_key,
        secret_key,
        base_url=URL('https://paper-api.alpaca.markets'),
        data_feed='iex'
    )
    conn.subscribe_quotes(print_quote, 'AAPL')
    conn.run()

    return conn

def run_connection(conn):
    try:
        conn.run()
    except Exception as e:
        print(f'Exception from websocket connection: {e}')
    finally:
        print("Trying to re-establish connection")
        time.sleep(3)
        run_connection(conn)