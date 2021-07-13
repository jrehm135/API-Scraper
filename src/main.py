# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.

import db_func
import websocket

import sys
import configparser
import time
import requests
import csv

def readAPI(dbconn, tickerList):
    config = configparser.RawConfigParser()
    config.read('config.properties')
    api_key = config.get('API', 'alphavantage_key')
    # iterate through 2 years by month
    for ticker in tickerList:
        tickerAccum = []
        for year_val in range(1, 3):
            for month_val in range(1, 13):
                print('Querying ' + ticker + ' historical data from ' + str(year_val - 1) + ' years ago, ' + str(month_val - 1) + ' months ago')
                request_string = 'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=' + ticker + '&interval=1min&slice=year' + str(year_val) + 'month' + str(month_val) + '&apikey=' + api_key

                for x in range(0, 6):  # try 6 times
                    try:
                        csv_response = requests.get(request_string)
                        #This means we have reached our maximum request limit
                        if csv_response.text.find('Note') != -1:
                            raise Exception
                        t_lines = csv_response.text.splitlines()
                        t_lines.pop(0)
                        m_reader = csv.reader(t_lines)
                        for row in m_reader:
                            tickerAccum.append(tuple(row))
                    except Exception as str_error:
                        print('Timeout, backing off for 10s')
                        time.sleep(10)
                        continue
                    break
        db_func.createTable(dbconn, ticker)
        db_func.insertDB(dbconn, ticker, tickerAccum)

if __name__ == '__main__':
    args = sys.argv

    if sys.argv[1] == '--current':
        conn = websocket.setup_connection()
        websocket.run_connection(conn)

    elif sys.argv[1] == '--scrape':
        if len(sys.argv) > 2:
            tickerList = sys.argv[2].split(',')
            adb = db_func.initDB()
            readAPI(adb, tickerList)
        else:
            print('A list of tickers to add must be supplied as comma-separated values')