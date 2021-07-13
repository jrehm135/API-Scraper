import mysql.connector
from mysql.connector import errorcode
import configparser
import os

def initDB():
    #needs to be run twice in order to move up a directory
    dirname = os.path.dirname(os.path.dirname(__file__))
    propfile = os.path.join(dirname, 'config.properties')

    config = configparser.RawConfigParser()
    config.read(propfile)
    db_username = config.get('DATABASE_CONF', 'username')
    db_password = config.get('DATABASE_CONF', 'password')
    try:
        cnx = mysql.connector.connect(user=db_username,
                                      password=db_password,
                                      database='adb')
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        else:
            print(err)
    else:
        return cnx

def createTable(dbconn, ticker):
    mycursor = dbconn.cursor()

    mycursor.execute('CREATE TABLE IF NOT EXISTS `' + ticker + '_2year_history` (`cur_datetime` DATETIME,'
                                                               '`open` DECIMAL(10, 2),'
                                                               '`close` DECIMAL(10, 2),'
                                                               '`high` DECIMAL(10, 2),'
                                                               '`low` DECIMAL(10, 2),'
                                                               '`volume` INT)')

def insertDB(dbconn, ticker, dataset):
    mycursor = dbconn.cursor()

    q = " INSERT IGNORE INTO " + ticker + "_2year_history (cur_datetime, open, high, low, close, volume) VALUES (%s, %s, %s, %s, %s, %s)"

    try:
        mycursor.executemany(q, dataset)
        dbconn.commit()
        print('Database insert successful')
    except:
        dbconn.rollback()
        print('Database insert failed')
