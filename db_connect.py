import getpass
import mysql.connector
import db_config
from mysql.connector import Error


def connect_db(db_name):
    config = db_config.config
    #print('db_name', db_name)
    # test_conn(config)

    while 1:
        print('Please provide the Database credentials')
        user_name = input('Enter User Name \n')
        #pass_word = getpass.getpass(prompt='Enter Password', stream=None)
        try:
            pass_word = getpass.getpass(
                prompt='Enter Password \n ', stream=None)
        except Exception as error:
            print('ERROR', error)
        else:
            print('Password entered:', pass_word)
            config[db_name]['user'] = user_name
            config[db_name]['password'] = pass_word
            db_test = test_conn(config[db_name])
            if db_test == 'S':
                return 'S'
                break
            else:
                print('Error in connection to DB, Please check DB connection details')


""" def test_db_connection(db_conn):
    print('Testing Database Connection')
    try:
        cnx = mysql.connector.connect(**db_conn)
        cnx.close()
        print('Connection Successful')
        return 'S'
    except:
        print('Connection Failed')
        return 'E' """


def test_conn(conn):
    try:
        connection = mysql.connector.connect(**conn)
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute("select database();")
            record = cursor.fetchone()
            print("You're connected to database: ", record)
            cursor.close()
            connection.close()
            print("MySQL connection is closed")
            return 'S'

    except Error as e:
        print("Error while connecting to MySQL", e)
        return 'E'
