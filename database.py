import mysql.connector
import datetime
import re
import sys

try:
    testFile = open(sys.argv[1], 'r')
except:
    print("Exception Occurred: ", sys.exc_info()[0])

testfileLines = testFile.readlines()

testFile.close()

headers = testfileLines[0].split(',')
headers[-1] = headers[-1].strip('\n')

my_db = mysql.connector.connect(
    host="localhost",
    user="root",
    database="test_db"
)

mycursor = my_db.cursor(prepared=True)


def printProgressBar(iteration, total, prefix='', suffix='', decimals=1, length=50, fill='█'):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : suffix string (Str)
        decimals    - Optional  : positive number of decimals in percent complete (Int)
        length      - Optional  : character length of bar (Int)
        fill        - Optional  : bar fill character (Str)
    """
    percent = ("{0:." + str(decimals) + "f}").format(100 *
                                                     (iteration / float(total)))
    filledLength = int(length * iteration // total)
    bar = fill * filledLength + '-' * (length - filledLength)
    print('{} |{}| {}% {}'.format(prefix, bar, percent, suffix), end='\r')
    # Print New Line on Complete
    if iteration == total:
        print()


def createDatabase(databaseName):
    mycursor.execute("CREATE DATABASE " + databaseName)
    # print("Database Created: ", databaseName)


def createTable(tableName):
    sqlCreate = "CREATE TABLE " + tableName

    sqlTableElements = """(
        {} INT NOT NULL AUTO_INCREMENT PRIMARY KEY,
        {} VARCHAR(8) NOT NULL,
        {} VARCHAR(128) NOT NULL,
        {} VARCHAR(3) NOT NULL,
        {} INT NOT NULL
    );""".format(*headers)
    sql = sqlCreate + sqlTableElements
    print(sql)
    mycursor.execute(sql)

    # print("Table Created", tableName)


def clearTable(tableName):
    clearSql = "TRUNCATE TABLE " + tableName

    mycursor.execute(clearSql)

    my_db.commit()

    # print("Table Reset: ", tableName)


def deleteTable(tableName):
    mycursor.execute("DROP TABLE " + tableName)
    # print("Table Deleted: ", tableName)


def insertRecord(tableName):
    sqlInsert = "INSERT INTO " + tableName
    sqlHeaderNames = "(" + ", ".join(headers) + ")"
    sqlValues = " VALUES (" + ", ".join(['%s'] * len(headers)) + ")"
    sql = sqlInsert + sqlHeaderNames + sqlValues
    return sql


def convertCSVtoSQL(tableName):
    numLines = len(testfileLines[1:])

    print("Migrating CSV to SQL Database")

    for idx, record in enumerate(testfileLines[1:]):
        record_list = record.split(",")
        if(record_list[0] == ""):
            break

        mycursor.execute(insertRecord(tableName), tuple(record_list))

        my_db.commit()

        printProgressBar(idx + 1, numLines,
                         prefix="Progress:", suffix="Complete", length=50)

    print("Conversion Complete")


class DatabaseMigrator:
    database = None
    cursor = None
    table = None

    def connectToDatabase(self, db_host, db_user, db_name):
        database = mysql.connector.connect(
            host=db_host, user=db_user, database=db_name)
        cursor = database.cursor(prepared=True)
        pass


createTable("location")

clearTable("location")

convertCSVtoSQL("location")
