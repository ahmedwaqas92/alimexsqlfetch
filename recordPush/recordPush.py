import sys
sys.path.append("../")
from libraries.libraries import *
from connection.connection import dbConnection




class salesOrderPush():
    def __init__(self):
        pass
    def testConnection():
        try:
            connection = dbConnection.connect()
            cursor = connection.cursor()
            cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables = cursor.fetchall()
            for table in tables:
                print(table.TABLE_NAME)
        except pyodbc.Error as err:
            print(f"Error: {err}")
        finally:
            connection.close()
    def getColumnNames(*tableNames):
        try:
            column_names = {}
            connection = dbConnection.connect()
            cursor = connection.cursor()
            
            for tableName in tableNames:
                cursor.execute("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?", tableName)
                columns = cursor.fetchall()
                column_names[tableName] = [column.COLUMN_NAME for column in columns]
                
            return column_names
        
        except pyodbc.Error as err:
            print(f"Error: {err}")
        
        finally:
            connection.close()


tables = salesOrderPush.getColumnNames("QT", "QTDTL", "SO", "SODTL", "DO", "DODTL", "IV", "IVDTL")
print(tables["SO"])


# cluster = Cluster(['34.45.131.69'], port=9042)
# session = cluster.connect()

# try:
#     row = session.execute("SELECT * FROM system_schema.keyspaces")
#     for item in row:
#         print(item)
# except Exception as e:
#     print(f"An error occurred: {e}")
# finally:
#     session.shutdown()
#     cluster.shutdown()