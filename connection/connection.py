import sys
sys.path.append("../")
from libraries.libraries import *
from configuration.config import configFile

class dbConnection():
    def __init__(self):
        pass
    def connect():
        config = configFile.connectionString()
        connection_string = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={config["server"]};DATABASE={config["database"]};UID={config["username"]};PWD={config["password"]}'
        connection = pyodbc.connect(connection_string)
        return connection
