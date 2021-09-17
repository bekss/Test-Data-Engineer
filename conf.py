import os

from dotenv import load_dotenv


load_dotenv()

user = os.getenv('USER')                # User for SQL
password = os.getenv('PASSWORD')        # Password for SQL
database = os.getenv('DATABASE')        # Database for SQL
host = os.getenv('HOST')                # Host for SQL

psql_port = os.getenv('PSQL_PORT')      # Port for Postgresql
mssql_port = os.getenv('MSSQL_PORT')    # Port for Microsoft SQL Server
mysql_port = os.getenv('MYSQL_PORT')    # Port for Mysql Server