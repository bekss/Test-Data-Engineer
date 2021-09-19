import os

from dotenv import load_dotenv


load_dotenv()

database = os.getenv('DATABASE')        # Database for SQL
host = os.getenv('HOST')

mssql_user = os.getenv('MSSQL_USER')
mssql_passowrd = os.getenv('MSSQL_PASSWORD')
mssql_port = os.getenv('MSSQL_PORT')    # Port for Microsoft SQL Server
mssql_server_name = os.getenv('MSSQL_SERVER_NAME')


psql_user = os.getenv('PSQL_USER')                # User for SQL
psql_password = os.getenv('PSQL_PASSWORD')        # Password for SQL              # Host for SQL
psql_port = os.getenv('PSQL_PORT')      # Port for Postgresql

mysql_user = os.getenv('MYSQL_USER')
mysql_password = os.getenv('MYSQL_PASSWORD')
mysql_port = os.getenv('MYSQL_PORT')    # Port for Mysql Server