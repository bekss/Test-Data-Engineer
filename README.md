Тестовое задание на позицию Test Data Engineer.

###  Settings for MSSQL SERVER:
create_engine(‘mssql+pyodbc://server_name/database_name?driver=SQL Server?Trusted_Connection=yes’)
server_name : server you want to connect to
database_name : database you want to work with
Trusted_Connection = yes, when using windows authentication. If you have set a separate username and password for your SQL database,
sal.create_engine(‘dialect+driver://username:password@host:port/database’)