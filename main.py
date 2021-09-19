import asyncio
import asyncpg
import pyodbc
import psycopg2
import random
import sqlalchemy as db

from sqlalchemy import create_engine
from faker import Faker
from conf import *
from time import time
from sqlalchemy import func


class Sql_Test:
    def __init__(self):
        self.mssql_engine = db.create_engine(
            f'mssql+pyodbc://{mssql_user}:{mssql_passowrd}@{host}:{mssql_port}/{database}?driver=ODBC Driver 17 for SQL Server')
        self.postgres_engine = db.create_engine(
            'postgresql+psycopg2://{}:{}@{}/{}'.format(psql_user, psql_password, host, database))
        self.mysql_engine = db.create_engine(
            'mysql+pymysql://{}:{}@{}/{}'.format(mysql_user, mysql_password, host, database))

    def connect_to_mssql_server(self):
        """
        For connect to MSSQL Server
        :return:
        """
        try:
            self.mssql_engine.connect()
        except:
            raise Exception(
                "Ваш пароль {} или пароль {} и.т.др не верны. Пожалуйста насторойте конфигурационный файл ".format(
                    mssql_user, mssql_passowrd))
        else:
            mssql_engine = db.create_engine(
                f'mssql+pyodbc://{mssql_server_name}/{database}?driver=ODBC Driver 17 for SQL Server')
        if mssql_engine.connect():
            print('Успешно соединено')


    def connect_to_postgres_server(self):
        """
        For connect to Postgresql server
        :return:
        """
        try:
            self.postgres_engine.connect()
        except:
            raise Exception(
                "Ваш пароль {} или пароль {} и.т.др не верны. Пожалуйста насторойте конфигурационный файл ".format(
                    psql_user, psql_password))

        if self.postgres_engine.connect():
            print('Успешно соединено')

    def connect_to_mysql_server(self):
        """
        For connect to Mysql server
        :return:
        """
        try:

            self.mysql_engine.connect()
        except:
            raise Exception(
                "Ваш пароль {} или пароль {} и.т.др не верны. Пожалуйста насторойте конфигурационный файл ".format(
                    mysql_user, mysql_password))
        if self.mysql_engine.connect():
            print('Успешно соединено')

    def create_table_for_all_server(self, max_row: int = None,
                                    max_value: int = None,
                                    unique_count_value: int = None,
                                    sql_server: str = None,
                                    table_name1: str = None):
        if sql_server == "mysql":
            table = self.mysql_engine.execute(" SHOW TABLES FROM data LIKE '{}'".format(table_name1))
            exists_table = list(table)

            if len(exists_table) == 0 and table_name1 != exists_table:
                self.mysql_engine.execute(
                    "create table {} (id int primary key not null  AUTO_INCREMENT, number bigint(10) not null);".format(table_name1))
                self.mysql_engine.execute(
                    "create table {} (id int primary key not null  AUTO_INCREMENT, number bigint(10) not null);".format(table_name1 +'1'))
                print("Успешно созданы ваши таблицы  {} и {} ".format(table_name1, table_name1 + '1'))

                self.insert_mysql(table_name1, max_value, max_row, unique_count_value)
            else:
                print('В Mysql таблица {} и {} уже существует. Пожалуйста выберите другое имя'.format(table_name1,
                                                                                                      table_name1 + '1'))

        elif sql_server == "postgresql":
            table = self.postgres_engine.execute(
                "select exists (select * from information_schema.tables where table_name = '{}' and table_schema = 'public');".format(
                    table_name1))
            exists_table = list(table)[0][0]

            if exists_table is not True:
                mysql_result = self.postgres_engine.execute(
                    "create table  {} (id  SERIAL PRIMARY KEY,number BIGINT not null);".format(table_name1))
                mysql_result = self.postgres_engine.execute(
                    "create table {} (id  SERIAL PRIMARY KEY,number BIGINT not null);".format(table_name1 +'1'))
                print("Успешно созданы ваши таблицы  {} и {} ".format(table_name1, table_name1 + '1'))
                self.insert_psql(table_name1, max_value, max_row, unique_count_value)
            else:
                print('В Postgresql таблица {} уже существует. Пожалуйста выберите другое имя '.format(table_name1))

        elif "mssql" in sql_server:
            pyodbc.pooling = False

            table = self.mssql_engine.table_names()
            print(table)
            check = sorted(set([table_name1]) - set(table))
            if check:
                self.mssql_engine.execute(
                    "create table  {} (id int IDENTITY(1,1) PRIMARY KEY,number bigint not null);".format(table_name1))
                self.mssql_engine.execute(
                    "create table {} (id int IDENTITY(1,1) PRIMARY KEY,number bigint not null);".format(table_name1 + '1'))
                print("Успешно созданы ваши таблицы  {} и {} ".format(table_name1, table_name1 + '1'))
                self.insert_mssql(table_name1, max_value, max_row, unique_count_value)
            else:
                print('В MSSQL сервере таблица {} уже существует. Пожалуйста выберите другое имя. '.format(table_name1))
        else:
            print("Выберите пожалуйтса, какой sql server использовать")

    def insert_mysql(self, table_name_to_insert: int,
                     max_value: int,
                     max_row: int,
                     unique_count_value: int):
        """
        Insert to Mysql SQL server a data
        :param table_name_to_insert:
        :param max_value:
        :param max_row:
        :param unique_count_value:
        :return:
        """

        values_to_insert = [[None]] * 2
        old = [random.randint(0, max_value) for _ in range(max_row - unique_count_value)]
        values_to_insert[0] = old

        new_not_insert = [random.randint(0, max_value) for _ in range(unique_count_value)]
        second_query = self.mysql_engine.execute("select number from {}".format(table_name_to_insert))
        if not list(second_query):
            values_to_insert[1] = sorted(set(new_not_insert) - set(values_to_insert[0]))
            print("Уникальные значения: ", values_to_insert[1])

            before = time()
            for count in range(len(values_to_insert[0])):
                self.mysql_engine.execute(
                "insert  {} (number) values ('{}')".format(table_name_to_insert, values_to_insert[0][count]))

                self.mysql_engine.execute(
                "insert  {} (number) values ('{}')".format(table_name_to_insert + '1',
                    values_to_insert[0][count]))

            for count in range(len(values_to_insert[1])):
                self.mysql_engine.execute(
                "insert  {} (number ) values ('{}')".format(table_name_to_insert + '1', values_to_insert[1][count]))
                self.mysql_engine.execute("insert {} (number ) values ('{}')".format(table_name_to_insert, values_to_insert[1][count]))

            after = time()
            print("Время исполнения вставки в Mysql", after - before, "s")
            self.mysql_engine.execute("CREATE INDEX number_index ON {} (id, number);".format(table_name_to_insert))

    def insert_psql(self, table_name_to_insert: int,
                     max_value: int,
                     max_row: int,
                     unique_count_value: int):
        """
        Insert to Postgresql SQL server a data
        :param table_name_to_insert:
        :param max_value:
        :param max_row:
        :param unique_count_value:
        :return:
        """

        values_to_insert = [[None]] * 2
        old = [random.randint(0, max_value) for _ in range(max_row - unique_count_value)]
        values_to_insert[0] = old
        new_not_insert = [random.randint(0, max_value) for _ in range(unique_count_value)]
        second_query = self.postgres_engine.execute("select number from {}".format(table_name_to_insert))
        if not list(second_query):
            values_to_insert[1] = sorted(set(new_not_insert) - set(values_to_insert[0]))
            print("Уникальные значения: ", values_to_insert[1])

            before = time()
            for count in range(len(values_to_insert[0])):
                self.postgres_engine.execute("insert into {} (number) values ('{}')".format(table_name_to_insert, values_to_insert[0][count]))
                self.postgres_engine.execute("insert into {} (number) values ('{}')".format(table_name_to_insert + '1',values_to_insert[0][count]))
            for count in range(len(values_to_insert[1])):
                self.postgres_engine.execute("insert into {} (number ) values ('{}')".format(table_name_to_insert + '1',values_to_insert[1][count]))
                self.postgres_engine.execute("insert into {} (number ) values ('{}')".format(table_name_to_insert, values_to_insert[1][count]))

            after = time()
            self.postgres_engine.execute("CREATE INDEX number_index ON {} ( id, number);".format(table_name_to_insert))
            print("Время исполнения вставки в Postgresql ", after - before, "s")

    def insert_mssql(self, table_name_to_insert: int,
                     max_value: int,
                     max_row: int,
                     unique_count_value: int):
        """
        Insert to MSSQL SQL server a data
        :param table_name_to_insert:
        :param max_value:
        :param max_row:
        :param unique_count_value:
        :return:
        """
        values_to_insert = [[None]] * 2
        old = [random.randint(0, max_value) for _ in range(max_row - unique_count_value)]
        values_to_insert[0] = old
        new_not_insert = [random.randint(0, max_value) for _ in range(unique_count_value)]
        second_query = self.mssql_engine.execute("select number from {}".format(table_name_to_insert))
        if not list(second_query):
            values_to_insert[1] = sorted(set(new_not_insert) - set(values_to_insert[0]))
            print("Уникальные значения: ", values_to_insert[1])

            before = time()
            for count in range(len(values_to_insert[0])):
                self.mssql_engine.execute("insert into {} (number) values ('{}')".format(table_name_to_insert, values_to_insert[0][count]))
                self.mssql_engine.execute("insert into {} (number) values ('{}')".format(table_name_to_insert + '1',values_to_insert[0][count]))
            for count in range(len(values_to_insert[1])):
                self.mssql_engine.execute("insert into {} (number ) values ('{}')".format(table_name_to_insert + '1',values_to_insert[1][count]))
                self.mssql_engine.execute("insert into {} (number ) values ('{}')".format(table_name_to_insert, values_to_insert[1][count]))
            after = time()
            self.mssql_engine.execute("CREATE INDEX number_index ON {} (id, number);".format(table_name_to_insert))
            print("Время исполнения вставки в MSSQL  ", after - before, "s")

    def select_count_table(self, table_name):
        """
        To find the count column number
        :param table_name:
        :return:
        """
        before = time("select count(*) from {}".format(table_name))
        self.mssql_engine.execute()
        after = time()
        print("Время исполнения запроса в MSSQL  ", after - before, "s")

        before = time()
        self.mysql_engine.execute("select count(*) from {}".format(table_name))
        after = time()
        print("Время исполнения запроса в Mysql  ", after - before, "s")

        before = time()
        self.postgres_engine.execute("select count(*) from {}".format(table_name))
        after = time()
        print("Время исполнения запроса в Postgres  ", after - before, "s")

    def select_max_table(self, table_name, column_name=None):
        """
        To find the max value
        Dont work a column choose.
        :param table_name:
        :return:
        """
        before = time("select max(number) from {}".format(table_name))
        self.mssql_engine.execute()
        after = time()
        print("Время исполнения запроса в MSSQL  ", after - before, "s")

        before = time()
        self.mysql_engine.execute("select max(number) from {}".format(table_name))
        after = time()
        print("Время исполнения запроса в Mysql  ", after - before, "s")

        before = time()
        self.postgres_engine.execute("select max(number) from {}".format(table_name))
        after = time()
        print("Время исполнения запроса в Postgres  ", after - before, "s")

    def select_from_to(self, value_from, value_to, table_name, table_column=None):
        """
        The table_column dont work!!!
        To get value from and to
        :param value_from:
        :param value_to:
        :param table_name:
        :return:
        """
        before = time
        self.mssql_engine.execute("select * from {} where number between {} and {}".format(table_name, value_from, value_to))
        after = time()
        print("Время исполнения запроса между значениями {} и {} в MSSQL  ".format(value_from, value_to), after - before, "s")

        before = time()
        self.mysql_engine.execute("select * from {} where number between {} and {}".format(table_name, value_from, value_to))
        after = time()
        print("Время исполнения запроса между значениями {} и {} в Mysql  ".format(value_from, value_to), after - before, "s")

        before = time()
        self.postgres_engine.execute("select * from {} where number between {} and {}".format(table_name, value_from, value_to))
        after = time()
        print("Время исполнения запроса между значениями {} и {} в Postgres  ".format(value_from, value_to), after - before, "s")

    def select_number_where(self, table_name, value):
        """
        Select where column name = value
        :param table_name:
        :return:
        """
        before = time
        self.mssql_engine.execute("select * from {} where number ={}".format(table_name, value))
        after = time()
        print("Время исполнения запроса знечение где number равен к {} в MSSQL   ".format(value), after - before, "s")

        before = time()
        self.mysql_engine.execute("select * from {} where number =  {}".format(table_name, value))
        after = time()
        print("Время исполнения запроса в где number равен к {}  Mysql  ".format(value), after - before, "s")

        before = time()
        self.postgres_engine.execute("select * from {} where number= {}".format(table_name, value))
        after = time()
        print("Время исполнения запроса в Postgres где number равен к {} ".format(value), after - before, "s")

    def in_both_table_number(self, table_name1, table_name2):
        """
        output where in two table have a value similar
        :param table_name1:
        :param table_name2:
        :return:
        """
        before = time
        self.mssql_engine.execute("select *from {} as d, {} as t where d.number = t.number".format(table_name1,table_name2))
        after = time()
        print("Время исполнения запроса присутствующие в обоих таблицах в MSSQL   ", after - before, "s")

        before = time()
        self.mysql_engine.execute("select *from {} as d, {} as t where d.number = t.number".format(table_name1,table_name2))
        after = time()
        print("Время исполнения запроса присутствующие в обоих таблицах в Mysql  ", after - before, "s")

        before = time()
        self.postgres_engine.execute("select *from {} as d, {} as t where d.number = t.number".format(table_name1,table_name2))
        after = time()
        print("Время исполнения запроса присутствующие в обоих таблицах в Postgres  ", after - before, "s")

    def choose_sql_server_name(self, sql_server_name, sql_server_name2=None):
        """
        :param sql_server_name:
        :return:
        Sql server names
        """
        switcher = {
            'mysql': 'mysql',
            'postgresql': 'postgresql',
            'mssql': 'mssql',
        }
        return switcher.get(sql_server_name)


if __name__ == '__main__':
    TABLE_NAME = 'new_table'
    server = Sql_Test()
    server.create_table_for_all_server(sql_server='mysql', table_name1=TABLE_NAME, max_value=10000, max_row=100, unique_count_value=10)
    server.create_table_for_all_server(sql_server='postgresql', table_name1=TABLE_NAME, max_value=10000, max_row=100, unique_count_value=10)
    server.create_table_for_all_server(sql_server='mssql', table_name1=TABLE_NAME, max_value=10000, max_row=100, unique_count_value=10)

    # server.select_from_to(1221, 2000,TABLE_NAME)
    # server.select_max_table(TABLE_NAME)
    # server.select_number_where(TABLE_NAME,344)
    # server.in_both_table_number(TABLE_NAME, TABLE_NAME+"1")