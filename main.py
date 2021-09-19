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
            result = mssql_engine.execute("select *from famile where name = 'beksultan'")
            for row in result:
                print(row)

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
            postgres_result = self.postgres_engine.execute("EXPLAIN ANALYZE select *from users")
            c = 0
            for row in postgres_result:
                if c == 2:
                    a = list()
                    a.append(row)
                    d = dict(a[0])
                    print(d['QUERY PLAN'])
                c += 1
                # a = list(row)
                # print(postgres_result)
                # print(dict(row))
            # query = postgres_engine.execute("")

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
            postgres_result = self.mysql_engine.execute("select *from famile")
            for result in postgres_result:
                print(result)

    def create_table_for_all_server(self, max_row: int = None,
                                    max_value: int = None,
                                    unique_count_value: int = None,
                                    sql_server: str = None,
                                    table_name1: str = None):
        if sql_server == "mysql":
            table = self.mysql_engine.execute(" SHOW TABLES FROM data LIKE '{}'".format(table_name1))
            exists_table = list(table)
            # print(len(exists_table))

            if len(exists_table) == 0 and table_name1 != exists_table:
                mysql_result = self.mysql_engine.execute(
                    "create table {} (id int primary key not null  AUTO_INCREMENT, number bigint(10) not null);".format(table_name1))
                mysql_result = self.mysql_engine.execute(
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

            # tablee = self.mssql_engine.execute(
            #     "IF EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'fafmile') BEGIN PRINT 'Table Exists' END")
            table = self.mssql_engine.table_names()
            print(table)
            check = sorted(set([table_name1]) - set(table))
            if check:
                mssql_result = self.mssql_engine.execute(
                    "create table  {} (id int IDENTITY(1,1) PRIMARY KEY,number bigint not null);".format(table_name1))
                mssql_result = self.mssql_engine.execute(
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

        values_to_insert = [[None]] * 2
        old = [random.randint(0, max_value) for _ in range(max_row - unique_count_value)]
        values_to_insert[0] = old
        new_not_insert = [random.randint(0, max_value) for _ in range(unique_count_value)]
        # second_list = list()
        second_query = self.mysql_engine.execute("select number from {}".format(table_name_to_insert))
        if not list(second_query):
            # for row in second_query:
            #     # print(row[0])
            #     second_list.append(row[0])
            values_to_insert[1] = sorted(set(new_not_insert) - set(values_to_insert[0]))
            print(values_to_insert[0])
            print("Уникальные значения: ", values_to_insert[1])

            before = time()
            for count in range(len(values_to_insert[0])):
                mysql_result = self.mysql_engine.execute(
                    "insert  {} (number) values ('{}')".format(table_name_to_insert, values_to_insert[0][count]))
                mysql_result = self.mysql_engine.execute(
                    "insert  {} (number) values ('{}')".format(table_name_to_insert + '1',
                                                                   values_to_insert[0][count]))
            for count in range(len(values_to_insert[1])):
                mysql_result1 = self.mysql_engine.execute(
                    "insert  {} (number ) values ('{}')".format(table_name_to_insert + '1',
                                                                    values_to_insert[1][count]))
                mysql_result1 = self.mysql_engine.execute(
                    "insert {} (number ) values ('{}')".format(table_name_to_insert, values_to_insert[1][count]))
            after = time()
            print("Время исполнения вставки в Mysql", after - before, "s")
            self.mysql_engine.execute("CREATE INDEX number_index ON {} (id, number);".format(table_name_to_insert))

    def insert_psql(self, table_name_to_insert: int,
                     max_value: int,
                     max_row: int,
                     unique_count_value: int):

        values_to_insert = [[None]] * 2
        old = [random.randint(0, max_value) for _ in range(max_row - unique_count_value)]
        values_to_insert[0] = old
        new_not_insert = [random.randint(0, max_value) for _ in range(unique_count_value)]
        # second_list = list()
        second_query = self.postgres_engine.execute("select number from {}".format(table_name_to_insert))
        print(list(second_query))
        if not list(second_query):
            # for row in second_query:
            #     # print(row[0])
            #     second_list.append(row[0])
            values_to_insert[1] = sorted(set(new_not_insert) - set(values_to_insert[0]))
            print(values_to_insert[0])
            print("Уникальные значения: ", values_to_insert[1])

            before = time()
            for count in range(len(values_to_insert[0])):
                mysql_result = self.postgres_engine.execute(
                    "insert into {} (number) values ('{}')".format(table_name_to_insert, values_to_insert[0][count]))
                mysql_result = self.postgres_engine.execute(
                    "insert into {} (number) values ('{}')".format(table_name_to_insert + '1',
                                                                   values_to_insert[0][count]))
            for count in range(len(values_to_insert[1])):
                mysql_result1 = self.postgres_engine.execute(
                    "insert into {} (number ) values ('{}')".format(table_name_to_insert + '1',
                                                                    values_to_insert[1][count]))
                mysql_result1 = self.postgres_engine.execute(
                    "insert into {} (number ) values ('{}')".format(table_name_to_insert, values_to_insert[1][count]))
            after = time()
            self.postgres_engine.execute("CREATE INDEX number_index ON {} ( id, number);".format(table_name_to_insert))
            print("Время исполнения вставки в Postgresql ", after - before, "s")

    def insert_mssql(self, table_name_to_insert: int,
                     max_value: int,
                     max_row: int,
                     unique_count_value: int):
        values_to_insert = [[None]] * 2
        old = [random.randint(0, max_value) for _ in range(max_row - unique_count_value)]
        values_to_insert[0] = old
        new_not_insert = [random.randint(0, max_value) for _ in range(unique_count_value)]
        second_list = list()
        second_query = self.mssql_engine.execute("select number from {}".format(table_name_to_insert))
        print(list(second_query))
        if not list(second_query):
            # for row in second_query:
            #     # print(row[0])
            #     second_list.append(row[0])
            values_to_insert[1] = sorted(set(new_not_insert) - set(values_to_insert[0]))
            print(values_to_insert[0])
            print("Уникальные значения: ", values_to_insert[1])

            before = time()
            for count in range(len(values_to_insert[0])):
                mysql_result = self.mssql_engine.execute(
                    "insert into {} (number) values ('{}')".format(table_name_to_insert, values_to_insert[0][count]))
                mysql_result = self.mssql_engine.execute(
                    "insert into {} (number) values ('{}')".format(table_name_to_insert + '1',
                                                                   values_to_insert[0][count]))
            for count in range(len(values_to_insert[1])):
                mysql_result1 = self.mssql_engine.execute(
                    "insert into {} (number ) values ('{}')".format(table_name_to_insert + '1',
                                                                    values_to_insert[1][count]))
                mysql_result1 = self.mssql_engine.execute(
                    "insert into {} (number ) values ('{}')".format(table_name_to_insert, values_to_insert[1][count]))
            after = time()
            self.mssql_engine.execute("CREATE INDEX number_index ON {} (id, number);".format(table_name_to_insert))
            print("Время исполнения вставки в MSSQL  ", after - before, "s")

    def time_thing(self, connect_to_database, des="Отправка запроса на MSSQL SERVER начало времени"):
        print("Running %s " % des, time.time())
        now = time.time()

        try:
            ret = connect_to_database()
            return ret
        finally:
            spent = time.time() - now
            print("Finished %s, took %d seconds" % (des, spent))

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
    server = Sql_Test()
    # server.create_table_for_all_server(sql_server='mysql', table_name1='sam', max_value=10000, max_row=100,unique_count_value=10)
    # server.create_table_for_all_server(sql_server='postgresql', table_name1='dam', max_value=10000, max_row=100,unique_count_value=10)
    # server.create_table_for_all_server(sql_server='mssql', table_name1='dam', max_value=10000, max_row=100,unique_count_value=10)

    # server.insert_psql('famile')
    # server.insert_mssql('famile')
    # server.insert_mysql('famile')

# loop = asyncio.get_event_loop()
# loop.run_until_complete(connect_to_database())
