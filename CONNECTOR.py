
import psycopg2
from environs import Env
from psycopg2 import extras

env = Env()
env.read_env()

class DbPostgres:
    __instance = None
    DB_NAME = env.str('DB_NAME')
    DB_USER = env.str('DB_USER')
    PASSWORD = env.str('PASSWORD')
    HOST = env.str('HOST')

    def __new__(cls, *args, **kwargs):
        if cls.__instance is None:
            cls.__instance = super().__new__(cls)
        return cls.__instance

    def fetch_one(self, query, arg=None, factory=None, clean=None):

        try:
            with self.__connection(factory) as cur:
                self.__execute(cur, query, arg)
                return self.__fetch(cur, clean)
        except(Exception, psycopg2.Error) as error:
            self.__error(error)

    def fetch_all(self, query, arg=None, factory=None):
        try:
            with self.__connection(factory) as cur:
                self.__execute(cur, query, arg)
                return cur.fetchall()
        except(Exception, psycopg2.Error) as error:
            self.__error(error)

    def query_update(self, query, arg=None, message='ок'):
        """Выполняет запросы на обновление, создание  и тд"""
        try:
            with self.__connection() as cur:
                self.__execute(cur, query, arg)
                return message
        except(Exception, psycopg2.Error) as error:
            self.__error(error)

    @classmethod
    def __connection(cls, factory=None):
        """Инициализатор соединения"""
        conn = psycopg2.connect(
            dbname=cls.DB_NAME,
            user=cls.DB_USER,
            password=cls.PASSWORD,
            host=cls.HOST
        )
        conn.autocommit = True

        if factory == 'dict':
            """Возвращает данные в виде ключ:значение"""
            cur = conn.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
        elif factory == 'list':
            """Возвращает данные в виде списка"""
            cur = conn.cursor(
                cursor_factory=psycopg2.extras.DictCursor
            )
        else:
            cur = conn.cursor()
            """Возвращает данные в виде кортежа"""
        return cur

    @staticmethod
    def __execute(cur, query, arg=None):
        """Метод __execute всегда возвращает None"""
        if arg:
            cur.execute(query, arg)
        else:
            cur.execute(query)

    @staticmethod
    def __fetch(cur, clean):
        """Если запрос выполнен успешно, получим данные с помощью fetchone"""
        if clean == 'no':
            fetch = cur.fetchone()
        else:
            fetch = cur.fetchone()[0]
        return fetch

    @staticmethod
    def __error(error):
        """В том числе, если в БД нет данных"""
        print(error)

