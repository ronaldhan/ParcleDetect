# -*- coding:utf-8 -*-
import psycopg2
import logging


class Connection(object):

    def __init__(self, host, database, user=None, password=None, port=5432):
        self.host = host
        self.database = database
        args = dict(database=database, user=user, password=password, host=host, port=port)
        self._args = args

        try:
            self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        except:
            logging.error("Cannot connect to MySQL on %s", self.host,
                          exc_info=True)

    def __del__(self):
        self.close()

    def close(self):
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def commit(self):
        if self.conn is not None:
            try:
                self.conn.commit()
            except Exception, e:
                self.conn.rollback()
                logging.exception("Can not commit", e)

    def reconnect(self):
        self.conn.close()
        self.conn = psycopg2.connect(**self._args)

    def rollback(self):
        if self.conn is not None:
            try:
                self.conn.rollback()
            except Exception, e:
                logging.error("Can not rollback")

    def _cursor(self):
        if self.conn is None: self.reconnect()
        return self.conn.cursor()

    def execute(self, query, *parameters):
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()
    def _execute(self, cursor, query, parameters):
        try:
            return cursor.execute(query, parameters)
        except OperationalError:
            logging.error("Error connecting to MySQL on %s", self.host)
            self.close()
            raise
