# -*- coding:utf-8 -*-
import psycopg2
import logging
import itertools


class Connection(object):

    def __init__(self, host, database, user=None, password=None, port=5432):
        self.host = host
        self.database = database
        args = dict(database=database, user=user, password=password, host=host, port=port)
        self._args = args

        try:
            self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        except:
            logging.error("Cannot connect to PostgreSQL on %s", self.host,
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

    def executemany(self, query, parameters):
        """Executes the given query against all the given param sequences.
        We return the lastrowid from the query.
        """
        cursor = self._cursor()
        try:
            cursor.executemany(query, parameters)
            return cursor.lastrowid
        finally:
            cursor.close()

    def query(self, query, *parameters):
        """Returns a row list for the given query and parameters."""
        cursor = self._cursor()
        try:
            self._execute(cursor, query, parameters)
            column_names = [d[0] for d in cursor.description]
            return [Row(itertools.izip(column_names, row)) for row in cursor]
        finally:
            cursor.close()

    def _execute(self, cursor, query, parameters):
        try:
            return cursor.execute(query, parameters)
        except OperationalError:
            logging.error("Error connecting to PostgreSQL on %s", self.host)
            self.close()
            raise


class Row(dict):
    """A dict that allows for object-like property access syntax."""
    def __getattr__(self, name):
        try:
            return self[name]
        except KeyError:
            raise AttributeError(name)
