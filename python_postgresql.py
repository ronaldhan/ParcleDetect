# -*- coding:utf-8 -*-
import psycopg2
import logging


class Connection(object):

    def __init__(self, host, database, user=None, password=None, port=5432):
        self.host = host
        self.database = database

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

    def rollback(self):
        if self.conn is not None:
            try:
                self.conn.rollback()
            except Exception, e:
                logging.error("Can not rollback")


