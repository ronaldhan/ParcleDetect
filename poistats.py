# -*- coding:utf-8 -*-
import python_postgresql as myconn
import time


def check_table(conn, tablename):
    #判断表是否存在
    sql = "select count(*) as count from pg_class where relname = '%s'" % tablename
    rexist = conn.query(sql)
    if int(rexist[0]['count']) != 0:
        sql = "drop table '%s'" % tablename
        connection.execute(sql)
        connection.commit()


def build_pair(result_table, polygon_table, point_table, radius=5.0):
    sql = 'create table %s as select a.gid as poly_gid,a.bz_new,a.utl,' \
          'b.gid as point_gid,b.name,b.catalog,b.subcatalog from %s a,%s b ' \
          'where a.geom && b.geom ' \
          'and st_within(b.geom, st_buffer(a.geom,%f))' % (result_table, polygon_table, point_table, radius)
    curtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'build polygon-point pair start computing:%s' % curtime
    connection.execute(sql)
    connection.commit()
    curtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'build polygon-point pair end computing:%s' % curtime


def parcle_change(conn, parcle_change_table):


if '__main__' == __name__:
    connection = myconn.Connection(host='localhost', database='postgis', user='postgres', password='ronald')
    poi_table = 'bdditu1'
    parcle_table = 'currentland'
    t_result = 'poi_in_parcle'
    buffer_radius = 5.0
    check_table(connection, t_result)
    build_pair(t_result, parcle_table, poi_table, buffer_radius)
    parcle_change(connection, t_result)