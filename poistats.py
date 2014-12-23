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


if '__main__' == __name__:
    connection = myconn.Connection(host='localhost', database='postgis', user='postgres', password='ronald')
    poi_table = 'bdditu1'
    parcle_table = 'currentland'
    t_result = 'poi_in_parcle'
    buffer_radius = 5.0
    check_table(connection, t_result)
    sql = 'create table %s as select a.gid as poly_gid,b.gid as point_gid from %s a,%s b ' \
          'where a.geom && b.geom ' \
          'and st_within(b.geom, st_buffer(a.geom,%f))' % (t_result, parcle_table, poi_table, buffer_radius)
    curtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'start computing:%s' % curtime
    connection.execute(sql)
    connection.commit()
    curtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'end computing:%s' % curtime
