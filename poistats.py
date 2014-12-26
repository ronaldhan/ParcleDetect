# -*- coding:utf-8 -*-
import python_postgresql as myconn
import time


def check_table(conn, tablename):
    #判断表是否存在
    sql = "select count(*) as count from pg_class where relname = '%s'" % tablename
    rexist = conn.query(sql)
    if int(rexist[0]['count']) != 0:
        sql = "drop table %s" % tablename
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


def parcle_change(conn, poi_in_parcle):
    sql = 'select distinct poly_gid, bz_new,utl from %s order by poly_gid asc' % poi_in_parcle
    pip = conn.query(sql)
    for row in pip:
        gid = row['poly_gid']
        bz_new = row['bz_new']
        utl = row['utl']
        sql = 'select catalog, count(*) as count from %s where poly_gid=%s ' \
              'group by subcatalog order by count dsc' % (poi_in_parcle, gid)
        groups = conn.query(sql)
        result = []
        poi = {}
        pair = {}
        kinds = []
        for group in groups:
            catalog = group['catalog']
            ccount = group['count']
            poi['catalog'] = catalog
            poi['count'] = ccount
            result.append(poi)
        pair['result'] = result
        pair['bz_new'] = bz_new
        pair['utl'] = utl


def is_school(polystats, kinds):
    """传入统计结果，再进行判断，polystats为json格式,kinds存储最终的类型判断结果"""
    #判断地块内是否包含学校
    for poi in polystats['result']:
        catalog = poi['catalog']
        if 'A3' in catalog:
            if catalog not in kinds:
                kinds.append(catalog)
    return kinds


def judge_kinds(polystats, kinds):
    """通用的判断类型的函数，提取出包含poi类别中排名前2位的，如果某一类别大于poi的一半，确定为唯一类型"""



if '__main__' == __name__:
    connection = myconn.Connection(host='localhost', database='postgis', user='postgres', password='ronald')
    poi_table = 'bdditu1'
    parcle_table = 'currentland'
    t_result = 'poi_in_parcle'
    buffer_radius = 5.0
    check_table(connection, t_result)
    build_pair(t_result, parcle_table, poi_table, buffer_radius)
