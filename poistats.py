# -*- coding:utf-8 -*-
from __future__ import division
import python_postgresql as myconn
import time


def check_table(conn, tablename):
    #判断表是否存在
    sql = "select count(*) as count from pg_class where relname = '%s'" % tablename
    rexist = conn.query(sql)
    if int(rexist[0]['count']) != 0:
        sql = "drop table %s" % tablename
        conn.execute(sql)
        conn.commit()


def check_column(conn, tablename, columnname):
    #判断列是否存在于表中，如果存在，先清空，否则，创建列
    sql = "SELECT a.attname,pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type " \
          "FROM pg_catalog.pg_attribute a," \
          "(SELECT c.oid FROM pg_catalog.pg_class c " \
          "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace " \
          "WHERE (c.relname) =lower('%s') " \
          "AND (n.nspname) = lower('public')) b " \
          "WHERE a.attrelid = b.oid " \
          "AND a.attnum > 0 AND NOT a.attisdropped ORDER BY a.attnum" % tablename
    cols = conn.query(sql)
    col_names = []
    for col in cols:
        col_names.append(col['attname'])
    if columnname in col_names:
        sql = 'update %s set %s=null' % (tablename, columnname)
        conn.execute(sql)
        conn.commit()
    else:
        sql = 'alter table %s add column %s character varying(20)' % (tablename, columnname)
        conn.execute(sql)
        conn.commit()


def build_pair(conn, result_table, polygon_table, point_table, radius=5.0):
    sql = 'create table %s as select a.gid as poly_gid,a.bz_new,a.utl,a.geom,' \
          'b.gid as point_gid,b.name,b.catalog,b.subcatalog from %s a,%s b ' \
          'where a.geom && b.geom ' \
          'and st_within(b.geom, st_buffer(a.geom,%f))' % (result_table, polygon_table, point_table, radius)
    curtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'build polygon-point pair start computing:%s' % curtime
    conn.execute(sql)
    conn.commit()
    curtime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
    print 'build polygon-point pair end computing:%s' % curtime


def parcle_change(conn, parcle_landuse, poi_in_parcle):
    check_table(conn, parcle_landuse)
    sql = 'create table %s as select distinct poly_gid, bz_new,utl,geom ' \
          'from %s order by poly_gid asc' % (parcle_landuse, poi_in_parcle)
    conn.execute(sql)
    conn.commit()
    col_name = 'poi_landuse'
    check_column(conn, parcle_landuse, col_name)
    sql = 'select * from %s' % parcle_landuse
    pip = conn.query(sql)
    for row in pip:
        gid = row['poly_gid']
        # bz_new = row['bz_new']
        # utl = row['utl']
        sql = 'select catalog, count(*) as count from %s where poly_gid=%s ' \
              'group by catalog order by count desc' % (poi_in_parcle, gid)
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
        # pair['bz_new'] = bz_new
        # pair['utl'] = utl
        kinds = judge_kinds(pair, kinds)
        kinds = is_school(pair, kinds)
        # pair['result'] = kinds
        strkinds = ','.join(kinds)
        sql = "update %s set %s='%s' where poly_gid=%s" % (parcle_landuse, col_name, strkinds, gid)
        conn.execute(sql)
        conn.commit()


def is_school(polystats, kinds):
    """传入统计结果，再进行判断，polystats为json格式,kinds存储最终的类型判断结果"""
    #判断地块内是否包含学校,如果存在则为唯一类型
    for poi in polystats['result']:
        catalog = poi['catalog']
        if 'A3' in catalog:
            if catalog not in kinds:
                kinds = []
                kinds.append(catalog)
                break
    return kinds


def judge_kinds(polystats, kinds):
    """通用的判断类型的函数，提取出包含poi类别中排名前2位的，如果某一类别大于poi的一半，确定为唯一类型"""
    result = polystats['result']
    length = len(result)
    if length == 1:
        kinds.append(result[0]['catalog'])
    else:
        count = result[0]['count']
        if count >= length/2:
            kinds = [result[0]['catalog']]
        else:
            kinds.append(result[0]['catalog'])
            if result[1]['catalog'] not in kinds:
                kinds.append(result[1]['catalog'])
    return kinds


if '__main__' == __name__:
    connection = myconn.Connection(host='localhost', database='postgis', user='postgres', password='ronald')
    poi_table = 'bdditu1'
    parcle_table = 'currentland'
    t_result = 'poi_in_parcle'
    t_landuse = 'parcle_landuse'
    buffer_radius = 5.0
    # check_table(connection, t_result)
    # build_pair(connection, t_result, parcle_table, poi_table, buffer_radius)
    parcle_change(connection, t_landuse, t_result)
    print 'compute finished'