# -*- coding:utf-8 -*-
import python_postgresql as myconn


connection = myconn.Connection(host='localhost', database='postgis', user='postgres', password='ronald')
tablename = 'bdditu1'
sql = 'select * from %s limit 10 offset 0' % tablename
rows = connection.query(sql)
for row in rows:
    id = row['id']
    name = row['name']
    catalog = row['catalog']
    subcatalog = row['subcatalog']
    uid = row['uid']
    print 'id:%s, name:%s, catalog:%s, subcatalog:%s, uid:%s' % (id, name, catalog, subcatalog, uid)
