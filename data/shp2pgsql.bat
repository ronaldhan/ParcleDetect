shp2pgsql -W GBK D:/ft/currentland.shp currentland > D:/ft/currentland.sql
psql -h localhost -U postgres -d postgis -f D:/ft/currentland.sql