__author__ = 'jmsfilipe'
import psycopg2
import ppygis
import datetime
from datetime import timedelta
import time

#DATABASE
try:
    conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
except:
    print "I am unable to connect to the database."

cur = conn.cursor()

# print "criar index start", datetime.datetime.now().time()
# cur.execute("CREATE INDEX main_index ON trip_points (point, timestamp)")
# conn.commit()
# print "criar index end", datetime.datetime.now().time()
#
# print "criar query start", datetime.datetime.now().time()
# cur.execute("SELECT point, timestamp FROM trip_points WHERE timestamp BETWEEN '2015-01-20 08:30:25' AND '2015-01-20 09:30:25'")
# print cur.rowcount
# conn.commit()
# print "criar query end", datetime.datetime.now().time()

def query_time_interval_return_point(base_time, interval):
    cur.execute("SELECT point, timestamp FROM trip_points WHERE timestamp BETWEEN '" + str(base_time) + "' AND '" + str(datetime.datetime.strptime(base_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(seconds = int(interval))) + "'")

def query_time_return_point(base_time):
    cur.execute("SELECT point, timestamp FROM trip_points WHERE timestamp = '" + str(base_time) + "'")

def query_space_range_return_point(lat, lon, range):
    cur.execute("SELECT point, timestamp FROM trip_points WHERE ST_DISTANCE(ST_GeomFromText('POINT(" + lon + " " + lon + ")',4326), point) > " + str(range))

def query_space_specific_return_point(lat, lon):
    cur.execute("SELECT point, timestamp FROM trip_points WHERE ST_X(point) = " + lat + " AND ST_Y(point) = " + lon )


#query_time_interval_return_point("2015-01-20 08:30:25", "360")
#query_time_return_point("2015-01-20 08:30:25")
print datetime.datetime.now().time()
#query_space_range_return_point("38.737824", "-9.142589", 10)
query_space_specific_return_point("38.737824", "-9.142589")
print cur.rowcount
print datetime.datetime.now().time()

cur.close()
conn.close()