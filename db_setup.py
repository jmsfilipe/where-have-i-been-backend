from gpxpy.name_locations import neighborhood

__author__ = 'jmsfilipe'
import psycopg2
import ppygis
import datetime
import glob
import gpxpy
import os

directory_name = 'hoje/'
saving_name = 'save/'
saving_directory = os.path.join(directory_name, saving_name)

def init_database():
      #DATABASE
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."

    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS trips (trip_id SERIAL PRIMARY KEY, start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS linestrings (trip_id SERIAL REFERENCES trips(trip_id), geom GEOGRAPHY(LineStringZ, 4326) NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS trip_points (point GEOGRAPHY(PointZ, 4326) NOT NULL, timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS places (description TEXT PRIMARY KEY, point GEOGRAPHY(POINTZ, 4326) NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS stays (stay_id TEXT, start_date TIMESTAMP WITHOUT TIME ZONE NOT NULL, end_date TIMESTAMP WITHOUT TIME ZONE NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS colors (category TEXT, color TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS categories (category TEXT, location TEXT)")
    conn.commit()

def insert_empty_trip_database(date, start, end):
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."

    start_date = date + " " + start
    end_date = date + " " + end

    cur = conn.cursor()
    cur.execute("INSERT INTO trips(start_date, end_date) VALUES('" + str(start_date) + "', '" + str(end_date) + "')")
    conn.commit()

def insert_trips_database():
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."

    cur = conn.cursor()
    print "start inserting", datetime.datetime.now().time()
    try:
        for entry in glob.glob(saving_directory + "*.gpx"):
            file = open(entry, 'rb')
            gpx_xml = file.read()
            file.close()

            gpx = gpxpy.parse(gpx_xml)
            for track in gpx.tracks:
                for segment in track.segments:
                    cur.execute("INSERT INTO trips(start_date, end_date) VALUES('" + str(segment.points[0].time.replace(second=0)) + "', '" + str(segment.points[-1].time.replace(second=0)) + "')")

            conn.commit()

            for track in gpx.tracks:
                for segment in track.segments:
                    for point in segment.points:
                        cur.execute("INSERT INTO trip_points(point, timestamp) VALUES('" + ppygis.Point(point.latitude, point.longitude, point.elevation, srid=4326).write_ewkb() + "', '" + str(point.time) + "')")

            conn.commit()

            for track in gpx.tracks:
                for segment in track.segments:
                    points=[]
                    for point in segment.points:
                        points.append(ppygis.Point(point.longitude, point.latitude, point.elevation, srid=4326))

                    cur.execute("INSERT INTO linestrings(geom) VALUES('" + ppygis.LineString((points), 4326).write_ewkb() +"')")
            conn.commit()
    except Exception as e:
        print e

    # for track in gpx.tracks:
    #     buffer = StringIO.StringIO()
    #     for segment in track.segments:
    #         points=[]
    #         for point in segment.points:
    #             points.append(ppygis.Point(point.latitude, point.longitude, point.elevation, mktime(point.time.timetuple()), srid=4326))
    #
    #         buffer.write(ppygis.LineString((points), 4326).write_ewkb() + '\n')
    #         buffer.seek(0)
    #
    # cur.copy_from(buffer, 'linestrings')
    #
    # conn.commit()

    print "end inserting", datetime.datetime.now().time()
    cur.close()
    conn.close()

def insert_spans_database():
    from gpxpy.semantic_places import read_days, minutes_to_military
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."
    from dateutil.parser import parse
    cur = conn.cursor()
    cur.execute("SET TIME ZONE '0'")
    conn.commit()

    semantic_file = read_days("location_semantics.txt")
    for day in semantic_file.days:
        for entry in day.entries:
            #start_date = parse((day.date + " " + minutes_to_military2(entry.start_date) + ' UTC%+d' %  entry.timezone).replace("_","/"))
            #end_date = parse((day.date + " " + minutes_to_military2(entry.end_date) + ' UTC%+d' % entry.timezone).replace("_","/"))


            start_date = datetime.datetime.strptime(day.date + minutes_to_military(entry.start_date), "%Y_%m_%d%H%M")
            end_date = datetime.datetime.strptime(day.date + minutes_to_military(entry.end_date), "%Y_%m_%d%H%M")
           # if entry.timezone != 0:
            #    start_date.replace(tzinfo=pytz.timezone('UTC'+str(entry.timezone)))
             #   end_date.replace(tzinfo=pytz.timezone('UTC'+str(entry.timezone)))
            print start_date, end_date
            cur.execute("INSERT INTO stays(stay_id, start_date, end_date) VALUES(%s, %s, %s)",(entry.location, start_date, end_date));
    cur.close()
    conn.commit()


def insert_trips_temp_database():
    from gpxpy.semantic_places import read_days, minutes_to_military
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."
    from dateutil.parser import parse
    cur = conn.cursor()
    cur.execute("SET TIME ZONE '0'")
    conn.commit()

    semantic_file = read_days("location_semantics.txt")
    for day in semantic_file.days:
        for prev, entry, next in neighborhood(day.entries):
            #start_date = parse((day.date + " " + minutes_to_military2(entry.start_date) + ' UTC%+d' %  entry.timezone).replace("_","/"))
            #end_date = parse((day.date + " " + minutes_to_military2(entry.end_date) + ' UTC%+d' % entry.timezone).replace("_","/"))

            if next:
                start_date = datetime.datetime.strptime(day.date + minutes_to_military(entry.end_date), "%Y_%m_%d%H%M")
                end_date = datetime.datetime.strptime(day.date + minutes_to_military(next.start_date), "%Y_%m_%d%H%M")
           # if entry.timezone != 0:
            #    start_date.replace(tzinfo=pytz.timezone('UTC'+str(entry.timezone)))
             #   end_date.replace(tzinfo=pytz.timezone('UTC'+str(entry.timezone)))
                print start_date, end_date
                cur.execute("INSERT INTO trips(start_date, end_date) SELECT %s, %s WHERE NOT EXISTS ( SELECT start_date, end_date FROM trips WHERE start_date = %s AND end_date = %s)",(start_date, end_date, start_date, end_date));
    cur.close()
    conn.commit()

def insert_place_database(location, point):
    if point is None:
        point = [0,0,0]
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."

    cur = conn.cursor()
    point = ppygis.Point(point[1], point[0], point[2], srid=4326).write_ewkb()
    query = "UPDATE places SET description=%s, point=%s WHERE description=%s;\
        INSERT INTO places(description, point)\
        SELECT %s, %s\
        WHERE NOT EXISTS (SELECT 1 FROM places WHERE description=%s);"

    cur.execute(query, (location, point, location, location, point, location))
    cur.close()
    conn.commit()
