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
    cur.execute("CREATE TABLE IF NOT EXISTS trips (trip_id SERIAL PRIMARY KEY, start_date TIMESTAMP NOT NULL, end_date TIMESTAMP NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS linestrings (trip_id SERIAL REFERENCES trips(trip_id), geom GEOGRAPHY(LineStringZ, 4326) NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS trip_points (point GEOGRAPHY(PointZ, 4326) NOT NULL, timestamp TIMESTAMP NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS places (description TEXT PRIMARY KEY, point GEOGRAPHY(POINTZ, 4326) NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS stays (stay_id TEXT, start_date TIMESTAMP NOT NULL, end_date TIMESTAMP NOT NULL)")
    cur.execute("CREATE TABLE IF NOT EXISTS colors (category TEXT, color TEXT)")
    cur.execute("CREATE TABLE IF NOT EXISTS categories (category TEXT, location TEXT[])")
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
                    cur.execute("INSERT INTO trips(start_date, end_date) VALUES('" + str(segment.points[0].time) + "', '" + str(segment.points[-1].time) + "')")

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
    except:
        print file

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

    cur = conn.cursor()
    semantic_file = read_days("location_semantics.txt")
    for day in semantic_file.days:
        for entry in day.entries:
            start_date = datetime.datetime.strptime(day.date + minutes_to_military(entry.start_date), "%Y_%m_%d%H%M")
            end_date = datetime.datetime.strptime(day.date + minutes_to_military(entry.end_date), "%Y_%m_%d%H%M")
            cur.execute("INSERT INTO stays(stay_id, start_date, end_date) VALUES('" + entry.location + "', '" + str(start_date) + "', '" + str(end_date) + "')")
    cur.close()
    conn.commit()

def insert_place_database(location, point):
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."

    cur = conn.cursor()
    point = ppygis.Point(point[1], point[0], point[2], srid=4326).write_ewkb()
    query = "UPDATE places SET description='{location}', point='{point}' WHERE description='{location}';\
        INSERT INTO places(description, point)\
        SELECT '{location}', '{point}'\
        WHERE NOT EXISTS (SELECT 1 FROM places WHERE description='{location}');"

    cur.execute(query.format(location=location, point=point))
    cur.close()
    conn.commit()
