__author__ = 'jmsfilipe'

if __name__ == '__main__':
    import gpxpy
    import gpxpy.gpx
    import glob
    import psycopg2
    import ppygis
    import os
    from time import mktime
    import StringIO

    directory_name = 'HOJE/'
    saving_name = 'save/'
    saving_directory = os.path.join(directory_name, saving_name)
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    if not os.path.exists(saving_directory):
        os.makedirs(saving_directory)

    for entry in glob.glob(directory_name + "*.gpx"):
        file = open(entry, 'rb')
        gpx_xml = file.read()
        file.close()

        gpx = gpxpy.parse(gpx_xml)

        gpx_list = gpx.track2trip(None, 120, 200, None)

        name = 0



        for segment in gpx_list:

            segment.smooth(True, 1.5, 1.05, 0)
            segment.simplify(0.01,5) #RDP
            segment.reduce_points(10, 5)

            gpx = gpxpy.gpx.GPX()

            # Create first track in our GPX:
            gpx_track = gpxpy.gpx.GPXTrack()
            gpx.tracks.append(gpx_track)

            # Create first segment in our GPX track:
            gpx_segment = gpxpy.gpx.GPXTrackSegment()
            gpx_track.segments.append(segment)

            filename= os.path.basename(entry)

            fo = open("{}-part{}.gpx".format(os.path.join(saving_directory, filename.replace(".gpx","")),name), "wb")
            fo.write(gpx.to_xml())
            fo.close()
            name += 1

    #DATABASE
    try:
        conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."

    cur = conn.cursor()
    cur.execute("CREATE TABLE IF NOT EXISTS gtest (geom GEOMETRY(LineStringZM, 4326) NOT NULL)")
    conn.commit()

    for entry in glob.glob(saving_directory + "*.gpx"):
        file = open(entry, 'rb')
        gpx_xml = file.read()
        file.close()

        gpx = gpxpy.parse(gpx_xml)

        # for track in gpx.tracks:
        #     for segment in track.segments:
        #         query = "ST_GeomFromText('LINESTRINGZM("
        #         for point in segment.points:
        #             insert_time = mktime(point.time.timetuple())
        #             query += str(point.latitude) + " " + str(point.longitude) + " " + str(point.elevation) + " " + str(mktime(point.time.timetuple())) + ", "
        #         query = query[:-2]
        #         query += ")', 4326)"
        #         query = "INSERT INTO gtest(id, name, geom) VALUES(2, 'nome'," + query + ")"
        #         print query
        #         cur.execute(query)

        for track in gpx.tracks:
            buffer = StringIO.StringIO()
            for segment in track.segments:
                points=[]
                for point in segment.points:
                    points.append(ppygis.Point(point.latitude, point.longitude, point.elevation, mktime(point.time.timetuple()), srid=4326))

                buffer.write(ppygis.LineString((points), 4326).write_ewkb() + '\n')
                buffer.seek(0)

        cur.copy_from(buffer, 'gtest')

        conn.commit()


    cur.close()
    conn.close()