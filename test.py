__author__ = 'jmsfilipe'

def getKey(customobj):
    return customobj.getKey()

if __name__ == '__main__':
    import gpxpy
    import gpxpy.gpx
    import glob
    import psycopg2
    import ppygis
    import os
    from time import mktime
    import StringIO
    import datetime
    import gpxpy
    import gpxpy.gpx
    import glob
    import os
    import time

    directory_name = 'HOJE/'
    saving_name = 'save/'
    saving_directory = os.path.join(directory_name, saving_name)
    DATE_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    if not os.path.exists(saving_directory):
        os.makedirs(saving_directory)
    current_day = ""
    for entry in sorted(glob.glob(directory_name + "*.gpx")):
        name = 0
        file = open(entry, 'rb')
        gpx_xml = file.read()
        file.close()

        gpx = gpxpy.parse(gpx_xml)
        gpx_list = gpx.track2trip(None, 120, 200, None)

        for segment in sorted(gpx_list):
            if segment.points[0].time.strftime("%Y-%m-%d") != current_day:
                name = 0
            else:
                name += 1
            segment.smooth(True, 1.5, 1.05, 0)
            segment.simplify(0.01,5) #RDP
            segment.reduce_points(10, 5)

            gpx_write = gpxpy.gpx.GPX()

            # Create first track in our GPX:
            gpx_track = gpxpy.gpx.GPXTrack()

            # Create first segment in our GPX track:
            gpx_segment = gpxpy.gpx.GPXTrackSegment()
            gpx_track.segments.append(segment)
            gpx_write.tracks.append(gpx_track)

            current_day =  segment.points[0].time.strftime("%Y-%m-%d")
            fo = open("{}-part{}.gpx".format(os.path.join(saving_directory,current_day),name), "wb")
            fo.write(gpx_write.to_xml())
            fo.close()
