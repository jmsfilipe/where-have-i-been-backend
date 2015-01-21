__author__ = 'jmsfilipe'

if __name__ == '__main__':
    import gpxpy

    file_name = 'test_files/teste4.gpx'
    file = open(file_name, 'r')
    gpx_xml = file.read()
    file.close()

    gpx = gpxpy.parse(gpx_xml)

    gpx_list = gpx.track2trip(None, 5, 10, None)

    name = 0
    for segment in gpx_list:

        segment.smooth(True, 1.5, 1.05, 0)
        segment.simplify(10)

        gpx = gpxpy.gpx.GPX()

        # Create first track in our GPX:
        gpx_track = gpxpy.gpx.GPXTrack()
        gpx.tracks.append(gpx_track)

        # Create first segment in our GPX track:
        gpx_track.segments.append(segment)

        fo = open("{}{}.gpx".format(segment.points[-1].time,name), "wb")
        fo.write(gpx.to_xml())
        fo.close()
        name += 1
