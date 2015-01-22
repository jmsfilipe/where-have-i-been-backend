__author__ = 'jmsfilipe'

if __name__ == '__main__':
    import gpxpy
    import gpxpy.gpx
    file_name = 'test_files/teste5.gpx'
    file = open(file_name, 'r')
    gpx_xml = file.read()
    file.close()

    gpx = gpxpy.parse(gpx_xml)

    gpx_list = gpx.track2trip(None, 2, 50, None)

    name = 0

    for segment in gpx_list:

        segment.smooth(True, 1.5, 1.05, 0)
        segment.simplify(10)

        gpx = gpxpy.gpx.GPX()

        # Create first track in our GPX:
        gpx_track = gpxpy.gpx.GPXTrack()
        gpx.tracks.append(gpx_track)

        # Create first segment in our GPX track:
        gpx_segment = gpxpy.gpx.GPXTrackSegment()
        gpx_track.segments.append(segment)

        fo = open("{}{}.gpx".format(name,name), "wb")
        fo.write(gpx.to_xml())
        fo.close()
        name += 1