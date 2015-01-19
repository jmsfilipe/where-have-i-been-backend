__author__ = 'jmsfilipe'

if __name__ == '__main__':
    import gpxpy
    import whib_smoothing as mod_simplification

    file_name = 'test_files/2760.gpx'
    file = open(file_name, 'r')
    gpx_xml = file.read()
    file.close()

    gpx = gpxpy.parse(gpx_xml)

    new_gpx = mod_simplification.smooth(gpx, True, 1.5, 1.05, 0)

    fo = open("reduced_test.gpx", "wb")
    fo.write(new_gpx.to_xml())

    # Close opend file
    fo.close()