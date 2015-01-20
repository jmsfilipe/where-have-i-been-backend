__author__ = 'jmsfilipe'

if __name__ == '__main__':
    import gpxpy

    file_name = 'test_files/teste.gpx'
    file = open(file_name, 'r')
    gpx_xml = file.read()
    file.close()

    gpx = gpxpy.parse(gpx_xml)

    gpx.smooth(True, 1.5, 1.05, 0)

    gpx.simplify(10)

    fo = open("reduced_test2.gpx", "wb")
    fo.write(gpx.to_xml())

    # Close opend file
    fo.close()