__author__ = 'jmsfilipe'

import gpxpy.name_locations as name_locations
import time
import gpxpy
import gpxpy.gpx
import glob
import os
import gpxpy.semantic_places as semantic_places
import datetime

if __name__ == '__main__':

    print "STARTING..."
    print datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    directory_name = 'tracks/'
    saving_name = 'save/'
    saving_directory = os.path.join(directory_name, saving_name)

    track_bits = []

    files =[]
    for f in os.listdir(saving_directory):
        files.append(f)
    files.sort()

    for f in files:
        file = open(os.path.join(saving_directory, f), 'rb')
        gpx_xml = file.read()
        file.close()

        gpx = gpxpy.parse(gpx_xml)

        for track in gpx.tracks:
            for segment in track.segments:
                    track_bits += [name_locations.find_track_bits(segment.points)]

    print "READY TO SUBMIT.", datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    name_locations.write_odds_ends(track_bits, True)
    print datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print "ALL DATA IS NOW STORED.\nYOU CAN CLOSE THIS."

