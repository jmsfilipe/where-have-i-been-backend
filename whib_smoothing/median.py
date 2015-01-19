__author__ = 'jmsfilipe'
import numpy
import gpxpy.geo as geo

def smooth(gpx, remove_extremes, how_much_to_smooth, min_sameness_distance, min_sameness_interval):
        #how_much_to_smooth = 1.75 min_sameness_distance = 1.5
        """ "Smooths" the elevation graph. Can be called multiple times. """
        for track in gpx.tracks:
            for track_segment in track.segments:

                if len(track_segment.points) <= 3:
                    return

                latitudes = []
                longitudes = []

                for point in track_segment.points:
                    latitudes.append(point.latitude)
                    longitudes.append(point.longitude)

                avg_distance = 0
                if remove_extremes:
                    # compute the average distance between two points:
                    distances = []
                    for i in range(len(track_segment.points))[1:]:
                        distances.append(track_segment.points[i].distance_2d(track_segment.points[i - 1]))
                    if distances:
                        avg_distance = 1.0 * sum(distances) / len(distances)

                # If The point moved more than this number * the average distance between two
                # points -- then is a candidate for deletion:
                remove_2d_extremes_threshold = how_much_to_smooth * avg_distance

                new_track_points = [track_segment.points[0]]

                for i in range(len(track_segment.points))[8:-8]:
                    new_point = None
                    point_removed = False

                    old_latitude = track_segment.points[i].latitude
                    new_latitude = numpy.median(latitudes[i - 8:i + 8])
                    old_longitude = track_segment.points[i].longitude
                    new_longitude = numpy.median(longitudes[i - 8:i + 8])

                    # TODO: This is not ideal.. Because if there are points A, B and C on the same
                    # line but B is very close to C... This would remove B (and possibly) A even though
                    # it is not an extreme. This is the reason for this algorithm:
                    d1 = geo.distance(latitudes[i - 1], longitudes[i - 1], None, latitudes[i], longitudes[i], None)
                    d2 = geo.distance(latitudes[i + 1], longitudes[i + 1], None, latitudes[i], longitudes[i], None)
                    d = geo.distance(latitudes[i - 1], longitudes[i - 1], None, latitudes[i + 1], longitudes[i + 1], None)

                    #print d1, d2, d, remove_extremes

                    if d1 + d2 > d * min_sameness_distance and remove_extremes:
                        d = geo.distance(old_latitude, old_longitude, None, new_latitude, new_longitude, None)
                        #print "d, threshold = ", d, remove_2d_extremes_threshold
                        if d < remove_2d_extremes_threshold:
                            new_point = track_segment.points[i]
                        else:
                            #print 'removed 2d'
                            point_removed = True
                    else:
                        new_point = track_segment.points[i]

                    if new_point and not point_removed:
                        new_track_points.append(new_point)

                    if remove_extremes:
                        track_segment.points[i].latitude = new_latitude
                        track_segment.points[i].longitude = new_longitude

                new_track_points.append(track_segment.points[- 1])

                #print 'len=', len(new_track_points)

                track_segment.points = new_track_points

        return gpx
