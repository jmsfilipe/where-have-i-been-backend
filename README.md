# where-have-i-been-backend
Where Have I Been - Visualizing Personal Geolocation Data - Server Side

Auxiliary library to process GPX tracks

Some important methods:

gpx.track2trip(split_on_new_track, split_on_new_track_interval, min_sameness_distance, min_sameness_interval)
Splits a track into smaller ones, according to how long has the user been standing without moving.

segment.smooth(remove_extremes, how_much_to_smooth, min_sameness_distance, min_sameness_interval)
Smooths gpx track

segment.simplify(max_distance, max_time)
Simplify using the Ramer-Douglas-Peucker algorithm

segment.reduce_points(min_distance, min_time)
Reduce overall points of a track
