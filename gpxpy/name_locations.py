__author__ = 'jmsfilipe'

import sys

reload(sys)
sys.setdefaultencoding('utf8')

import semantic_places
import gpxpy
import gpxpy.gpx
import glob
import os
import time
from datetime import datetime, timedelta
import cPickle
from math import sqrt
import psycopg2
import ppygis
import gpxpy.geo as geo
directory_name = 'tracks/'
saving_name = 'save/'
saving_directory = os.path.join(directory_name, saving_name)

try:
    conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
except:
    print "I am unable to connect to the database."

cur = conn.cursor()

def military_to_minutes(ts):
    h = int(ts[:2])
    m = int(ts[-2:])
    return h*60+m



def minutes_to_military(minutes):
    minutes = int(minutes)
    h = minutes / 60
    m = minutes % 60
    tmp = "%2d%2d" % (h, m)
    tmp = tmp.replace(" ","0")
    return tmp


curtimezone = 0
class SemanticFile:
    def __init__(self):
        self.days = []
        global curtimezone
        curtimezone = 0

    def __repr__(self):
        global curtimezone
        curtimezone = 0
        result = ""
        for day in self.days:
            result += str(day) + "\n"


        return result

    def add_day(self, day):
        self.days.append(day)

    def to_file(self, filename):
        global curtimezone
        curtimezone = 0
        open(filename, 'w').close()
        f = open(filename, 'w')
        f.write(str(self))
        f.close()


class Day:
    def __init__(self, date,):
        self.entries = []
        self.date = date

    def __repr__(self):
        result = str(self.date) + "\n"
        for entry in self.entries:
            result += str(entry) + "\n"

        return result

    def add_entry(self, entry):
        self.entries.append(entry)

class Entry:
    def __init__(self, start_date, end_date, location, timezone = 0):
        self.start_date = military_to_minutes(start_date)
        self.end_date = military_to_minutes(end_date)

        self.location = location
        if location is None:
            self.location = "*"

        self.timezone = timezone
        if timezone == 0 or timezone == "GMT":
            global curtimezone
            curtimezone = 0
            self.timezone = 0
        else:
            global curtimezone
            self.timezone = int(timezone[-2:])
            curtimezone = int(timezone[-2:])

    def __repr__(self):
        global curtimezone
        #print "KEL CHECK", self.timezone, curtimezone
        if self.timezone != curtimezone:
            tz = "GMT"+str("%+d" % (self.timezone))
            curtimezone = int(self.timezone)
            return tz + "\n" + str(minutes_to_military(self.start_date)) + "-" + str(minutes_to_military(self.end_date)) + ":" + self.location
        else:
            return str(minutes_to_military(self.start_date)) + "-" + str(minutes_to_military(self.end_date)) + ":" + self.location

    def start_gmt(self):
        x = self.start_date-self.timezone*60
        if x<0:
            return x, 1
        elif x>(60*24):
            return x, 2
        else:
            return x,0

    def end_gmt(self):
        x = self.end_date-self.timezone*60
        if x<0:
            return x, 1
        elif x>(60*24):
            return x, 2
        else:
            return x,0


class Trips:
    def __init__(self, start_date, end_date, start_coords, end_coords):
        self.start_date = start_date
        self.end_date = end_date
        self.start_coords = start_coords
        self.end_coords = end_coords

def find_track_bits(points):

    return Trips(points[0].time,
                 points[-1].time,
                 geo.Location(points[0].latitude, points[0].longitude, points[0].elevation),
                 geo.Location(points[-1].latitude, points[-1].longitude, points[-1].elevation))

def get_semantic_file_with_learning(track_bits, locations):
    last_analyzed_day = ""
    file = SemanticFile()
    day = None
    for b in range(len(track_bits)):
        if last_analyzed_day == track_bits[b].start_date.strftime("%Y_%m_%d"):
            sameday = True
        else:
            sameday = False

        if sameday:
            start = track_bits[b-1].end_date.strftime("%H%M")
            end = track_bits[b].start_date.strftime("%H%M")
            location = best_nearest(locations, track_bits[b].start_coords.latitude, track_bits[b].start_coords.longitude)
            if location:
                location = location[0]
            else:
                location = ""
            entry = Entry(start,end,location)
            day.add_entry(entry)
            if b > len(track_bits)-2 or track_bits[b].start_date.strftime("%Y_%m_%d") != track_bits[b+1].start_date.strftime("%Y_%m_%d") :
                start = track_bits[b].end_date.strftime("%H%M")
                end = "2359"
                location = best_nearest(locations, track_bits[b].start_coords.latitude, track_bits[b].start_coords.longitude)
                if location:
                    location = location[0]
                else:
                    location = ""
                entry = Entry(start,end,location)
                day.add_entry(entry)
        else:
            date=track_bits[b].start_date.strftime("%Y_%m_%d")
            start = "0000"
            end = track_bits[b].start_date.strftime("%H%M")
            location = best_nearest(locations, track_bits[b].start_coords.latitude, track_bits[b].start_coords.longitude)
            if location:
                location = location[0]
            else:
                location = ""
            day = Day(date)
            file.add_day(day)
            entry = Entry(start,end,location)
            day.add_entry(entry)
            if b > len(track_bits)-2 or  track_bits[b].start_date.strftime("%Y_%m_%d") != track_bits[b+1].start_date.strftime("%Y_%m_%d"): #if next day is another, then we need to finish this one first
                end = "2359"
                start = track_bits[b].end_date.strftime("%H%M")
                location = best_nearest(locations, track_bits[b].start_coords.latitude, track_bits[b].start_coords.longitude)
                if location:
                    location = location[0]
                else:
                    location = ""
                entry = Entry(start,end,location)
                day.add_entry(entry)

        last_analyzed_day = track_bits[b].start_date.strftime("%Y_%m_%d")

    return file

def write_odds_ends(track_bits, batch):
    from db_setup import insert_trips_database, insert_spans_database, insert_trips_temp_database
    locs, state, old_days, new_days = read_all_data(track_bits, batch)
    # if state == "not_first_time":
    #     locations = compute_locations(locs)
    #     file = get_semantic_file_with_learning(track_bits, locations)
    #     file.to_file("./location_semantics.txt")

    if not batch:
        check_gpx_changes(old_days, new_days)

    print "START" ,datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    insert_trips_database()
    insert_trips_temp_database()
    insert_spans_database()
    print "end ", datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]

def write_odds_ends_batch(track_bits):
    from db_setup import insert_trips_database, insert_spans_database, insert_trips_temp_database
    locs, state, new_days = read_all_data_batch(track_bits)
    # if state == "not_first_time":
    #     locations = compute_locations(locs)
    #     file = get_semantic_file_with_learning(track_bits, locations)
    #     file.to_file("./location_semantics.txt")

    #check_gpx_changes(old_days, new_days)
    insert_trips_temp_database()
    insert_trips_database()
    insert_spans_database()


def distance(x1, y1, x2, y2):
    return sqrt((x1-x2)**2 + (y1-y2)**2)

def nearest(locations,lat, lon, sample_size = 3):
    near = []
    k = locations.keys()
    k.sort(lambda x,y: cmp(distance(locations[x][0][0],locations[x][0][1], lat, lon), distance(locations[y][0][0],locations[y][0][1], lat, lon)))
    for kk in k[:sample_size]:
        d = distance(locations[kk][0][0],locations[kk][0][1], lat, lon)
        if d < 0.01:
            near.append((kk, d, locations[kk][0]))
    return near

    # below is deprecateddistance(locations[x][0][0],locations[x][1], lat, lon)
    for k in locations:
        d = distance(locations[k][0],locations[k][1], lat, lon)
        if d < dist:
            dist = d
            near = k
    return near, dist

def best_nearest(locations,lat, lon, sample_size = 3):
    near = nearest(locations,lat, lon, sample_size = sample_size)
    more_times = 0
    best = None
    for n in near:
        if n[2]>more_times:
            best = n
            more_times = n[2]
    return best


def compute_locations(locs):
    res = {}
    for key in locs.keys():
        average, near, far = filter_locs(locs[key])
        res[key] = (average, len(near)+len(far))
    return res

def filter_locs(locs,step=0):
    average = average_coords(locs)
    stdev = stdev_coords(locs, average)
    stdev = stdev[0]*(2-step), stdev[1]*(2-step)
    near = []
    far = []
    for l in locs:
        if (abs(l.latitude-average[0])>=stdev[0] and stdev[0]) or (abs(l.longitude-average[1])>=stdev[1] and stdev[1]):
            far.append(l)
        else:
            near.append(l)
    if step==0:
        a,n,f = filter_locs(near,1)
        near = n
        far = f+far
        average = a
    try:
        return average_coords(near), near, far
    except:
        return [0,0], near, far

def average_coords(coords):
    #print "COORDS", coords
    if coords is None:
        return None
    lat = reduce((lambda x,y:x+y),[x.latitude for x in coords])/len(coords)
    lon = reduce((lambda x,y:x+y),[x.longitude for x in coords])/len(coords)

    ele = reduce((lambda x,y:x+y),[x.elevation if x.elevation else 0 for x in coords])/len(coords)
    return [lat,lon,ele]



def stdev_coords(coords,avg):
    lat = reduce((lambda x,y:x+y),[(x.latitude-avg[0])**2 for x in coords])/len(coords)
    lon = reduce((lambda x,y:x+y),[(x.longitude-avg[1])**2 for x in coords])/len(coords)
    lat = sqrt(lat)
    lon = sqrt(lon)
    return [lat,lon]

def get_semantic_file(track_bits):
    last_analyzed_day = ""

    file = SemanticFile()
    for b in range(len(track_bits)):
        if last_analyzed_day == track_bits[b].start_date.strftime("%Y_%m_%d"):
            sameday = True
        else:
            sameday = False

        if sameday:
            start = track_bits[b-1].end_date.strftime("%H%M")
            end = track_bits[b].start_date.strftime("%H%M")
            location = "*"
            entry = Entry(start, end, location)
            day.add_entry(entry)
            if b == len(track_bits)-1 or track_bits[b].start_date.strftime("%Y_%m_%d") != track_bits[b+1].start_date.strftime("%Y_%m_%d") :
                start = track_bits[b].end_date.strftime("%H%M")
                end = "2359"
                location = "*"
                entry = Entry(start, end, location)
                day.add_entry(entry)
        else:
            date=track_bits[b].start_date.strftime("%Y_%m_%d")
            start = "0000"
            end = track_bits[b].start_date.strftime("%H%M")
            location = "*"
            day = Day(date)
            file.add_day(day)
            entry = Entry(start, end, location)
            day.add_entry(entry)
            if  b == len(track_bits)-1 or track_bits[b].start_date.strftime("%Y_%m_%d") != track_bits[b+1].start_date.strftime("%Y_%m_%d"): #if next day is another, then we need to finish this one first
                end = "2359"
                start = track_bits[b].end_date.strftime("%H%M")
                location = "*"
                entry = Entry(start, end, location)
                day.add_entry(entry)

        last_analyzed_day = track_bits[b].start_date.strftime("%Y_%m_%d")

    return file



from websockets.chatdemo import *

def start_server():
    tornado.options.parse_command_line()
    app = Application()
    app.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()

def first_time():
    import os
    return not os.path.isfile("points.dat")

def read_all_data(trackbits, batch):
    from db_setup import init_database
    import threading
    threading.Thread(target=start_server).start()
    state = ""
    if first_time():
        init_database()
        if not batch:
            old_days = get_semantic_file(trackbits)
            old_days.to_file("semantics/location_semantics.txt")
        else:
            old_days = None

        if wait_until_save(100000):
            days = semantic_places.read_days("semantics/location_semantics.txt")
            #days = semantic_places.read_days("partial.txt")
            locs, last_date = read_locations()
            #locs = []
            #if not locs:
            locs = gather_locations(days)
            state = "first_time"
            #print "Info for %s days, up to %s" % (len(locs), last_date)
            return locs, state, old_days, days
    else:
        if not batch:
            old_days = get_semantic_file(trackbits)
            old_days.to_file("semantics/location_semantics.txt")
        else:
            old_days = None
        locs, last_date = read_locations()
        locations = compute_locations(locs)
        file = get_semantic_file_with_learning(trackbits, locations)
        file.to_file("semantics/location_semantics.txt")
        if wait_until_save(100000):
            days = semantic_places.read_days("semantics/location_semantics.txt")
            #days = semantic_places.read_days("partial.txt")
            locs, last_date = read_locations()
            #locs = []
            #if not locs:
            locs = update_locations(days, locs, last_date)
            state = "not_first_time"
            #print "Info for %s days, up to %s" % (len(locs), last_date)

            return locs, state, old_days, days

def read_all_data_batch(trackbits):
    from db_setup import init_database
    state = ""
    if first_time():
        init_database()
        #old_days = get_semantic_file(trackbits)
    #old_days.to_file("location_semantics.txt")
    #if wait_until_save(100000):
    days = semantic_places.read_days("location_semantics.txt")
    #days = semantic_places.read_days("partial.txt")
    #locs, last_date = read_locations()
    #locs = []
    #if not locs:
    locs = gather_locations(days)
    state = "first_time"
    #print "Info for %s days, up to %s" % (len(locs), last_date)
    return locs, state, days
    # else:
    #     old_days = get_semantic_file(trackbits)
    #     old_days.to_file("location_semantics.txt")
    #     locs, last_date = read_locations()
    #     locations = compute_locations(locs)
    #     file = get_semantic_file_with_learning(trackbits, locations)
    #     file.to_file("location_semantics.txt")
    #     if wait_until_save(100000):
    #         days = semantic_places.read_days("location_semantics.txt")
    #         #days = semantic_places.read_days("partial.txt")
    #         locs, last_date = read_locations()
    #         #locs = []
    #         #if not locs:
    #         locs = update_locations(days, locs, last_date)
    #         state = "not_first_time"
    #         #print "Info for %s days, up to %s" % (len(locs), last_date)
    #
    #         return locs, state, old_days, days


def wait_until_save(timeout, period=0.25):
  import time
  global proceed
  mustend = time.time() + timeout
  while time.time() < mustend:
    if proceed:
        return True
    time.sleep(period)
  return False

proceed = False
def keep_processing():
    global proceed
    proceed = True


def check_gpx_changes(old_content_obj, new_content_obj):

    for old_day, new_day in zip(old_content_obj.days, new_content_obj.days):
        if len(old_day.entries) > len(new_day.entries): #delete gpx
            #print "FIRST PROB"
            for (old_entry_prev, old_entry, old_entry_next), (new_entry_prev, new_entry, new_entry_next) in zip(neighborhood(old_day.entries), neighborhood(new_day.entries)):
                if old_entry.start_date == new_entry.start_date and old_entry.end_date != new_entry.end_date:
                    join_gpx(old_entry.end_date, old_entry_next.end_date)
                elif old_entry.start_date == new_entry.start_date and old_entry.end_date == new_entry.end_date and old_entry_next.start_date != new_entry_next.start_date and old_entry_next.end_date != new_entry_next.end_date:
                    remove_gpx_file(old_day.date, old_entry.end_date)
        elif len(old_day.entries) < len(new_day.entries): #split gpx
            #print "SECOND PROB"
            for old_entry, new_entry in zip(old_day.entries, new_day.entries):
                if old_entry.end_date != new_entry.end_date and old_entry.start_date == new_entry.start_date:

                    end = ""
                    for prev,item,next in neighborhood(new_day.entries):
                        end = next.start_date
                        end_loc = next.location
                        break

                    from db_setup import insert_empty_trip_database
                    insert_empty_trip_database(new_day.date, str(minutes_to_military(new_entry.end_date)) , str(minutes_to_military(end)) )

                    print "You've added more entries. The trip you made on " + new_day.date + " starting at " + str(minutes_to_military(new_entry.end_date)) + ", " + new_entry.location + " and ending at " + str(minutes_to_military(end))+ ", "+ end_loc + " will not have any route associated."

def neighborhood(iterable):
    iterator = iter(iterable)
    prev = None
    item = iterator.next()  # throws StopIteration if empty.
    for next in iterator:
        yield (prev,item,next)
        prev = item
        item = next
    yield (prev,item,None)

def join_gpx(first_track_start, second_track_start):
    first_track_start = minutes_to_military(first_track_start)
    second_track_start = minutes_to_military(second_track_start)
    directory_name = 'tracks/'
    saving_name = 'save/'
    saving_directory = os.path.join(directory_name, saving_name)
   #print "####", first_track_start, second_track_start


    files =[]
    for f in os.listdir(saving_directory):
        files.append(f)
    files.sort()

    segments_to_delete = []
    for f_prev, f, f_next in neighborhood(files):
        filename = os.path.join(saving_directory, f)
        file = open(filename, 'rb')
        gpx_xml = file.read()
        file.close()
        gpx = gpxpy.parse(gpx_xml)

        if f_next is not None:
            filename_next = os.path.join(saving_directory, f_next)
            file_next = open(filename_next, 'rb')
            gpx_xml_next = file_next.read()
            file_next.close()
            gpx_next = gpxpy.parse(gpx_xml_next)

        for track, track_next  in zip(gpx.tracks, gpx_next.tracks):
            for segment, segment_next in zip(track.segments, track_next.segments):

                if segment.points[0].time.strftime("%H%M") == first_track_start and \
                    segment_next.points[0].time.strftime("%H%M") == second_track_start:
                    segment.join(segment_next)
                    #segments_to_delete.append(filename)
                    segments_to_delete.append(filename_next)

                    gpx = gpxpy.gpx.GPX()
                    # Create first track in our GPX:
                    gpx_track = gpxpy.gpx.GPXTrack()
                    gpx.tracks.append(gpx_track)
                    # Create first segment in our GPX track:
                    gpx_track.segments.append(segment)

                    open(filename, 'w').close()
                    fo = open(filename, "wb")
                    fo.write(gpx.to_xml())
                    fo.close()


    for f in segments_to_delete:
        os.remove(f)

def remove_gpx_file(date, start_time):
    start_time = minutes_to_military(start_time)
    directory_name = 'tracks/'
    saving_name = 'save/'
    saving_directory = os.path.join(directory_name, saving_name)


    files =[]
    for f in os.listdir(saving_directory):
        files.append(f)
    files.sort()

    for f in files:
        filename = os.path.join(saving_directory, f)
        file = open(filename, 'rb')
        gpx_xml = file.read()
        file.close()

        gpx = gpxpy.parse(gpx_xml)

        for track in gpx.tracks:
            for segment in track.segments:
                if segment.points[0].time.strftime("%H%M") == start_time and \
                    segment.points[0].time.strftime("%Y_%m_%d") == date:
                    os.remove(filename)

def read_locations():
    if not os.path.exists("points.dat"):
        return None, None
    else:
        f = open("points.dat")
        last_date = cPickle.load(f)
        points = cPickle.load(f)
        f.close()
        return points, last_date


def read_day_tracks(day):
    day = day.replace("_","-")
    files = []
    points = []

    directory_name = 'tracks/'
    saving_name = 'save/'
    saving_directory = os.path.join(directory_name, saving_name)

    for f in os.listdir(saving_directory):
        if f[:10]==day:
            files.append(f)
    files.sort()

    for f in files:
        file = open(os.path.join(saving_directory, f), 'rb')
        gpx_xml = file.read()
        file.close()

        gpx = gpxpy.parse(gpx_xml)

        for track in gpx.tracks:
            for segment in track.segments:
                points += [segment.points]

    return points

def gather_locations(days):
    #print "GATHERDAYS"
    #print days
    res = {}
    for d in days.days:
        points = read_day_tracks(d.date)
        #print points
        if len(points):
            for s in d.entries:
                #print s.location
                a,b = s.start_gmt()
                if b==1:
                    a = a+60*24
                    cp = get_closest_points(points,well_formed_date(yesterday(d.date),a))
                elif b==2:
                    a = a-60*24
                    cp = get_closest_points(points,well_formed_date(tomorrow(d.date),a))
                else:
                    #print points
                    #print d.date, a
                    cp = get_closest_points(points,well_formed_date(d.date,a))
                if cp:
                    res[s.location] = res.get(s.location,[])+[cp]
                a,b = s.end_gmt()
                if b==1:
                    a = a+60*24
                    cp = get_closest_points(points,well_formed_date(yesterday(d.date),a))
                elif b==2:
                    a = a-60*24
                    cp = get_closest_points(points,well_formed_date(tomorrow(d.date),a))
                else:
                    cp = get_closest_points(points,well_formed_date(d.date,a))
                if cp:
                    res[s.location] = res.get(s.location,[])+[cp]
                from db_setup import insert_place_database
                try:

                    coords =  average_coords(res[s.location])
                    insert_place_database(s.location, coords)
                except KeyError:
                    insert_place_database(s.location, None)

    f=open("points.dat","w")
    cPickle.dump(days.days[-1].date,f)
    cPickle.dump(res,f)
    f.close()
    return res


def update_locations(days, last_data, last_date):
    #print days
    #print "Updating with latest date info..."
    res = last_data
    days.days.sort(lambda x,y: cmp(x.date,y.date))
    for d in days.days:
        #print "aqui", d.date
        #print "ali", last_date
        if datetime.datetime.strptime(d.date, '%Y_%m_%d')>datetime.datetime.strptime(last_date, '%Y_%m_%d'):
            #print d.date,
            points = read_day_tracks(d.date)
            if len(points):
                for s in d.entries:
                    #print s.location
                    a,b = s.start_gmt()
                    if b==1:
                        a = a+60*24
                        cp = get_closest_points(points,well_formed_date(yesterday(d.date),a))
                    elif b==2:
                        a = a-60*24
                        cp = get_closest_points(points,well_formed_date(tomorrow(d.date),a))
                    else:
                        cp = get_closest_points(points,well_formed_date(d.date,a))
                    if cp:
                        res[s.location] = res.get(s.location,[])+[cp]
                    a,b = s.end_gmt()
                    if b==1:
                        a = a+60*24
                        cp = get_closest_points(points,well_formed_date(yesterday(d.date),a))
                    elif b==2:
                        a = a-60*24
                        cp = get_closest_points(points,well_formed_date(tomorrow(d.date),a))
                    else:
                        cp = get_closest_points(points,well_formed_date(d.date,a))
                    if cp:
                        res[s.location] = res.get(s.location,[])+[cp]

                    from db_setup import insert_place_database
                    insert_place_database(s.location, average_coords(res[unicode(s.location)]))

    f=open("points.dat","w")
    cPickle.dump(days.days[-1].date,f)
    cPickle.dump(res,f)
    f.close()
    return res


def well_formed_date(day,hour):
    d = day.replace("_","-")
    hour = semantic_places.minutes_to_military(hour)
    d = d+"T"+hour[:2]+":"+hour[-2:]+":00" # GMT"
    d = time.mktime(time.strptime(d, '%Y-%m-%dT%H:%M:%S'))
    return d
    #return parser.parse(d)

def tomorrow(last_date):
  tmp=datetime.datetime(int(last_date[:4]),int(last_date[5:7]), int(last_date[8:10]))+timedelta(days=1)
  return "%4d_%02d_%02d" % (tmp.year,tmp.month,tmp.day)

def yesterday(last_date):
  tmp=datetime.datetime(int(last_date[:4]),int(last_date[5:7]), int(last_date[8:10]))+timedelta(days=-1)
  return "%4d_%02d_%02d" % (tmp.year,tmp.month,tmp.day)


def get_closest_points(points, ts, limit = 60, snap = True, snap_limit = 60):  # one minute, in both cases. snap_limit shoul always be less than limit
    before = None
    after = None
    # find the point immediately before and after the given timestamp
    ts = datetime.datetime.fromtimestamp(ts)
    if snap:
        for segment in points:
            if segment:
                # if we want to snap to the ends of the segments, then:
                #    - look for the segment's start. If it is within limit of ts, that is it!
                #    - look for the segment's end. If it is within the limit of ts, that is it!
                if abs(segment[0].time-ts)<=timedelta(seconds=snap_limit):
                    before = segment[0]
                if abs(segment[-1].time-ts)<=timedelta(seconds=snap_limit):
                    before = segment[-1]
    if before:
        return before
    for segment in points:
        for p in segment:
            if not after: #if there is an after, then we're done!
                if p.time>=ts:
                    after = p
                else:
                    before = p

    # now we have the before and after. Were they too far from ts? (based on limit)
    if before and (abs(before.time-ts)>timedelta(seconds=limit)):
        before = None
    if after and (abs(after.time-ts)>timedelta(seconds=limit)):
        after = None
    # so here we have either a ts or None, if too far. So return either or the average (or None...)
    if (not before) and after:
        return after
    if (not after) and before:
        return before
    #return None
    if (not before) and (not after):
        #print "none"
        return None

    #print after, before
    return geo.Location((after.latitude+before.latitude)/2,(after.longitude+before.longitude)/2,(after.elevation+before.elevation)/2)


#   #DATABASE
# try:
#     conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
# except:
#     print "I am unable to connect to the database."
#
# cur = conn.cursor()
#
# updated_locations = {}
# def update_db_places_according_to_cache(cached_locations):
#     #calculates the average of the points stored in cache
#     for location in cached_locations:
#         number_of_times = 1.0
#         name = location[0]
#         lat = location[1].latitude
#         lon = location[1].longitude
#         for place in cached_locations:
#             if name == place[0]:
#                 number_of_times += 1.0
#                 lat += place[1].latitude
#                 lon += place[1].longitude
#         lat = lat/number_of_times
#         lon = lon/number_of_times
#         updated_locations[name] = [lat, lon]
#
#     #try to find in DB or insert if new
#     for location in updated_locations.keys():
#         try:
#             cur.execute("SELECT ST_X(point) as lat, ST_Y(point) as lon FROM places WHERE description='" + location + "'")
#             coords = cur.fetchone()
#             lat = coords[0]
#             lon = coords[1]
#             #here is the average between the cache and the database. It should not be an average. somekind of stdv? TODO
#             update_location_coordinates_db(location, (updated_locations[location][0]+lat)/2.0, (updated_locations[location][1]+lon)/2.0)
#         except:
#             #insert average point in database
#             conn.rollback()
#             cur.execute("INSERT INTO places(description, point) VALUES('" + location + "','" + ppygis.Point(updated_locations[location][0], updated_locations[location][1], 0, srid=4326).write_ewkb() + "')")
#             conn.commit()
#
#
# def update_location_coordinates_db(location, lat, lon):
#     cur.execute("UPDATE places SET point ='" + ppygis.Point(lat, lon, 0, srid=4326).write_ewkb() + "' WHERE description ='" + location + "'")
#     conn.commit()
#
# def update_db_stays(location, start_date, end_date):
#     cur.execute("INSERT INTO stays(stay_id, start_date, end_date) VALUES('" + location + "', '" + str(start_date) + "', '" + str(end_date) + "')")
#
# def locations_of_points_in_db(points):
#     start = points[0]
#     end = points[-1]
#     cur.execute("SELECT description FROM places WHERE @(ST_X(point)-'" + str(start.latitude) + "') < 0.7 AND @(ST_Y(point)-'"  + str(start.longitude) + "') < 0.7")
#     res = cur.fetchall()
#     start_loc = res[0][0]
#     end_loc = res[1][0]
#     update_db_stays(start_loc, None, start.time)
#     updated_locations(end_loc, end.time, None)
#     return None
#
# def verify_locations(points, spans):
#     #in case there is no locationns.txt
#     if not spans:
#         return locations_of_points_in_db(points)
#     #stores locations as a dictionary (name, coords) in cache
#     start, end = gather_locations(points, spans)
#     return start, end
#
#
# def gather_locations(points, spans):
#     res ={}
#     start = end = ""
#     for s in spans:
#         #print s.start
#         if abs(s.end - points[0].time) < timedelta(seconds=120):
#             start = s.place
#             res[s.place] = points[0]
#         if abs(s.start - points[-1].time) < timedelta(seconds=120):
#             end = s.place
#             res[s.place] = points[-1]
#     return [(start, res[start]), (end, res[end])]