# -*- coding: UTF-8 -*-
__author__ = 'jmsfilipe'

import tornado.httpserver
import tornado.websocket
import tornado.ioloop
import tornado.web

clients = []
conn = None

def send_to_all_clients(msg):
    for client in clients:
        client.write_message(msg)

class WSHandler(tornado.websocket.WebSocketHandler):
    def open(self):
        #clients = []
        print 'new connection'
        if len(clients) == 0:
            global conn
            conn = init_database()

        clients.append(self)


    def on_message(self, message):
        message = json.loads(message)
        print message["message"]
        if message["message"] == "query data":
            global moreResults
            moreResults = []
            if len(message["data"]) == 1: #case its an empty query: the user hasnt sketched anything
                self.send(empty_query())
            else:
                msg, mapData = temporary_on_search(message["data"])
                if msg is None or msg == []:
                    self.send(empty_query())
                else:
                    for item in msg:
                        self.send(item)
                    self.send(mapData)
            clean()

        elif message["message"] == "entry map request":
            locations, trips = entry_map_request(message["data"])
            self.send(send_map_data(locations, trips))
        elif message["message"] == "location request":
            data = fetch_location_geojson(message["data"])
            msg = "{\"message\":\"location result\", \"data\": " + json.dumps(data) + "}"
            self.send(msg)
        elif message["message"] == "route request":
            data = fetch_geojson(message["data"])
            msg = "{\"message\":\"route result\", \"data\": " + json.dumps(data) + "}"
            self.send(msg)
        elif message["message"] == "location names request":
            msg = fetch_location_names()
            self.send(msg)
        elif message["message"] == "more results request":
            msg = fetch_more_results(message["data"])
            for i in msg:
                self.send(i)
        elif message["message"] == "colapsable map request":
            locations, trips = colapsable_map_request(message["data"])
            self.send(send_map_data(locations, trips))
        elif message["message"] == "save settings":
            save_settings(message["data"])
        elif message["message"] == "load settings":
            categories, colors = load_settings()
            msg = "{\"message\":\"settings result\", \"data\": " + json.dumps([categories, colors]) + "}"
            self.send(msg)

    def send(self, message):
        self.write_message(message)


    def on_close(self):
        clean()
        print 'connection closed'

    def check_origin(self, origin):
        return True


application = tornado.web.Application([
    (r'/', WSHandler),
])

import json
import datetime
import psycopg2
import ppygis

def save_settings(data):
    colors = data["colors"]
    categories = data["categories"]
    global conn

    query = "TRUNCATE TABLE colors, categories"
    cur = conn.cursor()
    cur.execute(query)
    conn.commit()

    for entry in colors:
        query = "INSERT INTO colors(category, color) VALUES('%s' , '%s')"%(entry[0], entry[1])
        cur = conn.cursor()
        cur.execute(query)

    for entry in categories:
        cur = conn.cursor()
        cur.execute("INSERT INTO categories(category, location) VALUES(%s , %s)" , (entry[0], entry[1]))
    conn.commit()

def load_settings():
    global conn

    query = "SELECT * FROM categories"
    cur = conn.cursor()
    cur.execute(query)
    tempCategories = cur.fetchall()

    query = "SELECT * FROM colors"
    cur = conn.cursor()
    cur.execute(query)
    tempColors = cur.fetchall()

    return tempCategories, tempColors

def empty_query():
    return "{\"message\":\"empty query\"}"

def fetch_location_names():
    final = []
    global conn
    query = " SELECT DISTINCT"\
            " stay_id " \
            " FROM stays "

    cur = conn.cursor()
    cur.execute(query)
    temp = cur.fetchall()
    for entry in temp:
        final.append(entry[0])

    return "{\"message\":\"location names result\", \"data\": " + json.dumps(final) + "}"

def fetch_location_geojson(location):
    global conn
    query =  "SELECT row_to_json(fc) " \
 " FROM ( SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features " \
 " FROM (SELECT 'Feature' As type " \
    " , ST_AsGeoJSON(lg.point)::json As geometry " \
    " , row_to_json((SELECT l FROM (SELECT description, description) As l " \
      " )) As properties" \
   " FROM places As lg WHERE description = '%s'  ) As f )  As fc " %(location)

    try:
        cur = conn.cursor()
        cur.execute(query)
        temp = cur.fetchone()
        temp = temp[0]
        return json.loads(temp)
    except:
        send_to_all_clients(empty_query())
        return None



def entry_map_request(ids):
    locations = {}
    trips = {}
    for pair in ids:
        id = pair[0]
        type = pair[1]
        if type == "interval":
            trips[id] = fetch_geojson(id)
        else:
            locations[id] = fetch_location_geojson(id)

    return locations, trips


def fetch_geojson(id):
    global conn
    query =  "SELECT row_to_json(fc) " \
 " FROM ( SELECT 'FeatureCollection' As type, array_to_json(array_agg(f)) As features " \
 " FROM (SELECT 'Feature' As type " \
    " , ST_AsGeoJSON(lg.geom)::json As geometry " \
    " , row_to_json((SELECT l FROM (SELECT trip_id, trip_id) As l " \
      " )) As properties" \
   " FROM linestrings As lg WHERE trip_id = %s  ) As f )  As fc " %(id)

    try:
        cur = conn.cursor()
        cur.execute(query)
        temp = cur.fetchone()
        temp = temp[0]
        return json.loads(temp)
    except:
        return None


def join_date_time(date, time): #join 01/01/2001 with 01:01 to a  datetime

    if not time[0].isdigit(): #get rid of the signal
        time = time[1:]

    if(date != "--/--/----"):
        return datetime.datetime.strptime(date + " " + time, "%d/%m/%Y %H:%M")
    else:
        return datetime.datetime.strptime(time, "%H:%M").strftime("%H:%M:%S")

import re
def duration_to_sql(duration):

    minutes = re.search('\d{1,2}(?=m)', duration)
    if minutes is not None:
        minutes = int(minutes.group(0))
    else:
        minutes = 0
    hours = re.search('\d{1,2}(?=h)', duration)
    if hours is not None:
        hours = int(hours.group(0))
    else:
        hours = 0

    final_minutes = 0

    if hours is not None and minutes is None:
        final_minutes = hours*60

    if minutes is not None and hours is None:
        final_minutes = minutes

    if minutes is not None and hours is not None:
        final_minutes = minutes + hours*60

    return final_minutes


import time
def fuzzy_to_sql(duration): #duration is divided in half for query purposes
    minutes = int(''.join([x for x in duration if x.isdigit()])) / 2.0 #equivalent to js parseInt
    minutes = time.strftime("%H:%M:%S", time.gmtime(minutes*60)) #uses seconds
    return minutes

def spatial_range_to_meters(range):
    return int(''.join([x for x in range if x.isdigit()]))

def get_sign(duration): #get the first char, being the sign
    if not duration[0].isdigit():
        if duration[0] == u'≤':
            return '<='
        elif duration[0] == u'≥':
            return '>='
        else:
            return duration[0]
    else:
        return '='

def get_all_but_sign(duration): #get the first char, being the sign
    result = ""
    if not duration[0].isdigit():
        return duration[1:-1]
    else:
        return duration

def is_full_date():
    global date
    if date != "--/--/----":
        return 'TIMESTAMP', ""
    else:
        return 'TIME', "::time"

def is_coordinates(loc):
    return  re.match(r'^[-+]?([1-8]?\d(\.\d+)?|90(\.0+)?),\s*[-+]?(180(\.0+)?|((1[0-7]\d)|([1-9]?\d))(\.\d+)?)$', loc)

def switch_coordinates(loc):
    coords = [x.strip() for x in loc.split(',')]
    return coords[1] + ", " + coords[0]

class Range:
    start = ""
    end = ""
    temporalStartRange = 0 #minutes
    temporalEndRange = 0
    duration = "" #6h15m 6h 5m
    location = ""
    spatialRange = 0 #meters
    durationSign = ""
    startSign = ""
    endSign = ""
    spatialSign = ""
    fullDate = ""
    castTime = ""
    query = ""
    locationCoords = ""

    def get_query(self):
        return self.query

    def __init__(self, start, end, temporalStartRange, temporalEndRange, duration, location, spatialRange):
        global date
        global previousEndDate

        self.fullDate, self.castTime = is_full_date()

        if start != "--:--":
            self.start = join_date_time(date, start)
            self.startSign = get_sign(start)
        else:
            self.start = None
            self.startSign = None

        if end != "--:--":
            self.end = join_date_time(date, end)
            self.endSign = get_sign(end)
        else:
            self.end = None
            self.endSign = None

        if(self.start is not None and get_all_but_sign(start) < previousEndDate and date != "--/--/----"):
            self.start += datetime.timedelta(days=1)
        if(self.end is not None and get_all_but_sign(end) < previousEndDate and date != "--/--/----"):
            self.end += datetime.timedelta(days=1)

        if temporalStartRange != "0min": #stored as half
            self.temporalStartRange = fuzzy_to_sql(temporalStartRange)
        else:
            self.temporalStartRange = None

        if temporalEndRange != "0min": #stored as half
            self.temporalEndRange = fuzzy_to_sql(temporalEndRange)
        else:
            self.temporalEndRange = None

        if duration != "duration":
            self.duration = duration_to_sql(duration)
            self.durationSign = get_sign(duration)
        else:
            self.duration = None
            self.durationSign = None

        if spatialRange != "0m":
            self.spatialRange = spatial_range_to_meters(spatialRange)
            self.spatialSign = get_sign(spatialRange)
        else:
            self.spatialRange = None
            self.spatialSign = ""

        if location != "local":
            if is_coordinates(location):
                self.locationCoords = switch_coordinates(location)
                self.location = None
                if self.spatialRange is None:
                    self.spatialRange = 0
                    self.spatialSign = "="
            else:
                self.locationCoords = None
                self.location = location
                if self.spatialRange is None:
                    self.spatialRange = 0
                    self.spatialSign = "="
        else:
            self.location = None
            self.locationCoords = None

    def generate_query(self):
        query = " SELECT stay_id, start_date, end_date FROM stays "
        global date
        if date != "--/--/----":
            query += " WHERE start_date::date = '%s'"%(date)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None \
                and self.duration is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "SELECT stay_id, start_date, end_date FROM stays WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.startSign != '=':
                query += " AND start_date %s '%s' " %(self.startSign, self.start)
            if self.endSign != '=':
                query += " AND end_date %s '%s' " %(self.endSign, self.end)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None \
                and self.temporalStartRange is None and self.duration is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "SELECT stay_id, start_date, end_date FROM stays WHERE start_date%s %s '%s'" \
                    " AND " \
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date %s '%s' " %(self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None \
                and self.temporalEndRange is None and self.duration is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = " SELECT stay_id, start_date, end_date FROM stays WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " end_date%s %s '%s'" %(self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date %s '%s' " %(self.startSign, self.start)

        if self.start is not None and self.end is not None \
                and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "SELECT stay_id, start_date, end_date FROM stays WHERE start_date%s %s '%s' " \
                    " AND " \
                    " end_date%s %s '%s'" %(self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)

        if self.start is not None \
                and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "SELECT stay_id, start_date, end_date, start_date, end_date FROM stays WHERE start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.end is not None \
                and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "SELECT stay_id, start_date, end_date FROM stays WHERE end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None\
                and self.duration is None and self.location is None and self.spatialRange is None and self.end is None and self.temporalEndRange is None and self.locationCoords is None:
            query = "SELECT stay_id, start_date, end_date FROM stays WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.end is not None and self.temporalEndRange is not None \
                and self.duration is None and self.location is None and self.spatialRange is None and self.start is None and self.temporalStartRange is None and self.locationCoords is None:
            query = "SELECT stay_id, start_date, end_date FROM stays WHERE"\
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

################
        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None\
                and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None\
                and self.temporalStartRange is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.duration is not None\
                and self.temporalEndRange is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date %s '%s'" %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.temporalStartRange, self.endSign, self.fullDate, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.end is not None and self.duration is not None\
                and self.temporalStartRange is None and self.temporalEndRange is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)

        if self.start is not None and self.duration is not None\
                and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.location is None and self.spatialRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' " %(self.durationSign, self.duration, self.castTime, self.startSign, self.start)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.end is not None and self.duration is not None\
                and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None and self.location is None and self.spatialRange is None and self.locationCoords:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.duration is not None\
                and self.location is None and self.spatialRange is None and self.end is None and self.temporalEndRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)


        if self.end is not None and self.temporalEndRange is not None and self.duration is not None\
                and self.location is None and self.spatialRange is None and self.start is None and self.temporalStartRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' AND " \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.durationSign, self.duration, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)


########
        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.locationCoords is not None and self.spatialSign is not None\
                and self.duration is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.temporalStartRange is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s %s '%s' " \
                    " AND " \
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.duration is None and self.temporalStartRange is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s %s '%s' " \
                    " AND " \
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.temporalEndRange is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " end_date%s %s '%s' AND "\
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.duration is None and self.temporalEndRange is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " end_date%s %s '%s' AND "\
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.end is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.temporalEndRange is None and self.temporalStartRange is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s %s '%s' " \
                    " AND " \
                    " end_date%s %s '%s' AND "\
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end, self.spatialSign, self.spatialRange)

        if self.start is not None and self.end is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.duration is None and self.temporalEndRange is None and self.temporalStartRange is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s %s '%s' " \
                    " AND " \
                    " end_date%s %s '%s' AND "\
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)


        if self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.start is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.castTime, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.duration is None and self.start is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.castTime, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.end is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.duration is None and self.end is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    " AND " \
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.start is None and self.temporalStartRange is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE "\
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.end is not None and self.temporalEndRange is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.duration is None and self.start is None and self.temporalStartRange is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE "\
                    " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND "\
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.spatialRange is not None and self.location is not None\
                and self.duration is None and self.end is None and self.temporalEndRange is None and self.locationCoords is None:
            query = "WITH l AS ( "\
                    "SELECT point "\
                    "FROM places "\
                    "WHERE description = '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "\
                    " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id " \
                    %(self.location, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.duration is None and self.end is None and self.temporalEndRange is None and self.location is None:
            query = "WITH l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                "SELECT stay_id, start_date, end_date FROM stays, places, l WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "\
                    " l.description = stays.stay_id " \
                    %(self.locationCoords, self.spatialSign, self.spatialRange, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)


###################

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.location is not None\
                and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ("\
                        "SELECT point "\
                        "FROM places "\
                        "WHERE description = '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND"\
                            " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id  "\
                            %(self.location, self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.fullDate, self.start, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.location is not None \
                and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND"\
                            " l.description = stays.stay_id  "\
                            %(self.location ,self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.fullDate, self.start, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)


        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.temporalStartRange is None and self.location is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ("\
                        "SELECT point "\
                        "FROM places "\
                        "WHERE description = '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND"\
                            " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id  "\
                            %(self.location, self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)


        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.temporalStartRange is None and self.location is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND"\
                            " l.description = stays.stay_id  "\
                            %(self.locationCoords, self.spatialSign, self.spatialRange, self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.temporalStartRange is not None and self.duration is not None and self.spatialRange is not None and self.location is not None\
                and self.temporalEndRange is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ("\
                        "SELECT point "\
                        "FROM places "\
                        "WHERE description = '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s %s '%s' AND"\
                            " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id  "\
                            %(self.location, self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.end is not None and self.temporalStartRange is not None and self.duration is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.temporalEndRange is None and self.location is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s %s '%s' AND"\
                            " l.description = stays.stay_id  "\
                            %(self.locationCoords, self.spatialSign, self.spatialRange, self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.location is not None\
                 and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ("\
                        "SELECT point "\
                        "FROM places "\
                        "WHERE description %s '%s' ) "\
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s %s '%s' AND"\
                            " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id  "\
                            %(self.location, self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end, self.spatialSign, self.spatialRange)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.location is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                            " SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s %s '%s' AND"\
                            " l.description = stays.stay_id  "\
                            %(self.locationCoords, self.spatialSign, self.spatialRange, self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)


        if self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.location is not None\
                and self.start is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ("\
                        "SELECT point "\
                        "FROM places "\
                        "WHERE description = '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND"\
                            " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id  "\
                            %(self.location, self.durationSign, self.duration, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange, self.spatialSign, self.spatialRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.start is None and self.location is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) AND"\
                            " l.description = stays.stay_id  "\
                            %(self.locationCoords, self.spatialSign, self.spatialRange, self.durationSign, self.duration, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.location is not None\
                and self.end is None and self.locationCoords is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ("\
                        "SELECT point "\
                        "FROM places "\
                        "WHERE description = '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " ST_DISTANCE(l.point, places.point) %s '%s' AND places.description = stays.stay_id  "\
                            %(self.location, self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.spatialSign, self.spatialRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.temporalEndRange is not None and self.duration is not None and self.spatialRange is not None and self.locationCoords is not None\
                and self.end is None and self.location is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays), " \
                    "l AS ( "\
                    "SELECT description "\
                    "FROM places "\
                    "WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326)) %s '%s' ) "\
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, places, l, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " l.description = stays.stay_id  "\
                            %(self.locationCoords, self.spatialSign, self.spatialRange, self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

##################

        if self.duration is not None\
                and self.start is None and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.location is None and self.spatialRange is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s'"\
                    %(self.durationSign, self.duration)

        if self.duration is not None and self.start is not None\
                and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.location is None and self.spatialRange is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' "\
                            "AND start_date%s %s '%s' " %(self.durationSign, self.duration, self.castTime, self.startSign, self.start)

        if self.duration is not None and self.end is not None\
                and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None and self.location is None and self.spatialRange is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' "\
                            "AND end_date%s %s '%s' " %(self.durationSign, self.duration, self.castTime, self.endSign, self.end)

        if self.duration is not None and self.end is not None and self.start is not None\
                and self.temporalStartRange is None and self.temporalEndRange is None and self.location is None and self.spatialRange is None:
            query = "WITH durations AS ( " \
                        "SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        "DATE_PART('hour', end_date - start_date)) * 60 +" \
                        "DATE_PART('minute', end_date - start_date) AS duration FROM stays) " \
                            "SELECT DISTINCT stay_id, start_date, end_date FROM stays, durations WHERE durations.duration %s '%s' "\
                            "AND end_date %s '%s' AND "\
                            "start_date%s %s '%s' "%(self.durationSign, self.duration, self.endSign, self.end, self.castTime, self.startSign, self.start)

        if self.location is not None \
            and self.end is None and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None and (self.spatialRange is None or self.spatialRange == 0):
            query = "SELECT DISTINCT stay_id, start_date, end_date FROM stays WHERE stay_id LIKE '%s'" %(self.location)

        if self.location is not None and (self.spatialRange is not None or self.spatialRange != 0)\
            and self.end is None and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None:
             query= " WITH l AS (  SELECT point  FROM  places  WHERE description = '%s' ), "\
             " k AS( "\
             " SELECT description FROM places,l "\
             " WHERE ST_Distance(places.point, l.point) %s '%s' )"\
            " SELECT stay_id, start_date, end_date FROM k inner join stays on description = stays.stay_id " %(self.location, self.spatialSign, self.spatialRange)

        if self.locationCoords is not None \
            and self.location is None and self.end is None and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None:
            query = " WITH l AS ( "\
                    " SELECT description "\
                    " FROM  places "\
                    " WHERE ST_Distance(point, ST_SetSRID(ST_MakePoint(%s),4326) ) %s '%s' ) "\
                    " SELECT DISTINCT stay_id, start_date, end_date FROM l,stays WHERE stay_id = l.description" %(self.locationCoords, self.spatialSign, self.spatialRange)

        print query
        self.query =  query



class Interval:
    start = ""
    end = ""
    temporalStartRange = 0 #minutes
    temporalEndRange = 0
    duration = ""
    route = ""
    durationSign = ""
    startSign = ""
    endSign = ""
    fullDate = ""
    castTime = ""
    query = ""

    def get_query(self):
        return self.query

    def __init__(self, start, end, temporalStartRange, temporalEndRange, duration, route):
        global date
        global previousEndDate

        self.fullDate, self.castTime = is_full_date()

        if start != "--:--":
            self.start = join_date_time(date, start)
            self.startSign = get_sign(start)
        else:
            self.start = None
            self.startSign = None

        if end != "--:--":
            self.end = join_date_time(date, end)
            self.endSign = get_sign(end)
        else:
            self.end = None
            self.endSign = None

        if(self.start is not None and get_all_but_sign(start) < previousEndDate):
            self.start += datetime.timedelta(days=1)
        if(self.end is not None and get_all_but_sign(end) < previousEndDate):
            self.end += datetime.timedelta(days=1)

        if temporalStartRange != "0min": #stored as half
            self.temporalStartRange = fuzzy_to_sql(temporalStartRange)
        else:
            self.temporalStartRange = None

        if temporalEndRange != "0min": #stored as half
            self.temporalEndRange = fuzzy_to_sql(temporalEndRange)
        else:
            self.temporalEndRange = None

        if duration != "duration":
            self.duration = duration_to_sql(duration)
            self.durationSign = get_sign(duration)
        else:
            self.duration = None
            self.durationSign = None

        if route != "route" and is_coordinates(route):
            self.route = switch_coordinates(route)
        else:
            self.route = None


    def generate_query(self):
        query = "SELECT DISTINCT trip_id, start_date, end_date FROM trips"

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None \
                and self.duration is None and self.route is None:
            query = "SELECT DISTINCT trip_id, start_date, end_date FROM trips WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    "AND " \
                    "end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None \
                and self.duration is None and self.route is None:
            query = "WITH a AS (SELECT trip_id, start_date, end_date FROM trips WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    "AND " \
                    "end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None \
                and self.temporalStartRange is None and self.duration is None and self.route is None:
            query = "SELECT DISTINCT trip_id, start_date, end_date FROM trips WHERE start_date%s %s '%s'" \
                    "AND " \
                    "end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.route is not None \
                and self.temporalStartRange is None and self.duration is None:
            query = "WITH a AS (SELECT trip_id, start_date, end_date FROM trips WHERE start_date%s %s '%s'" \
                    "AND " \
                    "end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

            query += ")"

            query += " SELECT  DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.start is not None and self.temporalStartRange is not None and self.end is not None \
                and self.temporalEndRange is None and self.duration is None and self.route is None:
            query = "SELECT DISTINCT trip_id, start_date, end_date FROM trips WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    "AND " \
                    "end_date%s %s '%s'" %(self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.route is not None \
                and self.temporalEndRange is None and self.duration is None:
            query = "WITH a AS (SELECT trip_id, start_date, end_date FROM trips WHERE start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND " \
                    "CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " \
                    "AND " \
                    "end_date%s %s '%s'" %(self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

        if self.start is not None and self.end is not None \
                and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None and self.route is None:
            query = "SELECT  DISTINCT trip_id, start_date, end_date FROM trips WHERE start_date%s %s '%s' " \
                    "AND " \
                    "end_date%s %s '%s'" %(self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.route is not None\
                and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None:
            query = "WITH a AS (SELECT trip_id, start_date, end_date FROM trips WHERE start_date%s %s '%s' " \
                    "AND " \
                    "end_date%s %s '%s'" %(self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.start is not None \
                and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None and self.route is None:
            query = "SELECT DISTINCT trip_id, start_date, end_date FROM trips WHERE start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.route is not None\
                and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None:
            query = "WITH a AS (SELECT trip_id, start_date, end_date FROM trips WHERE start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

        if self.end is not None \
                and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None and self.route is None:
            query = "SELECT DISTINCT trip_id, start_date, end_date FROM trips WHERE end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.end is not None and self.route is not None\
                and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None and self.duration is None:
            query = "WITH a AS (SELECT trip_id, start_date, end_date FROM trips WHERE end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

################
        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None\
                and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)



        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.route is not None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            ", a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None\
                and self.temporalStartRange is None and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.route is not None\
                and self.temporalStartRange is None:
            query = " WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

            query += ")"

            query += " DISTINCT SELECT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.duration is not None\
                and self.temporalEndRange is None and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.end is not None and self.duration is not None and self.route is not None\
                and self.temporalEndRange is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) "  \
                            " AND "  \
                            " end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange, self.castTime, self.endSign, self.end)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.start is not None and self.end is not None and self.duration is not None\
                and self.temporalStartRange is None and self.temporalEndRange is None and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)

        if self.start is not None and self.end is not None and self.duration is not None and self.route is not None\
                and self.temporalStartRange is None and self.temporalEndRange is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s %s '%s' "  \
                            " AND "  \
                            " end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start, self.castTime, self.endSign, self.end)
            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None\
                and self.route is None and self.start is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.durationSign, self.duration, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

        if self.temporalStartRange is not None and self.end is not None and self.temporalEndRange is not None and self.duration is not None and self.route is not None\
                and self.start is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " end_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS INTERVAL) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL)" %(self.durationSign, self.duration, self.castTime, self.end, self.fullDate, self.temporalEndRange, self.end, self.fullDate, self.temporalEndRange)
            if self.endSign != '=':
                query += " AND end_date%s %s '%s' " %(self.castTime, self.endSign, self.end)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.start is not None and self.temporalStartRange is not None and self.temporalEndRange is not None and self.duration is not None\
                and self.route is None and self.end is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS TIME) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

        if self.start is not None and self.temporalStartRange is not None and self.temporalEndRange is not None and self.duration is not None and self.route is not None\
                and self.end is None:
            query = " WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s' AND " \
                            " start_date%s BETWEEN CAST('%s' AS %s) - CAST('%s' AS TIME) AND "  \
                            " CAST('%s' AS %s) + CAST('%s' AS INTERVAL) " %(self.durationSign, self.duration, self.castTime, self.start, self.fullDate, self.temporalStartRange, self.start, self.fullDate, self.temporalStartRange)
            if self.startSign != '=':
                query += " AND start_date%s %s '%s' " %(self.castTime, self.startSign, self.start)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

#######################

        if self.duration is not None\
                and self.start is None and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                    %(self.durationSign, self.duration)

        if self.duration is not None and self.route is not None\
                and self.start is None and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                    %(self.durationSign, self.duration)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

        if self.route is not None\
                and self.duration is None and self.start is None and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None:

            query = " SELECT DISTINCT trips.trip_id, start_date, end_date FROM linestrings INNER JOIN trips ON (linestrings.trip_id = trips.trip_id) "\
                    " WHERE trips.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.duration is not None and self.start is not None\
                and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                            " AND start_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start)

        if self.duration is not None and self.start is not None and self.route is not None\
                and self.end is None and self.temporalStartRange is None and self.temporalEndRange is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                            " AND start_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.startSign, self.start)
            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)


        if self.duration is not None and self.end is not None\
                and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " SELECT DISTINCT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                            " AND end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.endSign, self.end)

        if self.duration is not None and self.end is not None and self.route is not None\
                and self.start is None and self.temporalStartRange is None and self.temporalEndRange is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a as (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                            " AND end_date%s %s '%s'" %(self.durationSign, self.duration, self.castTime, self.endSign, self.end)

            query += ")"

            query += " DISTINCT SELECT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

        if self.duration is not None and self.end is not None and self.start is not None\
                and self.temporalStartRange is None and self.temporalEndRange is None and self.route is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " DISTINCT SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                            " AND end_date%s %s '%s' AND "\
                            " start_date%s %s '%s' " %(self.durationSign, self.duration, self.castTime, self.endSign, self.end, self.castTime, self.startSign, self.start)

        if self.duration is not None and self.end is not None and self.start is not None and self.route is not None\
                and self.temporalStartRange is None and self.temporalEndRange is None:
            query = "WITH durations AS ( " \
                        " SELECT (DATE_PART('day', end_date - start_date) * 24 + " \
                        " DATE_PART('hour', end_date - start_date)) * 60 +" \
                        " DATE_PART('minute', end_date - start_date) AS duration FROM trips) " \
                            " , a AS (SELECT trip_id, start_date, end_date FROM trips, durations WHERE durations.duration %s '%s'"\
                            " AND end_date%s %s '%s' AND "\
                            " start_date%s %s '%s' " %(self.durationSign, self.duration, self.castTime, self.endSign, self.end, self.castTime, self.startSign, self.start)

            query += ")"

            query += " SELECT DISTINCT a.trip_id, a.start_date, a.end_date FROM a, linestrings "\
                    " WHERE a.trip_id = linestrings.trip_id AND ST_DWithin(linestrings.geom, ST_SetSRID(ST_MakePoint(%s),4326)::geography, 250)"%(self.route)

########################
        print query
        self.query =  query

items = []
date = ""
fullDate = ""
previousEndDate = ""
results = []
all = []
def init_database():
      #DATABASE
    try:
        return psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
    except:
        print "I am unable to connect to the database."

# def on_search(list):
#     global conn
#     parse_items(list)
#     generate_queries()
#     conn = init_database()
#     return fetch_from_db()

def fetch_more_results(id):
    o = []
    global moreResults
    items = moreResults[int(id)]
    for result in items:
        temp = "{\"message\":\"query colapsed results\", \"data\":["
        for item in result:
            item.groupBy = id
            temp += json.dumps(item.__dict__, cls = MyEncoder) + ","
        temp = temp[:-1]
        temp += "]}"
        o.append(temp)
    return o

def temporary_on_search(list):
    global conn
    parse_items(list)
    generate_queries()
    conn = init_database()
    return temporary_fetch_from_db()

moreResults = []

def represent_int(s):
    try:
        int(s)
        return True
    except ValueError:
        return False

def temporary_fetch_from_db():
    final = []
    global results, conn
    cur = conn.cursor()

    size = len(items)
    if size > 1:
        template = "SELECT %s " \
                   " FROM (%s) q1 INNER JOIN (%s) q2 ON q1.end_date = q2.start_date " \
                   " INNER JOIN (%s) q3 ON q2.end_date = q3.start_date "

        add_template = " INNER JOIN (%s) q%s ON q%s.end_date = q%s.start_date " \
                       " INNER JOIN (%s) q%s ON q%s.end_date = q%s.start_date "

        select = ""
        for i in range(1, size+1):
            if i % 2 == 0:
                select += "q"+str(i)+".trip_id,q"+str(i)+".start_date,q"+str(i)+".end_date, "
            else:
                select += "q"+str(i)+".stay_id,q"+str(i)+".start_date,q"+str(i)+".end_date, "
        select = select.rstrip(', ')

        template = template%(select, items[0].get_query(),items[1].get_query(), items[2].get_query())

        if size > 3:
            id = 4
            how_many = (size-3)/2
            for j in range(1, how_many+1):
                if id <= size:
                    template += add_template%(items[id-1].get_query(), id, id-1, id, items[id].get_query(), id+1, id, id+1)
                    id += 2
    elif size ==1:
        template = items[0].get_query()


    try:
        print "started query ", datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        print template
        cur.execute(template)
        temp = cur.fetchall()
        print "ended query ", datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        if temp == []:
            return ([],[])

        for result in temp:
            for i in range(0, size*3, 3):
                id = result[i]
                start_date = result[i+1]
                end_date = result[i+2]

                try:
                    int(id)
                    results.append(ResultInterval(id, start_date, end_date, None))
                except ValueError:
                    results.append(ResultRange(id, start_date, end_date, None))

            all.append(results)
            results = []
    except psycopg2.ProgrammingError as e:
        print "error ", e
        send_to_all_clients(empty_query())

    size2 = len(all)

    to_show = all

    to_show = refine_with_group_by(to_show)
    to_show = refine_with_group_by_date(to_show)



    for key, value in to_show.iteritems():
        global moreResults
        temp = value
        moreResults.append(temp)


    id = 0
    for key, value in to_show.iteritems():
        to_show[id] = to_show.pop(key)
        id += 1

    locations, trips = get_global_map_data(to_show)


    summary = quartiles(to_show, size2)

    i = 0
    import json
    end = []
    for key, value in summary.iteritems():
        if value != []:
            temp = "{\"message\":\"query colapsable results\", \"size\":\""+str(size)+"\", \"total\":\""+str(len(to_show))+"\",  \"data\":["
            for item in value:
                if represent_int(item[2]):
                    temp1 = ResultInterval(item[2], item[0], item[1], item[3])
                    temp1.moreResultsId = i
                    temp += json.dumps(temp1.__dict__, cls = MyEncoder) + ","
                else:
                    temp1 = ResultRange(item[2], item[0], item[1], item[3])
                    temp1.moreResultsId = i
                    temp += json.dumps(temp1.__dict__, cls = MyEncoder) + ","
            temp = temp[:-1]
            temp += "]}"
            end.append(temp)
        i += 1

    return end, send_global_map_data(locations, trips)

def quartiles(to_show, nr_queries):
    dict = {}

    for key, value in to_show.iteritems():
        size = len(value)
        startList = []
        endList = []

        startListInterval = []
        endListInterval = []

        for result in value:
            for range in result[::2]:
                startList.append((time.mktime(range.start_date.timetuple()), range.id, range.date))
                endList.append((time.mktime(range.end_date.timetuple()), range.id, range.date))
            for interval in result[1::2]:
                startListInterval.append((time.mktime(interval.start_date.timetuple()), interval.id, interval.date))
                endListInterval.append((time.mktime(interval.end_date.timetuple()), interval.id, interval.date))

        endListOrdered = sorted(endList, key = lambda tup: tup[0])
        startListOrdered = sorted(startList, key = lambda tup: tup[0])

        endListIntervalOrdered = sorted(endListInterval, key = lambda tup: tup[0])
        startListIntervalOrdered = sorted(startListInterval, key = lambda tup: tup[0])

        if size > 4:
            size = 4
        #size=4
        import numpy
        endTimes = numpy.array_split(numpy.array([x[0] for x in endListOrdered]), size)
        startTimes = numpy.array_split(numpy.array([x[0] for x in startListOrdered]), size)

        endTimesInterval = numpy.array_split(numpy.array([x[0] for x in endListIntervalOrdered]), size)
        startTimesInterval = numpy.array_split(numpy.array([x[0] for x in startListIntervalOrdered]), size)


        if nr_queries == 1:
            tempEnd = endTimes
            tempStart = startTimes
            endTimes = []
            startTimes = []

            for array in tempEnd:
                endTimes.append([sum(array)/len(array)])

            for array in tempStart:
                startTimes.append([sum(array)/len(array)])


        endT = []
        startT = []

        endTI = []
        startTI = []

        for array in endTimes:
            for date in array:
                endT.append(datetime.datetime.fromtimestamp(date))

        for array in startTimes:
            for date in array:
                startT.append(datetime.datetime.fromtimestamp(date))

        for array in endTimesInterval:
            for date in array:
                endTI.append(datetime.datetime.fromtimestamp(date))

        for array in startTimesInterval:
            for date in array:
                startTI.append(datetime.datetime.fromtimestamp(date))

        dict[key] = list(zip(startT, endT, [x[1] for x in startListOrdered], [x[2] for x in startListOrdered]) + zip(startTI, endTI, [x[1] for x in startListIntervalOrdered],[x[2] for x in startListIntervalOrdered]))

    return dict


def colapsable_map_request(id):
    id = int(id)
    items = moreResults[id][0]


    locations = {}
    trips = {}


    for item in items:
        if isinstance(item, ResultRange):
            locations[item.id] = fetch_location_geojson(item.id)
        else:
            trips[item.id] = fetch_geojson(item.id)

    return locations, trips

def send_map_data(locations, trips):

    message = {}
    message["message"] = "map data"
    message["data"] = {}
    message["data"]["locations"] = locations
    message["data"]["trips"] = trips

    send = json.dumps(message)
    return send

def send_global_map_data(locations, trips):

    message = {}
    message["message"] = "global map data"
    message["data"] = {}
    message["data"]["locations"] = locations
    message["data"]["trips"] = trips

    send = json.dumps(message)

    return send

def get_global_map_data(to_show):
    #dictionaries with all locations coordinates and
    #geojson for each FIRST trip (TODO, do not choose the first, make some kind of summary)
    locations = {}
    trips = {}

    for key, value in to_show.iteritems():
        locations_in_item = value[0][0::2]
        for location in locations_in_item:
            locations[location.id] = fetch_location_geojson(location.id)


    for key, value in to_show.iteritems():
        trips_in_item = value[0][1::2]
        for trip in trips_in_item:
            trips[trip.id] = fetch_geojson(trip.id)

    return locations, trips



from numpy import cumsum

import datetime as dtm
def avg_time(times):
    avg = 0
    for elem in times:
        avg += elem.second + 60*elem.minute + 3600*elem.hour
    avg /= len(times)
    rez = str(avg/3600) + ' ' + str((avg%3600)/60) + ' ' + str(avg%60)
    return dtm.datetime.strptime(rez, "%H %M %S")

def percentage_split(seq, percentages):
    cdf = cumsum(percentages)
    assert cdf[-1] == 1.0
    stops = map(int, cdf * len(seq))
    return [seq[a:b] for a, b in zip([0]+stops, stops)]


def generate_queries():
    for item in items:
        item.generate_query()

# import itertools
# def fetch_from_db():
#     final = []
#     global results, conn
#     cur = conn.cursor()
#     for item in items:
#         try:
#             cur.execute(item.get_query())
#             temp = cur.fetchall()
#             for result in temp:
#                 id = result[0]
#                 start_date = result[1]
#                 end_date = result[2]
#
#                 if isinstance(item, Range):
#                     results.append(ResultRange(id, start_date, end_date))
#                 else:
#                     results.append(ResultInterval(id, start_date, end_date))
#
#             all.append(results)
#             results = []
#         except psycopg2.ProgrammingError:
#             print "error"
#             send_to_all_clients(empty_query())
#
#     size = len(all)
#     to_show = []
#     if size > 1:
#         to_show = match_queries(all, size, 0, 1, 2, {}, 0)
#         to_show = remove_uncorresponding_entries(to_show, size)
#
#
#         to_show = refine_with_group_by(to_show, 2)
#
#
#     else:
#         #print all
#         for item in all:
#             for derp in item:
#                 to_show.append([derp])
#
#
#     import json
#     for result in to_show:
#         if result != []:
#             temp = "{\"message\":\"query results\", \"data\":["
#             for item in result:
#                 temp += json.dumps(item.__dict__, cls = MyEncoder) + ","
#             temp = temp[:-1]
#             temp += "]}"
#             final.append(temp)
#     return final

class groupby(dict):
    def __init__(self, seq, key=lambda x:x):
        for value in seq:
            k = key(value)
            self.setdefault(k, []).append(value)
    __iter__ = dict.iteritems

class groupbyDate(dict):
    def __init__(self, seq, key=lambda x:x):
        for value in seq:
            k = key(value)
            self.setdefault(str(k), []).append(value)
    __iter__ = dict.iteritems

def refine_with_group_by(to_show):
    dict = {}

    from operator import itemgetter

    to_show.sort(key=lambda item: item[0].id)
    for elt, items in groupby(to_show, lambda item: item[0].id):
        dict[elt] = []
        for i in items:
            dict[elt].append(i)
    #
    #     key += 1
    #
    #     for item in entry:
    #
    #         if dict
    #         try:
    #             dict[entry[i].id].append(entry)
    #         except KeyError:
    #             dict[entry[i].id] = []
    #             dict[entry[i].id].append(entry)
    #
    #
    #
    # else:
    # for k, g in groupby(to_show, key=lambda r: r[0].id):
    #      for record in g:
    #          if k not in dict:
    #              dict[k] = []
    #              dict[k].append(record)
    #          else:
    #              dict[k].append(record)

    return dict
    # dict = {}
    # for result in to_show:
    #     for entry in result:
    #         if isinstance(entry, ResultRange):
    #             if entry not in dict[entry.trip]:
    #                 dict[entry.trip] = 1
    #             else:
    #                 dict[entry.trip] += 1

    #return to_show


import datetime as DT
import itertools

start_date=DT.datetime.now()

def fortnight(date):
    return (date-start_date).seconds // 3600 // 5


def refine_with_group_by_date(to_show):
    dict = {}


    # for key, value in to_show.iteritems():
    #     print "value"
    #     print value
    #     for result in value:
    #         print "result"
    #         print result
    #         try:
    #             dict[key].append(result[2].start_date)
    #         except:
    #             dict[key] = []
    #             dict[key].append(result[2].start_date)
    id = 0
    for key1, value in to_show.iteritems():
        transactions=value

        transactions.sort(key=lambda r: r[0].end_date)

        for key,grp in itertools.groupby(transactions,key=lambda date:fortnight(date[0].end_date)):
            list1 = list(grp)
            try:
                dict[str(key1)+str(key)] += list1
            except KeyError:
                dict[str(key1)+str(key)] = []
                dict[str(key1)+str(key)] += list1
    return dict
    # dict = {}
    # for result in to_show:
    #     for entry in result:
    #         if isinstance(entry, ResultRange):
    #             if entry not in dict[entry.trip]:
    #                 dict[entry.trip] = 1
    #             else:
    #                 dict[entry.trip] += 1

   # return to_show

def remove_duplicates(values):
    output = []
    seen = set()
    for value in values:
        # If value has not been encountered yet,
        # ... add it to both list and set.
        if value not in seen:
            output.append(value)
            seen.add(value)
    return output

def remove_uncorresponding_entries(to_show, size):
    final = []
    for key in sorted(to_show):
        if len(to_show[key]) == size:
            final.append(to_show[key])


    return final

def match_queries(all, size, i, j, k, to_show, l):
    for a, b, c in itertools.product(all[i], all[j], all[k]):
       # print "size", size
       # print "len", len(to_show[l])
       #  if len(to_show[l]) == size+1:
       #      print "add"
       #      to_show.append([])
       #      l += 1
        if a.date == b.date and \
                b.date == c.date and \
                c.date == a.date and \
                a.end_date.strftime('%H:%M') == b.start_date.strftime('%H:%M') and \
                b.end_date.strftime('%H:%M') == c.start_date.strftime('%H:%M'):
            #if(k == 2): #add the first only when it runs once and is only 3 items
            try:
                #if a not in to_show[str(a.date)]:
                to_show[str(a.date)].append(a)
            except KeyError:
                to_show[str(a.date)] = []
                to_show[str(a.date)].append(a)

            try:
                #if b not in to_show[str(b.date)]:
                to_show[str(b.date)].append(b)
            except KeyError:
                to_show[str(b.date)] = []
                to_show[str(b.date)].append(b)

            try:
                #if c not in to_show[str(c.date)]:
                to_show[str(c.date)].append(c)
            except KeyError:
                to_show[str(c.date)] = []
                to_show[str(c.date)].append(c)

            i += 2
            j += 2
            k +=2
            if(size > 3 and j < size):
                match_queries(all, size, i, j, k, to_show, l)

    return to_show



def parse_items(list):
    obj = list
    global date
    date = obj[0]["date"]

    iterobj = iter(obj) #skip the first, that is the date
    next(iterobj)

    for item in iterobj:
        if item.get("spatialRange"): #its a range
            global previousEndDate
            if len(items) == 0:
                previousEndDate = get_all_but_sign(item["start"])
            else:
                previousEndDate = get_all_but_sign(item["end"])
            items.append(Range(item["start"], item["end"], item["temporalStartRange"], item["temporalEndRange"], item["duration"], item["location"], item["spatialRange"]))
        else: #its an interval
            items.append(Interval(item["start"], item["end"], item["temporalStartRange"], item["temporalEndRange"], item["duration"], item["route"]))

def clean():
    global items, clients, results, all
    #items = []
    #results = []
    #all = []
    del items[:]
    del clients[:]
    del results[:]
    del all[:]
    #clients = []

class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return str(obj)

        if isinstance(obj, datetime.date):
            return str(obj)

        return json.JSONEncoder.default(self, obj)

class ResultRange:
    id = ""
    start_date = None
    end_date = None
    type = "range"
    date = None

    def __init__(self, id, start_date, end_date, date):
        now = datetime.datetime.now()
        if date:
            self.date = date
        else:
            self.date = start_date.date()
        self.id = id
        self.start_date = start_date.replace(year=now.year, day=now.day, month=now.month)
        self.end_date = end_date.replace(year=now.year, day=now.day, month=now.month)
        self.type = "range"

    def __repr__(self):
        return str(self.id) + " " + str(self.start_date) + " " + str(self.end_date) + " "

    def __hash__(self):
        return hash((self.start_date, self.end_date, self.id, self.type))

    def __eq__(self, other):
        return self.start_date == other.start_date and self.end_date == other.end_date and self.id == other.id and self.type == other.type

class ResultInterval:
    id = ""
    start_date = None
    end_date = None
    type = "interval"
    date = None

    def __init__(self, id, start_date, end_date, date):
        now = datetime.datetime.now()
        if date:
            self.date = date
        else:
            self.date = start_date.date()
        self.id = id
        self.start_date = start_date.replace(year=now.year, day=now.day, month=now.month)
        self.end_date = end_date.replace(year=now.year, day=now.day, month=now.month)
        self.type = "interval"

    def __repr__(self):
        return str(self.id) + " " + str(self.start_date) + " " + str(self.end_date) + " "

    def __hash__(self):
        return hash((self.start_date, self.end_date, self.id, self.type))

    def __eq__(self, other):
        return self.start_date == other.start_date and self.end_date == other.end_date and self.id == other.id and self.type == other.type

if __name__ == "__main__":
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    tornado.ioloop.IOLoop.instance().start()