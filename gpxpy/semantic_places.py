# -*- coding: cp1252 -*-

import os
import datetime


def military_to_minutes(ts):
    h = int(ts[:2])
    m = int(ts[-2:])
    return h*60+m



def minutes_to_military(minutes):
    h = minutes / 60
    m = minutes % 60
    tmp = "%2d%2d" % (h, m)
    tmp = tmp.replace(" ","0")
    return tmp

def minutes_to_military2(minutes):
    h = minutes / 60
    m = minutes % 60
    tmp = "%2d:%2d" % (h, m)
    tmp = tmp.replace(" ","0")
    return tmp

class Day:
    def __init__(self, date):
        self.date = date
        self.spans = []

    def add_span(self,span):
        self.spans.append(span)

    def all_places(self):
        res = {}
        for s in self.spans:
            res[s.place] = res.get(s.place,0)+s.length()
        return res

    def somewhere(self,exclude_travel=True):
        tmp = sum([x.length() for x in self.spans])
        if exclude_travel:
            return tmp-self.total_at("travel",cat=True)-self.total_at("sightseeing",cat=True)
        else:
            return tmp

    def moving(self):
        return 24*60-self.somewhere()


    def __repr__(self):
        tmp = self.date+"\n"
        for s in self.spans:
            tmp+=str(s)+"\n"
        return tmp




class Span:
    def __init__(self, start, end, place, timezone = "gmt+0"):
        # start, end in the "military time" format: "1543". timezone is "GMT+4", etc.
        # internally, we'll convert to minutes since the day began
        self.start = military_to_minutes(start)
        self.end = military_to_minutes(end)
        self.place = place
        if timezone=="gmt+0":
            self.timezone = 0
        else:
            self.timezone = int(timezone[3:])

    def start_gmt(self):
        x = self.start-self.timezone*60
        if x<0:
            return x, 1
        elif x>(60*24):
            return x, 2
        else:
            return x,0

    def end_gmt(self):
        x = self.end-self.timezone*60
        if x<0:
            return x, 1
        elif x>(60*24):
            return x, 2
        else:
            return x,0

    def length(self):
        return self.end - self.start

    def __repr__(self):
        if self.timezone==0:
            tz = "GMT+0"
        else:
            tz = "GMT"+str(self.timezone)
        return minutes_to_military(self.start)+"-"+ \
               minutes_to_military(self.end)+":"+self.place+" ("+tz+")"






########################################################################
########################################################################
########################################################################
########################################################################
curtimezone = "GMT+0"
def read_days(filename): #reads a semantic file and returns a semantic structure
    import websockets.chatdemo as validate
    from gpxpy.name_locations import SemanticFile, Entry, Day
    curday=None
    #try:
    file = SemanticFile()
    mylist = open(filename).read().splitlines()
    for line in mylist:
        if len(line)==0 or line[0]=="*" or line[0] == '\n' or line[0] == '\t' or line[0] == ' ':
            pass
        elif validate.validate(line):
            curday = Day(validate.get_date(line))
            file.add_day(curday)
        elif get_gmt(line):
            global curtimezone
            curtimezone = get_gmt(line)
        else:
            dates,descr = line.split(":")
            descr = get_location(line)
            global curtimezone
            curday.add_entry(Entry(dates[:4],dates[-4:],descr,curtimezone))
    global curtimezone
    curtimezone = "GMT+0"

    return file

def get_gmt(line):
    import re
    regexp = re.compile(r'(GMT(\+|\-)*[1-9]*)')
    found = regexp.search(line)
    if found is not None:
        return found.group(0).strip()
    else:
        return None

def get_location(line):
    import re
    regexp = re.compile(r'(?![\d{4}\-\d{4}:])(.+)')
    found = regexp.search(line)
    if found is not None:
        return found.group(0).strip()
    else:
        return None