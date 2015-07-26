#!/usr/bin/env python
#
# Copyright 2009 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
"""Simplified chat demo for websockets.

Authentication, error handling, etc are left as an exercise for the reader :)
"""

import logging
import tornado.escape
import tornado.ioloop
import tornado.options
import tornado.web
import tornado.websocket
import os.path
import uuid

from tornado.options import define, options
import gpxpy
from gpxpy.name_locations import curtimezone

define("port", default=8888, help="run on the given port", type=int)


class Application(tornado.web.Application):
    def __init__(self):
        handlers = [
            (r"/", MainHandler),
            (r"/whib", SocketHandler),
        ]
        settings = dict(
            cookie_secret="__TODO:_GENERATE_YOUR_OWN_RANDOM_VALUE_HERE__",
            template_path=os.path.join(os.path.dirname(__file__), "templates"),
            static_path=os.path.join(os.path.dirname(__file__), "static"),
            xsrf_cookies=True,
        )
        tornado.web.Application.__init__(self, handlers, **settings)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.render("index.html", messages=SocketHandler.cache)


class SocketHandler(tornado.websocket.WebSocketHandler):
    clients = set()
    cache = []
    cache_size = 200

    def get_compression_options(self):
        # Non-None enables compression with default options.
        return {}

    def open(self):
        SocketHandler.clients.add(self)

    def on_close(self):
        SocketHandler.clients.remove(self)

    @classmethod
    def update_cache(cls, chat):
        cls.cache.append(chat)
        if len(cls.cache) > cls.cache_size:
            cls.cache = cls.cache[-cls.cache_size:]

    @classmethod
    def send_updates(cls, chat):
        logging.info("sending message to %d waiters", len(cls.clients))
        for client in cls.clients:
            try:
                print chat
                client.write_message(chat)
            except:
                logging.error("Error sending message", exc_info=True)

    old_content = ""
    def on_message(self, message):
        from gpxpy.name_locations import keep_processing, check_gpx_changes
        from gpxpy.semantic_places import read_days
        import websockets.Event as event
        logging.info("got message %r", message)
        parsed = tornado.escape.json_decode(message)
        import HTMLParser
        h = HTMLParser.HTMLParser()
        if parsed == "start":
            final = {}
            daysList = str(read_days("./semantics/location_semantics.txt"))
            global old_content
            old_content = daysList
            final["html"] = self.render_string("locations.html", table=h.unescape(daysList))
            SocketHandler.send_updates(final)
        if parsed[0] == "save":

            file = get_semantic_file_from_string(parsed[1])
            file.to_file("./semantics/location_semantics.txt")
            keep_processing()

curtimezone = "GMT"
def get_semantic_file_from_string(content): #gets a semantic string and resutnrs a semantic object
    from gpxpy.name_locations import SemanticFile, Entry, Day
    from gpxpy.semantic_places import  get_location, get_gmt
    curday=None
    #try:
    file = SemanticFile()
    mylist = content.split('\n')
    for line in mylist:
        if len(line)==0 or line[0]=="*" or line[0] == '\n':
            pass
        elif validate(line):
            curday = Day(get_date(line))
            file.add_day(curday)
        elif get_gmt(line):
            global curtimezone
            curtimezone = get_gmt(line)
        else:
            global curtimezone
            dates,descr = line.split(":")
            descr = get_location(line)
            curday.add_entry(Entry(dates[:4],dates[-4:],descr,curtimezone))
    global curtimezone
    curtimezone = "GMT"

    return file



import datetime
def validate(date_text):
    if date_text[:2] == "--":
        date_text = date_text[2:]
    print date_text
    try:
        return datetime.datetime.strptime(date_text, '%Y_%m_%d').date()
    except Exception, e:
        return False

def get_date(date_text):
    if date_text[:2] == "--":
        return date_text[2:]
    else:
        return date_text
def main():
    tornado.options.parse_command_line()
    app = Application()
    app.listen(options.port)
    tornado.ioloop.IOLoop.instance().start()
    #list_files()


if __name__ == "__main__":
    main()
