__author__ = 'jmsfilipe'

import httplib, urllib
file = open('teste.gpx', 'rb')

headers = {'Content-Type':'application/gpx+xml', 'Accept':'application/json'}
conn = httplib.HTTPConnection('test.roadmatching.com')
conn.request("POST", "/rest/mapmatch/?app_id=b84af26e&app_key=e1072ef78b5c8fe69b29cfd719eaf162", file, headers)
response = conn.getresponse()
print response.status, response.reason

data = response.read()
print data

conn.close()