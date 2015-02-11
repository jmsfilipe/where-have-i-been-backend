__author__ = 'jmsfilipe'
import psycopg2
import ppygis

#DATABASE
try:
    conn=psycopg2.connect("host=localhost dbname=postgres user=postgres password=postgres")
except:
    print "I am unable to connect to the database."

cur = conn.cursor()

cur.execute('SELECT * FROM gtest')
for row in cur:
    print row[0]

conn.commit()

cur.close()
conn.close()