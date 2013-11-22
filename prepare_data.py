import datetime
import pdb
import psycopg2

from pydap.client import open_url

import parsers

url='http://opendap.solab.rshu.ru:8080/opendap/allData/SSMI/f13/bmaps_v07/y1995/m05/f13_19950503v7.gz'

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DBNAME = "cube"

dataset = open_url(url)

con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": POSTGRES_DBNAME,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD
})

parsers.wind.parse(con, dataset)

con.commit()
con.close()