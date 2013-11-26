import datetime
import pdb
import psycopg2

from pydap.client import open_url

POSTGRES_USER = "kate"
POSTGRES_PASSWORD = "06Sen2013"
POSTGRES_DBNAME = "cube_db"

con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": POSTGRES_DBNAME,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD
})

cur = con.cursor()

cur.execute("DROP TABLE wind")

con.commit()
con.close()
