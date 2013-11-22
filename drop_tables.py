import datetime
import pdb
import psycopg2

from pydap.client import open_url

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DBNAME = "cube"

con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": POSTGRES_DBNAME,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD
})

cur = con.cursor()

cur.execute("DROP TABLE wind")

con.commit()
con.close()