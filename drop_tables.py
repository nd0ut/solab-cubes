import config
import datetime
import pdb
import psycopg2

from pydap.client import open_url


con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": config.POSTGRES_DBNAME,
    "user": config.POSTGRES_USER,
    "password": config.POSTGRES_PASSWORD
})

cur = con.cursor()

cur.execute("DROP TABLE wind")

con.commit()
con.close()
