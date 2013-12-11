import config
import psycopg2

con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": config.POSTGRES_DBNAME,
    "user": config.POSTGRES_USER,
    "password": config.POSTGRES_PASSWORD
})

cur = con.cursor()

cur.execute("CREATE TABLE IF NOT EXISTS wind (id SERIAL, datetime timestamp, wind_speed REAL, lon REAL, lat REAL, part_day REAL)")
cur.execute("create index ymdhm_idx on wind ( extract(year from datetime), extract(month from datetime), extract(day from datetime), extract(hour from datetime), extract(minute from datetime), lat, lon  );")

con.commit()
con.close()
