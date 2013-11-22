import datetime
import pdb
import psycopg2

from pydap.client import open_url

url='http://posada.solab.rshu.ru/pydap/public/allData/SSMI/f13/bmaps_v07/y1995/m05/f13_19950531v7_d3d.gz'

POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DBNAME = "cube"

dataset = open_url(url)

wspd = dataset.wspd[:,:,:]
time = dataset.time[:,:,:]

wspd_scale_factor = dataset.wspd.scale_factor
wspd_add_offset = dataset.wspd.add_offset
time_scale_factor = dataset.time.scale_factor
time_add_offset = dataset.time.add_offset

d=dataset.attributes['SSMI_GLOBAL']['original_filename']
mydate = d[d.index('_')+1:d.index('_')+9]
year=mydate[0:4]
mon=mydate[4:6]
day=mydate[6:8]
filedate = datetime.date(int(year), int(mon), int(day))

con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": POSTGRES_DBNAME,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD
})
cur = con.cursor()

for i in range(wspd.shape[0]):
    for j in range(wspd.shape[1]):

        # get coordinates
        lon = j
        lat = i

        # get wind speed value
        value = wspd[i, j, 0]

        if value < 250:
            value = value * wspd_scale_factor + wspd_add_offset

        # get date
        t = time[i, j, 0]
        if t < 250:
            t = t * time_scale_factor + time_add_offset
            hour = int(t/60)
            min = t%60
            filetime = datetime.time(int(hour), int(min))

        else:
            filetime = datetime.time(0, 0)

        date = str(filedate) + ' ' + str(filetime)

        # add to database
        wind_item = (date, float(value), lon, lat)

        if value < 250:
            cur.execute("INSERT INTO wind (datetime, wind_speed, lon, lat) VALUES (%s, %s, %s, %s)", wind_item)

con.commit()
con.close()