import datetime

def parse(dbConn, dataset):
  cur = dbConn.cursor()

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