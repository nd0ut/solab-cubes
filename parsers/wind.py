import datetime
import ipdb

def parse(dbConn, dataset):
  cur = dbConn.cursor()

  lat_min = 600

  wspd_data = dataset.wspd.wspd[:,:,:]

  wspd = wspd_data[0][:,lat_min:,:]
  lats = wspd_data[2][lat_min:]
  lons = wspd_data[1][:]

  time = dataset.time.time[:,:,:][0]
  time = time[:,lat_min:,:]

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
        for k in range(wspd.shape[2]):

          # get wind speed value
          value = wspd[i, j, k]
          if value >= 250:
              continue
          else:
              value = value * wspd_scale_factor + wspd_add_offset

          # get coordinates
          lat = lats[j]
          lon = lons[i]
          part_day = k

          # get date
          t = time[i, j, k]
          if t < 250:
              t = t * time_scale_factor + time_add_offset
              hour = int(t/60)
              min = t%60

              if hour > 23:
                hour = 23
                min = 59

              filetime = datetime.time(int(hour), int(min))

          else:
              filetime = datetime.time(0, 0)

          date = str(filedate) + ' ' + str(filetime)
          #print date

          # add to database
          wind_item = (date, float(value), float(lon), float(lat), part_day)

          cur.execute("INSERT INTO wind (datetime, wind_speed, lon, lat, part_day) VALUES (%s, %s, %s, %s, %s)", wind_item)
  dbConn.commit()
