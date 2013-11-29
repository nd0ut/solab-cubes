import config
from time import gmtime, strftime
import re
import ipdb
import psycopg2
import urllib
from xml.dom.minidom import *
from pydap.client import open_url

import parsers

con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": config.POSTGRES_DBNAME,
    "user": config.POSTGRES_USER,
    "password": config.POSTGRES_PASSWORD
})

print "Job started at %s" % strftime("%Y-%m-%d %H:%M:%S", gmtime())

root_path = 'http://posada.solab.rshu.ru'
dap_folders_url = '%s/pydap/public/allData/SSMI/f13/bmaps_v07/' % root_path

filecounter = 0

# each year
xml = parse(urllib.urlopen(dap_folders_url + 'catalog.xml'))
folders = xml.getElementsByTagName('catalogRef')

for folder in folders:
  if folder.attributes['name'].value != 'weeks':
    year_url = folder.attributes['xlink:href'].value
    year_name = folder.attributes['name'].value

    print ""
    print ""
    print 'Processing %s year' % year_name[1:]

    # each month
    xml = parse(urllib.urlopen(year_url))
    month_folders = xml.getElementsByTagName('catalogRef')

    for month_folder in month_folders:
      month_url = month_folder.attributes['xlink:href'].value
      month_name = month_folder.attributes['name'].value

      print '  %s month' % month_name[1:]

      # each file
      xml = parse(urllib.urlopen(month_url))
      files = xml.getElementsByTagName('dataset')[0].getElementsByTagName('dataset')

      for data_file in files:
        file_url = root_path + '/pydap/' + data_file.attributes['urlPath'].value
        file_name = data_file.attributes['name'].value

        if re.match('.+_..........\.gz', file_name):
          if filecounter <= 2 :
            print '    %s' % file_name

            dataset = open_url(file_url)
            parsers.wind.parse(con, dataset)
            filecounter = filecounter + 1

        con.commit()

con.close()
