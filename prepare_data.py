import config
from time import gmtime, strftime
import re
import traceback
import sys
import ipdb
import psycopg2
import urllib
from xml.dom.minidom import *
from pydap.client import open_url
from pydap.exceptions import DapError

import parsers

con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": config.POSTGRES_DBNAME,
    "user": config.POSTGRES_USER,
    "password": config.POSTGRES_PASSWORD
})

print "Job started at %s" % strftime("%Y-%m-%d %H:%M:%S", gmtime())

parsed_log_file = open('parsed.log', 'a+')
parsed_arr = parsed_log_file.readlines()
parsed_arr = map(lambda file: file.strip(), parsed_arr)

root_path = 'http://posada.solab.rshu.ru'
dap_folders_url = '%s/pydap/public/allData/SSMI/f13/bmaps_v07/' % root_path

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
          sys.stdout.write('    %s' % file_name)
          sys.stdout.flush()

          if file_name in parsed_arr:
            print " SKIP"
            continue

          try:
            dataset = open_url(file_url)
            parsers.wind.parse(con, dataset)
            parsed_log_file.write('%s\n' % file_name)
            parsed_log_file.flush()
            print " OK"
          except (DapError, RuntimeError, TypeError, ValueError), e:
            print " ERROR"
            print e
            traceback.print_exc()


        con.commit()

con.close()
parsed_log_file.close()