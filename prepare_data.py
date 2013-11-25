import pdb
import psycopg2
import urllib
from xml.dom.minidom import *

from pydap.client import open_url

import parsers

# Postgres config
POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DBNAME = "cube"

# initialize postgres connection
con = psycopg2.connect('dbname=%(dbname)s user=%(user)s password=%(password)s' % {
    "dbname": POSTGRES_DBNAME,
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD
})

root_path = 'http://posada.solab.rshu.ru/'
dap_folders_url = '%s/pydap/public/allData/SSMI/f13/bmaps_v07/' % root_path

# parse each year
xml = parse(urllib.urlopen(dap_folders_url + 'catalog.xml'))
folders = xml.getElementsByTagName('catalogRef')

for folder in folders:
  if folder.attributes['name'].value != 'weeks':
    year_url = root_path + folder.attributes['ID'].value
    year_name = folder.attributes['name'].value

    print 'Processing %s year' % year_name[1:]

    # parse each month
    xml = parse(urllib.urlopen(year_url + 'catalog.xml'))
    month_folders = xml.getElementsByTagName('catalogRef')

    for month_folder in month_folders:
      month_url = root_path + month_folder.attributes['ID'].value

      # parse each file
      xml = parse(urllib.urlopen(month_url + 'catalog.xml'))
      files = xml.getElementsByTagName('dataset')[0].getElementsByTagName('dataset')

      for file in files:
        file_url = root_path + file.attributes['ID'].value
        file_name = file.attributes['name'].value
        # add data to database
        dataset = open_url(file_url)
        parsers.wind.parse(con, dataset)

con.commit()
con.close()