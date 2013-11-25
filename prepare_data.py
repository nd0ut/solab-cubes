import pdb
import psycopg2
import urllib
from xml.dom.minidom import *
from pydap.client import open_url

import parsers


def addPydapToUrl(url):
  return insert(url, 'pydap/', len(root_path))

def insert(original, new, pos):
  return original[:pos] + new + original[pos:]

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
    year_url = addPydapToUrl(folder.attributes['xlink:href'].value)
    year_name = folder.attributes['name'].value

    print 'Processing %s year' % year_name[1:]

    # parse each month
    xml = parse(urllib.urlopen(year_url))
    month_folders = xml.getElementsByTagName('catalogRef')

    for month_folder in month_folders:
      month_url = addPydapToUrl(month_folder.attributes['xlink:href'].value)

      # parse each file
      xml = parse(urllib.urlopen(month_url))
      files = xml.getElementsByTagName('dataset')[0].getElementsByTagName('dataset')

      for data_file in files:
        file_url = root_path + 'pydap/' + data_file.attributes['urlPath'].value
        file_name = data_file.attributes['name'].value

        # add data to database
        dataset = open_url(file_url)
        parsers.wind.parse(con, dataset)

con.commit()
con.close()