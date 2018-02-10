# TODO: OBJECT STORE (BLOB STORAGE)
# TODO: MAYBE ADD DB AND NOT CHECK DOWNLOADED DIRECTORY
import json
import os
import requests
from requests.auth import HTTPBasicAuth
import datetime
import time
from dateutil import parser
import sys
import multiprocessing
import threading
import re

# get number of cores for the multi download ability
num_cores = multiprocessing.cpu_count()
# using semaphore to bound the number of active download threads
pool_sema = threading.BoundedSemaphore(value = 2 * num_cores)

# load custom filter from file
config = json.load(open(sys.argv[1]))

stats_endpoint_request = config["stats_endpoint_request"]
download_directory = config["download_directory"]
item_types_files = config["item_types_assets"]

# init the session object
session = requests.Session()
# get the Planet API key from envitoment varible
session.auth = (os.environ['PL_API_KEY'], '')

# What we want to do with each page of search results
def handle_page(page):
  for feature in page["features"]:
    global index
    index += 1
    item_type = feature["properties"]["item_type"]
    item_id = feature["id"]
    acquired = feature["properties"]["acquired"]

    # print feature for debug purposes
    # print(json.dumps(feature, indent=4, sort_keys=True))

    # request an item
    asset = json.loads(session.get(feature["_links"]["assets"]).text)

    # print assest for debug purposes
    # print(json.dumps(asset, indent=4, sort_keys=True))

    print(index, acquired, item_type, item_id, sep='\t')

    if is_item_inactive(asset, item_type):
      t = threading.Thread(name='activate_visual', target=activate_visual, args=(feature,asset,))
      t.start()
    else:
      print(feature["id"], "is active")

    if is_item_active(asset, item_type):
      t = threading.Thread(name='download_asset', target=download_asset, args=(feature,asset,))
      t.start()
    else:
      print(feature["id"], "is not active yet, skipping and returning to it next loop")
      global query
      query = True

    # print(index, acquired, item_type, item_id, product["visual"]["status"], sep='\t')

def is_item_inactive(asset, item_type):
  for file in item_types_files[item_type]:
    if asset["visual"]["status"] == 'inactive':
      return True
  return False

def is_item_active(asset, item_type):
  for file in item_types_files[item_type]:
    if asset["visual"]["status"] != 'active':
      return False
  return True

def activate_visual(feature, asset):
  global pool_sema
  pool_sema.acquire()

  try:
    print("activating", feature["id"])
    for file in item_types_files[feature["properties"]["item_type"]]:
      activation_url = asset[file]["_links"]["activate"]
      response = session.post(activation_url)
      print(file, "response:", response.status_code)
  finally:
    pool_sema.release()

def download_asset(feature, asset):
  global pool_sema
  pool_sema.acquire()

  print("downloading asset of", feature["id"])

  dt = parser.parse(feature["properties"]["acquired"])

  directory = '{}/{}/{}/{}/{}/{}'.format(str(download_directory),\
                                  str(dt.year),\
                                  str(dt.month),\
                                  str(dt.day),\
                                  str(feature["properties"]["item_type"]),\
                                  str(feature["id"]))
  try:
    if not os.path.exists(directory):
      os.makedirs(directory)
      save_feature_json(feature, directory)
      for file in item_types_files[feature["properties"]["item_type"]]:
        download_file(asset[file]["location"], directory)
    else:
      print("already downloaded", feature["id"], "skipping")
  finally:
    pool_sema.release()

def save_feature_json(feature, directory):
  filepath = '{}/{}.{}'.format(str(directory),\
                               str(feature["id"]),\
                               "json")
  print("saving:", filepath)
  with open(filepath, 'w') as f:
    print(json.dumps(feature,indent=4, sort_keys=True), file=f)

def download_file(url, directory):
  r = requests.get(url, allow_redirects=True)
  filename = str(re.findall("filename=\"(.+)\"", r.headers.get('Content-Disposition'))[0])
  filepath = '{}/{}'.format(str(directory),\
                               filename)
  print("downloading to:", filepath)
  with open(filepath, 'wb') as f:
    for chunk in r.iter_content(chunk_size=1024):
      if chunk:
        f.write(chunk)

# How to Paginate:
# 1) Request a page of search results
# 2) do something with the page of results
# 3) if there is more data, recurse and call this method on the next page.
def fetch_page(search_url):
    page = session.get(search_url).json()
    handle_page(page)
    next_url = page["_links"].get("_next")
    if next_url:
      fetch_page(next_url)

query = True
while (query):
  query = False
  # Create a Saved Search
  saved_search = \
    session.post(
      'https://api.planet.com/data/v1/searches/',
      json=stats_endpoint_request)

  # This is what is needed to execute the search.
  saved_search_id = saved_search.json()["id"]

  index = 0

  first_page = \
      ("https://api.planet.com/data/v1/searches/{}" +
          "/results?_page_size={}").format(saved_search_id, 250)

  # kick off the pagination
  fetch_page(first_page)
