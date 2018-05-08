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
import pymongo
from pymongo import MongoClient

# get number of cores for the multi download ability
num_cores = multiprocessing.cpu_count()
# using semaphore to bound the number of active download threads
pool_sema = threading.BoundedSemaphore(value = 2 * 5)

# load custom filter from file
config = json.load(open(sys.argv[1]))

stats_endpoint_request = config["stats_endpoint_request"]
item_types_files = config["item_types_assets"]

planet_api_key = os.environ['PL_API_KEY']

#setup db
client = MongoClient('mongodb://localhost:27017/')
db = client.planet
images_collection = db.images
images_collection.create_index([('updateTime', pymongo.ASCENDING)], unique=True)

if "download_directory" in config:
  is_cloud_storage = False
  download_directory = config["download_directory"]
else:
  raise Exception('No storage device given')

# init the session object
session = requests.Session()
# get the Planet API key from envitoment varible
session.auth = (planet_api_key, '')


def set_to_download(feature, item_type, item_id, acquired, asset):
  #Handle the case where the assest is empty
  if( not(asset) ):
    return 

  if( not(images_collection.find_one({ "_id": item_id })) ):
    print("\n Adding new image to DB - ", item_id)
    images_collection.insert_one({"_id": item_id, "status": "new", "updateTime": datetime.datetime.now(), "filesPath": "" , "feature": feature })

def activate_image(feature, item_type, item_id, acquired, asset):
  if( not( images_collection.find_one({ "_id": item_id }) ) ): 
    print("shouldn't activate - not in db")
    return

  if( not(images_collection.find_one({ "_id": item_id, "status":{"$in":["activated", "downloaded"]} })) ):
    if is_item_inactive(asset, item_type):
      print("starting a new activation thread")  
      t = threading.Thread(name='activate_asset', target=activate_asset, args=(feature,asset,))
      t.start()
    else:
      print(feature["id"], "is already active")
      print("\n changing status to activated in DB for - ", feature["id"])
      images_collection.update({"_id": feature["id"]}, {"$set": {"status": "activated", "updateTime": datetime.datetime.now() }})


def download_image(feature, item_type, item_id, acquired, asset):
  if(not(images_collection.find_one({ "_id": item_id }))):
    print("shouldn't download - not in db")
    return

  if(not(images_collection.find_one({ "_id": item_id, "status": "downloaded" }))):
    if is_item_active(asset, item_type):
      print("starting a new download thread")
      t = threading.Thread(name='download_asset', target=download_asset, args=(feature,asset,))
      t.start()
    else:
      print(feature["id"], "is not active yet, skipping and returning to it next loop")


def download_page(feature):
  item_type = feature["properties"]["item_type"]
  item_id = feature["id"]
  acquired = feature["properties"]["acquired"]
  asset = json.loads(session.get(feature["_links"]["assets"]).text)

  should_download = True
  set_to_download(feature, item_type, item_id, acquired, asset)
  if( not(images_collection.find_one({ "_id": feature["id"] })) ):
    should_download = False
  
  if should_download:
    activate_image(feature, item_type, item_id, acquired, asset)
    download_image(feature, item_type, item_id, acquired, asset)

# What we want to do with each page of search results
def handle_page(page):
  for feature in page["features"]:
    download_page(feature)

# Check active state

def is_item_inactive(asset, item_type):
  for file in item_types_files[item_type]:
    if (asset[file]["status"] == 'inactive'):
      return True
  return False

def is_item_active(asset, item_type):
  for file in item_types_files[item_type]:
    if (asset[file]["status"] != 'active'):
      return False
  return True


def activate_asset(feature, asset):
  global pool_sema
  pool_sema.acquire()

  try:
    print("activating", feature["id"])
    for file in item_types_files[feature["properties"]["item_type"]]:
      activation_url = asset[file]["_links"]["activate"]
      response = session.post(activation_url)
      print(file, "response:", response.status_code)
  except Exception as e:
    pool_sema.release()
    print(e)
  finally:
    print("\n changing status to activated in DB for - ", feature["id"])
    images_collection.update({"_id": feature["id"]}, {"$set": {"status": "activated", "updateTime": datetime.datetime.now() }})
    pool_sema.release()


def download_asset(feature, asset):
  global pool_sema
  pool_sema.acquire()
  files_path = ""
  print("going to download - ", feature["id"])

  try:
    if is_cloud_storage:
      download_asset_to_cloud(feature, asset)
    else:
      files_path = download_asset_locally(feature, asset)
  except Exception as e:
    pool_sema.release()
    print(e)
  finally:
    print("\n changing status to downloaded in DB for - ", feature["id"])
    images_collection.update({"_id": feature["id"]}, {"$set": {"status": "downloaded", "updateTime": datetime.datetime.now(), "filesPath": files_path }})
    pool_sema.release()


def download_asset_locally(feature, asset):
  print("downloading asset of", feature["id"], 'locally')

  dt = parser.parse(feature["properties"]["acquired"])

  directory = '{}/{}/{}/{}/{}/{}'.format(str(download_directory),\
                                  str(dt.year),\
                                  str(dt.month),\
                                  str(dt.day),\
                                  str(feature["properties"]["item_type"]),\
                                  str(feature["id"]))

  if not os.path.exists(directory):
    os.makedirs(directory)
  save_feature_json_locally(feature, directory)
  for file in item_types_files[feature["properties"]["item_type"]]:
    download_file_locally(asset[file]["location"], directory)
  print("downloaded - ", feature["id"])

  return directory

def save_feature_json_locally(feature, directory):
  filename = '{}.{}'.format(str(feature["id"]),\
                               "json")
  filepath = '{}/{}'.format(str(directory),\
                               filename)

  json_dump = json.dumps(feature,indent=4, sort_keys=True)

  if os.path.exists(filepath):
    if (len(json_dump) == \
        os.path.getsize(filepath) - 1):
      print("already downloaded", filename, "skipping")
      return
    else:
      os.remove(filepath)

  print("saving json:", filepath)
  with open(filepath, 'w') as f:
    print(json_dump, file=f)

def download_file_locally(url, directory):
  r = requests.get(url, allow_redirects=True)
  filename = str(re.findall("filename=\"(.+)\"", r.headers.get('Content-Disposition'))[0])
  filepath = '{}/{}'.format(str(directory),\
                               filename)

  if os.path.exists(filepath):
    if (int(r.headers.get('Content-Length')) == \
        os.path.getsize(filepath)):
      print("already downloaded", filename, "skipping")
      return
    else:
      os.remove(filepath)

  print("downloading to:", filepath)
  with open(filepath, 'wb') as f:
    for chunk in r.iter_content(chunk_size = 1024 * 1024):
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

  #add a loop to firstly download all the things we have yet to download
  while(images_collection.count({"status":{"$in":["new", "activated"]}}) > 0 ):
    for image in images_collection.find({"status":{"$in":["new", "activated"]}}):
      print("image in DB wasn't downloaded:", image["_id"])
      download_page(image["feature"]) 

  # Create a Saved Search
  saved_search = \
    session.post(
      'https://api.planet.com/data/v1/searches/',
      json=stats_endpoint_request)

  # This is what is needed to execute the search.
  saved_search_id = saved_search.json()["id"]

  first_page = \
      ("https://api.planet.com/data/v1/searches/{}" +
          "/results?_page_size={}").format(saved_search_id, 250)

  # kick off the pagination
  fetch_page(first_page)
  time.sleep(600)