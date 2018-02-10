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
from azure.storage.blob import AppendBlobService

# get number of cores for the multi download ability
num_cores = multiprocessing.cpu_count()
# using semaphore to bound the number of active download threads
pool_sema = threading.BoundedSemaphore(value = 2 * num_cores)

# load custom filter from file
config = json.load(open(sys.argv[1]))

stats_endpoint_request = config["stats_endpoint_request"]
item_types_files = config["item_types_assets"]

planet_api_key = os.environ['PL_API_KEY']

if "blob_storage" in config:
  is_cloud_storage = True
  azure_blob_storage_account_name = os.environ['AZURE_BS_ACC_NAME']
  azure_blob_storage_API_KEY = os.environ['AZURE_BS_API_KEY']
  container_name = config["blob_storage"]["container_name"]
  append_blob_service = AppendBlobService(account_name=azure_blob_storage_account_name,\
                                        account_key=azure_blob_storage_API_KEY)
elif "download_directory" in config:
  is_cloud_storage = False
  download_directory = config["download_directory"]
else:
  raise Exception('No storage device given')

# init the session object
session = requests.Session()
# get the Planet API key from envitoment varible
session.auth = (planet_api_key, '')

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

  try:
    if is_cloud_storage:
      download_asset_to_cloud(feature, asset)
    else:
      download_asset_locally(feature, asset)
  except Exception as e:
    print(e)
  finally:
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

def download_asset_to_cloud(feature, asset):
  print("downloading asset of", feature["id"], 'to blob storage')

  dt = parser.parse(feature["properties"]["acquired"])
  prefix = '{}/{}/{}/{}/{}'.format(str(dt.year),\
                                   str(dt.month),\
                                   str(dt.day),\
                                   str(feature["properties"]["item_type"]),\
                                   str(feature["id"]))

  save_feature_json_to_cloud(feature, prefix)
  for file in item_types_files[feature["properties"]["item_type"]]:
    download_file_to_cloud(asset[file]["location"], prefix)

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

  print("saving:", filepath)
  with open(filepath, 'w') as f:
    print(json_dump, file=f)

def save_feature_json_to_cloud(feature, prefix):
  blob_name = '{}/{}.{}'.format(prefix,\
                                str(feature["id"]),\
                                "json")

  json_dump = json.dumps(feature,indent=4, sort_keys=True)

  if append_blob_service.exists(container_name, blob_name):
    if (len(json_dump) == \
        append_blob_service.get_blob_properties(container_name, blob_name).properties.content_length):
      print("already downloaded", blob_name, "skipping")
      return
    else:
      append_blob_service.delete_blob(container_name, blob_name)

  print("saving:", blob_name)
  append_blob_service.create_blob(container_name, blob_name)
  append_blob_service.append_blob_from_text(container_name, blob_name, json_dump)

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


def download_file_to_cloud(url, prefix):
  r = requests.get(url, allow_redirects=True)
  filename = str(re.findall("filename=\"(.+)\"", r.headers.get('Content-Disposition'))[0])
  blob_name = '{}/{}'.format(prefix,\
                             filename)

  # check if needs to re-download the blob
  if append_blob_service.exists(container_name, blob_name):
    if (int(r.headers.get('Content-Length')) == \
        append_blob_service.get_blob_properties(container_name, blob_name).properties.content_length):
      print("already downloaded", blob_name, "skipping")
      return
    else:
      append_blob_service.delete_blob(container_name, blob_name)

  print("downloading to:", blob_name)
  append_blob_service.create_blob(container_name, blob_name)
  for chunk in r.iter_content(chunk_size = 1024 * 1024):
    if chunk:
      append_blob_service.append_blob_from_bytes(container_name, blob_name, chunk)

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
