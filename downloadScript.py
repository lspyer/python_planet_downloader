import os
import json
import sys
import datetime
import time
from dateutil import parser
import pymongo
from dateutil import parser
import shutil 
from distutils.dir_util import copy_tree
from pymongo import MongoClient

#setup db
client = MongoClient('mongodb://localhost:27017/')
db = client.planet
images_collection = db.images
images_collection.create_index([('updateTime', pymongo.ASCENDING)], unique=True)

# load custom filter from file
config = json.load(open(sys.argv[1]))
last_download_time = config["time"]

config["time"] = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")

#update the filter for next download
with open(sys.argv[1], "w") as f:
	json.dump(config, f)

#make directories to copy to 
if not os.path.exists("../image_downloads"):
	os.mkdir("../image_downloads")

dir_route = "../image_downloads/download_"
directory_time = str(datetime.datetime.now().strftime("%Y-%m-%d--%H-%M"))
dir_full = dir_route + directory_time

os.mkdir(dir_full)

#copy all files into new directory 
for image in images_collection.find({"status": "downloaded", "updateTime": {"$gte": parser.parse(last_download_time) }}):
	path = image["filesPath"]
	print("copied image ", image["_id"])
	os.system("cp -rf " + path + " " + dir_full)