import numpy as np
import pandas as pd
import requests
import pymongo
import configparser
from datetime import datetime

# Read config file
config = configparser.ConfigParser(interpolation=None)
config.read('config.ini')

# API credentials
api_key = config['API_KEYS']['api_key']

# API url
url = 'http://api.weatherapi.com/v1/current.json'

# Read data from csv file
top_100 = pd.read_csv('Top100-US.csv', sep = ';')

client = pymongo.MongoClient('mongodb://mongo:27017/')
db = client['twitter']
collection = db['tweets']

db = client['weather']
collection = db['city_weather']

for element in top_100['Zip']:
    city_weather = {}
    city_weather['zip'] = element
    city_weather['city'] = top_100[top_100['Zip'] == element]['City'].values[0]
    # datetime object containing current date and time
    now = datetime.now()
    # dd/mm/YY H:M:S
    dt_string = now.strftime("%d/%m/%Y %H:%M:%S")
    city_weather['created_at'] = dt_string

    # Requests to API
    query = element
    params = {'key': api_key, 'q': query}
    response = requests.get(url, params=params)
    city_weather['weather'] = response.json()

    # Insert one new document to MongoDB
    collection.insert_one(city_weather)
