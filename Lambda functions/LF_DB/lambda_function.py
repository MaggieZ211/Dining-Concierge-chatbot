from collections import defaultdict
import csv
import time
from datetime import datetime
from decimal import *
import json
import boto3
import requests

def emptyStringFunc(s):
    if len(str(s)) != 0:
        return s
    else:
        return 'N/A'

def connect_dynamoDB():
    #Call the Yelp API to collect 5,000+ random restaurants from Manhattan
    #Use put_item function to put the data into dynamoDB
    
    
    #Connect to dynamoDB
    
    db = boto3.resource('dynamodb', region_name='us-east-1')
    table = db.Table("yelp-restaurants")


    #Call the Yelp API
    #Follow the instruction for Yelp website
    #Generate our unique api_key in Yelp website
    #Provide headers and url for business search
    
    api_key = 'J3yHwq61qketjMbsKvO9f7wFl4JxLqT3rv9wpdNgpnIEcDsnTMcSfBjGks1UIlxaTsWsctH3OKiOx-ehK7rjs-lHBj5AItJLbhKXyAGhs82ggOLcYJyUYf5QtfRhYXYx'
    headers = {'Authorization': 'Bearer %s' % api_key}
    url = 'https://api.yelp.com/v3/businesses/search'


    #According to descriptions in https://www.yelp.com/developers/documentation/v3/business_search ------
    #term: Search term, for example "food" or "restaurants". The term may also be business names, such as "Starbucks".
    #limit: Number of business results to return. By default, it will return 20. Maximum is 50.
    #location: The geographic area to be used when searching for businesses.
    #offset: Offset the list of returned business results by this amount.

    params = {'term': 'chinese',
              'limit': 50,
              'location': 'Manhattan',
              'offset': 0
    }

    #7 kinds of cuisines are allowed for search
    cuisines_lists = ['italian', 'american', 'japanese', 'chinese', 'korean', 'indian', 'mexican']

    #for each cuisine, fetch 200 restaurants from yelp API
    for cuisine in cuisines_lists:
        for i in range(0,4):
            params['term'] = cuisine
            params['offset'] = i * 50
            # make a request and then get response from yelp API
            response = requests.get(url, params=params, headers=headers)
            # response.json() returns a JSON object of the result
            business_entries = response.json()["businesses"]
            # insert data into dynamoDB
            for entry in business_entries:
                # insertedAtTimestamp
                now = datetime.now()
                datetime_format = now.strftime("%d/%m/%Y %H:%M:%S")
                table.put_item(
                    Item = {
                        'Business_ID': emptyStringFunc(entry['id']),
                        'insertedAtTimestamp': emptyStringFunc(datetime_format),
                        'Name': emptyStringFunc(entry['name']),
                        'Cuisine': emptyStringFunc(cuisine),
                        'Rating': emptyStringFunc(Decimal(entry['rating'])),
                        'Number of Reviews': emptyStringFunc(Decimal(entry['review_count'])),
                        'Address': emptyStringFunc(entry['location']['address1']),
                        'Zip Code': emptyStringFunc(entry['location']['zip_code']),
                        'Latitude': emptyStringFunc(str(entry['coordinates']['latitude'])),
                        'Longitude': emptyStringFunc(str(entry['coordinates']['longitude'])),
                        'Open': 'N/A'
                    }
                )
                #print(entry['name'])


def lambda_handler(event, context):
    connect_dynamoDB()
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
