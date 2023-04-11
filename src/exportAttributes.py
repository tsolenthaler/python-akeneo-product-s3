from os import getenv
import json
import sys
from akeneo import akeneo
from dotenv import load_dotenv, find_dotenv
from s3client import dictToS3, updateObject

load_dotenv(find_dotenv())

## Load from Job Environment Variables
AKENEO_HOST = getenv('AKENEO_HOST')
AKENEO_CLIENT_ID = getenv('AKENEO_CLIENT_ID')
AKENEO_CLIENT_SECRET = getenv('AKENEO_CLIENT_SECRET')
AKENEO_USERNAME = getenv('AKENEO_USERNAME')
AKENEO_PASSWORD = getenv('AKENEO_PASSWORD')
AKENEO_GET_PRODUCT_QUERY = getenv('AKENEO_GET_PRODUCT_QUERY')
S3_ENDPOINT = getenv('S3_ENDPOINT')
S3_BUCKET = getenv('S3_BUCKET')
S3_REGION = getenv('S3_REGION')
S3_ACCESS_KEY = getenv('S3_ACCESS_KEY')
S3_OBJECT_CONFIG_ATTRIBUTES_PATH = getenv('S3_OBJECT_CONFIG_ATTRIBUTES_PATH')
S3_OBJECT_CONFIG_ATTRIBUTES_INDEX = getenv('S3_OBJECT_CONFIG_ATTRIBUTES_INDEX')

## Exract Data from Akeneo
def getAttributesFromAkeneo():
    client = akeneo.Akeneo(AKENEO_HOST, AKENEO_CLIENT_ID, AKENEO_CLIENT_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)
    #product = client.getProductByCode(AKENEO_GET_PRODUCT_QUERY)
    attriebutes = client.getAttributes()
    return attriebutes

def createAttribute(attriebutes):
    print("Create Attribute")
    for attribute in attriebutes:
        attribute.pop('_links')
        dictToS3(attribute, S3_BUCKET, S3_OBJECT_CONFIG_ATTRIBUTES_PATH+attribute['code']+".json")

def __main__():
    print("Start Export")
    print(f"Arguments count: {len(sys.argv)}")
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i:>6}: {arg}")
    # Extract
    attriebutes = getAttributesFromAkeneo()
    # Transform
    # Remove _links
    print(attriebutes)
    
    # Load
    dictToS3(attriebutes, S3_BUCKET, S3_OBJECT_CONFIG_ATTRIBUTES_INDEX)
    createAttribute(attriebutes)
    print("Export Done")

if __name__== "__main__":
    __main__()