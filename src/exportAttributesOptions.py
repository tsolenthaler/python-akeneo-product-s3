from os import getenv
import json
import sys
from akeneo import akeneo
from dotenv import load_dotenv, find_dotenv
from s3client import dictToS3, updateObject, getObject

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


## Exract Data from S3 Storage
def getAttributesFromStorage():
    # fetch attributes from S3
    attributes = getObject(S3_BUCKET, S3_OBJECT_CONFIG_ATTRIBUTES_INDEX)
    return attributes

def getAttributesOptionsFromAkeneo(attributeCode):
    client = akeneo.Akeneo(AKENEO_HOST, AKENEO_CLIENT_ID, AKENEO_CLIENT_SECRET, AKENEO_USERNAME, AKENEO_PASSWORD)
    #product = client.getProductByCode(AKENEO_GET_PRODUCT_QUERY)
    attriebutes = client.getAttributOptions(attributeCode)
    return attriebutes

def checkAttributeTypSelection(attriebutes):
    attriebutesWithSelection = []
    for attribute in attriebutes:
        if attribute['type'] == 'pim_catalog_simpleselect' or attribute['type'] == 'pim_catalog_multiselect':
            print(attribute['code'])
            attriebutesWithSelection.append(attribute)

    return attriebutesWithSelection

def createAttributeOptionsIndex(attributes):
    print("Create Attribute")
    for attribute in attributes:
        attribute.pop('_links')
        options = getAttributesOptionsFromAkeneo(attribute['code'])
        dictToS3(options, S3_BUCKET, S3_OBJECT_CONFIG_ATTRIBUTES_PATH+attribute['code']+"/options.json")
        print(options)
        createAttributeOptions(attribute['code'], options)

def createAttributeOptions(attributCode, attributOptions):
    print("Create Attribute Options")
    for attributeOption in attributOptions:
        attributeOption.pop('_links')
        dictToS3(attributeOption, S3_BUCKET, S3_OBJECT_CONFIG_ATTRIBUTES_PATH+attributCode+"/options/"+attributeOption['code']+".json")

def __main__():
    print("Start Export")
    print(f"Arguments count: {len(sys.argv)}")
    for i, arg in enumerate(sys.argv):
        print(f"Argument {i:>6}: {arg}")
    # Extract
    attributes = getAttributesFromStorage()
    attributewithSeletion = checkAttributeTypSelection(attributes)
    # Transform
    
    # Load
    #dictToS3(attriebutes, S3_BUCKET, S3_OBJECT_CONFIG_ATTRIBUTES_INDEX)
    createAttributeOptionsIndex(attributewithSeletion)
    print("Export Done")

if __name__== "__main__":
    __main__()