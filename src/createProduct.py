from os import getenv
from dotenv import load_dotenv, find_dotenv
from s3client import dictToS3, updateObject, getObject

load_dotenv(find_dotenv())

## Load from Job Environment Variables
S3_ENDPOINT = getenv('S3_ENDPOINT')
S3_BUCKET = getenv('S3_BUCKET')
S3_REGION = getenv('S3_REGION')
S3_ACCESS_KEY = getenv('S3_ACCESS_KEY')
S3_SECRET_ACCESS_KEY = getenv('S3_SECRET_ACCESS_KEY')
S3_OBJECT_PRODUCTS_INDEX = getenv('S3_OBJECT_PRODUCTS_INDEX')
S3_OBJECT_PRODUCT_PATH = getenv('S3_OBJECT_PRODUCT_PATH')

def getIndex():
    print("Get Index")
    products = getObject(S3_BUCKET, S3_OBJECT_PRODUCTS_INDEX)
    return products

def createProduct(products):
    print("Create Product")
    product = {}
    for product in products:
        product[product['identifier']] = product
        dictToS3(product, S3_BUCKET, S3_OBJECT_PRODUCT_PATH+"/"+product['identifier']+"id.json")

def createIdIndex(products):
    print("Create Index")
    index = {}
    for product in products:
        index[product['identifier']] = product
    return index

def __main__():
    print("Start Create Index")
    products = getIndex()
    index = createProduct(products)

if __name__== "__main__":
    __main__()