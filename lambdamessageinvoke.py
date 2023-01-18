#succes
####################### 1st
import json


def lambda_handler(event, context):
    print("lambda2")
    print(event)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
#########################################  1st

import json
import boto3

# Define the client to interact with AWS Lambda
client = boto3.client('lambda')


def lambda_handler(event, context):
    message = {"message": "hello from lambda1"}
    response = client.invoke(
        FunctionName='arn:aws:lambda:ap-south-1:906454641256:function:toinvoker',
        InvocationType='RequestResponse',
        Payload=json.dumps(message)
    )
    print(response)
    return {
        'body': json.dumps("hello from lambda!")
    }
################################################### 2 nd fuction ####
import json
import uuid


def lambda_handler(event, context):
    # 1 Read the input parameters
    productName = event['ProductName']
    quantity = event['Quantity']
    unitPrice = event['UnitPrice']

    # 2 Generate the Order Transaction ID
    transactionId = str(uuid.uuid1())

    # 3 Implement Business Logic
    amount = quantity * unitPrice

    # 4 Format and return the result
    return {
        'TransactionID': transactionId,
        'ProductName': productName,
        'Amount': amount
    }
####################################  2nd

import json
import boto3

# Define the client to interact with AWS Lambda
client = boto3.client('lambda')


def lambda_handler(event, context):
    # Define the input parameters that will be passed
    # on to the child function
    inputParams = {
        "ProductName": "iPhone SE",
        "Quantity": 2,
        "UnitPrice": 499
    }

    response = client.invoke(
        FunctionName='arn:aws:lambda:ap-south-1:906454641256:function:toinvoker',
        InvocationType='RequestResponse',
        Payload=json.dumps(inputParams)
    )

    responseFromChild = json.load(response['Payload'])

    print('\n')
    print(responseFromChild)