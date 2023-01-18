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


#########################################################################
# No need to include the following snippet into the lambda function
# Only used to test the function locally
event = {
    "ProductName": "iPhone SE",
    "Quantity": 2,
    "UnitPrice": 499
}
response = lambda_handler(event, '')
print(response)
#########################################################################

'''
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
        FunctionName='arn:aws:lambda:eu-west-1:890277245818:function:ChildFunction',
        InvocationType='RequestResponse',
        Payload=json.dumps(inputParams)
    )

    responseFromChild = json.load(response['Payload'])

    print('\n')
    print(responseFromChild)
'''