#success

def lambda_handler(event,context):
    print("invoke")
    params = event['list']
    return {"params" : params + ["abc"]}

import boto3
import json
def lambda_handler(event,context):
   lambda_client = boto3.client('lambda')
   a=[1,2,3]
   x = {"list" : a}
   response = lambda_client.invoke(FunctionName="functionA",
                                       InvocationType='RequestResponse',
                                       Payload=json.dumps(x))
   print (response['Payload'].read())