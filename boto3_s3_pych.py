#LISTING S3 BUCKETS
import boto3
client=boto3.client('s3')
list_buck=client.list_buckets()
for buck in list_buck['Buckets']:
    print(buck['Name'])

client = boto3.client('s3')
list_bucket=client.list_buckets()
for bucket in list_bucket['Buckets']:
   print(bucket['Name'])

#CERATE S3 BUCKET IN AWS
import boto3
s3 = boto3.resource('s3')
bucket = s3.create_bucket(Bucket='jayesh2432',CreateBucketConfiguration={'LocationConstraint': 'ap-south-1'})
print (bucket)
'''
#FILE UPLOAD AND DELETE
import boto3
s3 = boto3.resource('s3')
bucket_name="jayesh28374"
fileupload= s3.Bucket(bucket_name).upload_file('E:\\bigdatafile\\IBM.csv', 'ibm.csv')
print(fileupload)

#s3.Object(bucket_name,'README.md').delete()

#DELETE S3 BUCKET
import boto3
client = boto3.client('s3')
bucket_name="jayesh28374"
response = client.delete_bucket(Bucket=bucket_name)
'''
'''
#READ FROM S3 AND CREATE DATAFRAME USING PANDAS
import pandas as pd
import boto3

bucket = "jayesh28374"
file_name = "ibm.csv"

s3 = boto3.client('s3')
# 's3' is a key word. create connection to S3 using default config and all buckets within S3

obj = s3.get_object(Bucket= bucket, Key= file_name)
# get object and file (key) from bucket

initial_df = pd.read_csv(obj['Body'])# 'Body' is a key word
print(initial_df)

'''
'''
import re
#list the keys from perticular bucket
s3 = boto3.resource('s3')
bucket=s3.Bucket("nik114")
lst=list(bucket.objects.all())
#print(lst)
lst1=[]
for i in lst:
     lst1.append(i.key)        #get the folder and file from bucket
print(lst1)  #print(i.bucket_name) #Get bucket name
lst2=[]
for i in lst1:
   j=i.split('/')
   lst2.append(j)
print(lst2)
lst3=[]
for i in lst2:
    if len(i) == 1:
        lst3.append(i[0])
    elif len(i) == 2:
        lst3.append(i[1])
    elif len(i) ==3:
        lst3.append(i[2])
print(lst3)
files=[]
for i in lst3:
    if i != '':
        files.append(i)
print(files)
#extract csv file from the list
for i in files:
     m=re.search("\.csv$",i)
     if m:
         print(i)


for file in lst:
         client.download_file('nik114',file.key,file.key)

'''