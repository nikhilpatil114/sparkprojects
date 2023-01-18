from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import re
spark = SparkSession.builder.master("local[2]").appName("test").config("spark.sql.session.timeZone", "EST").getOrCreate()
data="E:\\Final_project\\QueenElizabeth_TwitterSentiment\\QueenElizabeth_TwitterSentiment.csv"
df2 = spark.read\
.format("csv")\
.option("header", "true")\
.option("escape","\"")\
.load(data)
#exp=r'[http://]'
#df2.select("tweet").show(truncate=False)

#df3=df2.withColumn("tweet",regexp_replace("tweet",exp,''))
#df4=df3.withColumn("tweet2",regexp_replace('tweet',r'[^a-zA-Z0-9" "]','')).select("tweet","tweet2")
#df4.show(truncate=False)
#df2.show(truncate=False)

df2.printSchema()

import string
english_punctuations = string.punctuation
punctuations_list = english_punctuations
def cleaning_punctuations(text):
    translator = str.maketrans('', '', punctuations_list)
    return text.translate(translator)
uf=udf(cleaning_punctuations)
#spark.udf.register("newfun",uf)
df2.withColumn('tweet2',uf(col('tweet'))).select("tweet","tweet2").show(truncate=False)

'''
#.option("escapeQuotes", "true")\
# import re
import numpy as np
import pandas as pd
#import seaborn as sns
#from wordcloud import WordCloud
#from nltk.stem import WordNetLemmatizer

np.sum(df.isnull().any(axis=1))


df['target'].unique()

df['target'].nunique()


stopwordlist = ['a', 'about', 'above', 'after', 'again', 'ain', 'all', 'am', 'an',
             'and','any','are', 'as', 'at', 'be', 'because', 'been', 'before',
             'being', 'below', 'between','both', 'by', 'can', 'd', 'did', 'do',
             'does', 'doing', 'down', 'during', 'each','few', 'for', 'from',
             'further', 'had', 'has', 'have', 'having', 'he', 'her', 'here',
             'hers', 'herself', 'him', 'himself', 'his', 'how', 'i', 'if', 'in',
             'into','is', 'it', 'its', 'itself', 'just', 'll', 'm', 'ma',
             'me', 'more', 'most','my', 'myself', 'now', 'o', 'of', 'on', 'once',
             'only', 'or', 'other', 'our', 'ours','ourselves', 'out', 'own', 're','s', 'same', 'she', "shes", 'should', "shouldve",'so', 'some', 'such',
             't', 'than', 'that', "thatll", 'the', 'their', 'theirs', 'them',
             'themselves', 'then', 'there', 'these', 'they', 'this', 'those',
             'through', 'to', 'too','under', 'until', 'up', 've', 'very', 'was',
             'we', 'were', 'what', 'when', 'where','which','while', 'who', 'whom',
             'why', 'will', 'with', 'won', 'y', 'you', "youd","youll", "youre",
             "youve", 'your', 'yours', 'yourself', 'yourselves']

#remove stop words
STOPWORDS = set(stopwordlist)
def cleaning_stopwords(text):
    return " ".join([word for word in str(text).split() if word not in STOPWORDS])
#dataset['text'] = dataset['text'].apply(lambda text: cleaning_stopwords(text))
#dataset['text'].head()

# Cleaning and removing punctuations

import string
english_punctuations = string.punctuation
punctuations_list = english_punctuations
def cleaning_punctuations(text):
    translator = str.maketrans('', '', punctuations_list)
    return text.translate(translator)
#dataset['text']= dataset['text'].apply(lambda x: cleaning_punctuations(x))
#dataset['text'].tail()

# Cleaning and removing repeating characters

def cleaning_repeating_char(text):
    return re.sub(r'(.)1+', r'1', text)
#dataset['text'] = dataset['text'].apply(lambda x: cleaning_repeating_char(x))
#dataset['text'].tail()

# Cleaning and removing URL’s

def cleaning_URLs(data):
    return re.sub('((www.[^s]+)|(https?://[^s]+))',' ',data)
#dataset['text'] = dataset['text'].apply(lambda x: cleaning_URLs(x))
#dataset['text'].tail()

# Cleaning and removing Numeric numbers

def cleaning_numbers(data):
    return re.sub('[0-9]+', '', data)
#dataset['text'] = dataset['text'].apply(lambda x: cleaning_numbers(x))
#dataset['text'].tail()

#Getting tokenization of tweet text
from nltk.tokenize import RegexpTokenizer
tokenizer = RegexpTokenizer(r'w+')
#dataset['text'] = dataset['text'].apply(tokenizer.tokenize)
#dataset['text'].head()

#Applying Stemming

import nltk
st = nltk.PorterStemmer()
def stemming_on_text(data):
    text = [st.stem(word) for word in data]
    return data
#dataset['text']= dataset['text'].apply(lambda x: stemming_on_text(x))
#dataset['text'].head()

#Applying Lemmatizer

lm = nltk.WordNetLemmatizer()
def lemmatizer_on_text(data):
    text = [lm.lemmatize(word) for word in data]
    return data
#dataset['text'] = dataset['text'].apply(lambda x: lemmatizer_on_text(x))
#dataset['text'].head()
'''