### create df
import pandas as pd
print(pd.__version__)

mydataset = {
           'cars': ["BMW", "Volvo", "Ford"],
           'passings': [3, 7, 2]
            }
myvar = pd.DataFrame(mydataset)
print(myvar)


##########
a = [1, 7, 2]

myvar = pd.Series(a)

print(myvar)

#################3

a = [1, 7, 2]

myvar = pd.Series(a, index = ["x", "y", "z"])

print(myvar)

################

calories = {"day1": 420, "day2": 380, "day3": 390}

myvar = pd.Series(calories)

print(myvar)

##################33
data = {
  "calories": [420, 380, 390],
  "duration": [50, 40, 45]
}

myvar = pd.DataFrame(data)

print(myvar)
#############
import pandas as pd
try:
  data = {
    "calories": [420, 380, 390],
    "duration": [50, 40, 45]
  }

  # load data into a DataFrame object:
  df = pd.DataFrame(data)

  print(df)
  print(df.loc[0])
  print(df.loc[[0, 1]])

  ####################
  data = {
    "calories": [420, 380, 390],
    "duration": [50, 40, 45]
  }

  df = pd.DataFrame(data, index=["day1", "day2", "day3"])

  print(df)
  print(df.loc["day2"])

  ###   Load a CSV file into a Pandas DataFrame:

  df = pd.read_csv('E:\\bigdatafile\\donation.csv')
  print(df.to_string())
  print(df)
  print(pd.options.display.max_rows)  ## pd.options.display.max_rows=1000
  ###
  import pandas as pd

  # df = pd.read_json('E:\\bigdatafile\\cars.json')
  # print(df.to_string())
  ###########
  data = {
    "Duration": {
      "0": 60,
      "1": 60,
      "2": 60,
      "3": 45,
      "4": 45,
      "5": 60
    },
    "Pulse": {
      "0": 110,
      "1": 117,
      "2": 103,
      "3": 109,
      "4": 117,
      "5": 102
    },
    "Maxpulse": {
      "0": 130,
      "1": 145,
      "2": 135,
      "3": 175,
      "4": 148,
      "5": 127
    },
    "Calories": {
      "0": 409,
      "1": 479,
      "2": 340,
      "3": 282,
      "4": 406,
      "5": 300
    }
  }

  df = pd.DataFrame(data)

  print(df)
  ######################### create change in new dataframe
  df = pd.read_csv('E:\\bigdatafile\\corona.csv')

  new_df = df.dropna()

  #print(new_df.to_string())
  ######################### change in original dataframe use inplace true
  # df = pd.read_csv('E:\\bigdatafile\\corona.csv')
  df.dropna(inplace=True)
  #print(df.to_string())

  ############################### replace with value
  # df = pd.read_csv('data.csv')
  df.fillna(130, inplace=True)
 # df["Calories"].fillna(130, inplace=True)  # for perticular column
  ######################## replace with mean,median,mode
  #x = df["Calories"].mean()
  #x = df["Calories"].median()
  #x = df["Calories"].mode()[0]
 # df["Calories"].fillna(x, inplace=True)
  ##################### date format
  #df['Date'] = pd.to_datetime(df['Date'])
  #print(df.to_string())
  ########################  removing row
  #df.dropna(subset=['Date'], inplace=True)

  ######Delete rows where "Duration" is higher than 120:
  #for x in df.index:
    #if df.loc[x, "Duration"] > 120:
     # df.drop(x, inplace=True)
  ##################################  remove duplicate
  #print(df.duplicated())
  #df.drop_duplicates(inplace=True)

  #########
  print(df.head(20))
except Exception as e:
  print(e)



