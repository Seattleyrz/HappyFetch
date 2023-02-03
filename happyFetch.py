import json
from numpy import negative
import numpy as np
import pandas as pd
def happyFetch(points_spent, path):
  df = pd.read_csv(path)
  df = df.sort_values(by = 'timestamp', ignore_index = True)
  total_points = -abs(points_spent)
  negative_sum = 0
#   for loop to get all negative values and sum them up
  for key, value in df.get('points').iteritems():
    if(value < 0):
      negative_sum += value
      df.at[key, 'points'] = 0
#   for loop to balance points according to the order of timestamp
  for key, value in df.get('points').iteritems():
    if(value < abs(negative_sum)):
      df.at[key, 'points'] = 0
      negative_sum += value
    else:
      df.at[key, 'points'] += negative_sum
      negative_sum = 0
  
  
  df = df.sort_values(by = 'timestamp', ignore_index = True)
  points_sum = sum(df.get('points'))
#   edge case: if payers are running out of points
  if(points_sum < points_spent):
      return("Running out of points")
#   for loop to use up all the points given
  for key, value in df.get('points').iteritems():
    if(value != 0):
      if(value < abs(total_points)):
        df.at[key, 'points'] = 0
        total_points += value
      else:
        df.at[key, 'points'] += total_points
        total_points = 0
  
  df = df.sort_values(by = 'timestamp', ignore_index = True)
  df.drop("timestamp", axis = 1, inplace = True)
  df_new = df.groupby(df['payer']).aggregate({'points': 'sum'})

  
  output = json.dumps(df_new.to_dict()['points'], indent = 4) 
  print(output)
  return output

happyFetch(5000, "transactions.csv")
