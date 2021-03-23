import sys
import os.path
import pandas as pd
from pathlib import Path    

def read_files(filepath):
  tp = pd.read_csv( filepath, iterator=True, chunksize=10000)  # reading the files for every 10000 rows using a iterator 
  df = pd.concat(tp, ignore_index=True)  
  return df

def add_column(df, nameOfFile):
    length_df = len(df)
    column = [nameOfFile]*length_df
    df['filename'] = column
    return df

def save_File(df):
  df.to_csv( 'trail1.csv', index = False , chunksize=10000, header = True)  

def main():
    #print ('Number of arguments:', len(sys.argv), 'arguments.')
    #print ('Argument List:', str(sys.argv))
    values = list(sys.argv)
    #print(values)
    final_df = pd.DataFrame()
    for i in range(1,len(values)):
        try:
            df = read_files(values[i])
            nameOfFile = Path(values[i]).name
            result = add_column(df, nameOfFile)
            #print(result['filename'].str[-3:])
            final_df = pd.concat([final_df,result])
        except Exception as e:
            print(e)
    pd.set_option('display.max_rows', None)
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', None)
    pd.set_option('display.max_colwidth', None)
    #save_File(final_df)
    print(final_df.to_string(index=False))
    return (final_df)



a = main()
