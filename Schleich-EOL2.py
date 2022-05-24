
import glob
import os
import pandas as pd
import pandas.io.sql
import pyodbc
import numpy as np
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
import datetime




filenames = glob.glob("I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Schleich-EOL\CSV/*.csv")
dfs=[]

for filename in filenames:
    
    p = pd.read_csv(filename, sep='\n', encoding='ISO 8859-1',  engine='python', header=None)[0].str.split(';', expand=True)
    
    Spalten = [ 'Text1', 'Ist', 'Soll',	'Minimum',	'Maximum',	'Einheit',	'IO_NIO', 'Testplan', 'SerNr', '']
    x = []
    y = []
    t = os.path.basename(filename).split("_")[1]
    while len(x)<len(p):
       x.append(os.path.basename(filename).split("_")[0])
       y.append(t.split(".")[0])

    df = pd.DataFrame(p)
    
    df['Testplan'] = x
    df['SerNr'] = y
    
    dfs.append(df)


df = pd.concat(dfs, ignore_index=True)
df.columns = Spalten


df.loc[df['Text1'] == 'Date', 'Date1'] =  df['Ist'] 
df.loc[df['Text1'] == 'Time', 'Time1'] =  df['Ist'] 
df.loc[df['Text1'] == 'Result', 'Result'] =  df['Ist']
df.loc[df['Text1'] == 'Inspector', 'Inspector'] =  df['Ist']
df.loc[df['Text1'] == 'Order number', 'OrderNumber'] =  df['Ist']
df['Date1'] = df['Date1'].fillna(0)
df['Time1'] = df['Time1'].fillna(0)
df['Result'] = df['Result'].fillna(0)
df['Inspector'] = df['Inspector'].fillna(0)
df['OrderNumber'] = df['OrderNumber'].fillna(0)

for i in df.index:
   if i > 0:
     if df['Date1'][i] == 0:
        df['Date1'][i] = df['Date1'][i-1]
     if df['Time1'][i] == 0:
        df['Time1'][i] = df['Time1'][i-1]
     if df['Result'][i] == 0:
        df['Result'][i] = df['Result'][i-1]
     if df['Inspector'][i] == 0:
        df['Inspector'][i] = df['Inspector'][i-1]
     if df['OrderNumber'][i] == 0:
        df['OrderNumber'][i] = df['OrderNumber'][i-1]
        
index1 = df[ df['Text1'] == '[Text]' ].index
index2 = df[ df['Text1'] == '[SCHRITT]' ].index

df.drop(index1 , inplace=True)
df.drop(index2 , inplace=True)

df['Minimum'].replace('', np.nan, inplace=True)
df.dropna(subset=['Minimum'], inplace=True)
df['L0'] = df['Ist'].str.split('=').str[0]
df['L_1'] = df['Ist'].str.split('=').str[1]
df['L_1'] = df['L_1'].str.split(' ').str[0]
df['L_1'] = df['L_1'].fillna(0) 
df['L_2'] = df['Ist'].str.split('=').str[2] 
df['L_2'] = df['L_2'].str.split(' ').str[0]
df['L_2'] = df['L_2'].fillna(0) 
df['L_3'] = df['Ist'].str.split('=').str[3]    
df['L_3'] = df['L_3'].str.split('@').str[0]
df['L_3'] = df['L_3'].fillna(0)
df['Ist_'] = df['Ist'].str.split('@').str[2] 
df['Ist_'] = df['Ist_'].fillna(0) 
df['Ist'] = df['Ist'].str.split('@').str[0] 
for i in df.index:
    if df['Ist_'][i] == 0:
       df['Ist_'][i] = df['Ist'][i]
    if df['L0'][i] == 'L1':
       df['Ist_'][i] = 0
del df['']
del df['L0']
del df['Ist']
df['Ist'] = df['Ist_'].map(lambda x: str(x).rstrip(' Â°C '))
del df['Ist_']

print(len(df))  
server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()

for row in df.itertuples():
    try:
      cursor.execute('''
                  INSERT INTO dbo.Schleich_EOL(
                  Text1,  Soll, Minimum, Maximum, Einheit, IO_NIO, Testplan, SerNr, Date1, Time1, Result, Inspector, OrderNumber, L_1, L_2, L_3, Ist)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  row.Text1, 
                  row.Soll, 
                  row.Minimum, 
                  row.Maximum, 
                  row.Einheit,
                  row.IO_NIO,
                  row.Testplan,
                  row.SerNr,
                  row.Date1,
                  row.Time1,
                  row.Result,
                  row.Inspector,
                  row.OrderNumber,
                  row.L_1,
                  row.L_2,
                  row.L_3,
                  row.Ist)
      conn.commit()
    except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open(V.Schleich_EOL + filename1+ ".txt", "a")
            f.write(str(Argument))
            f.write(f'{row}\n')
            f.close()
conn.close()     