# -*- coding: utf-8 -*-
"""
Created on Mon Jan 17 15:15:01 2022

@author: ibraham
"""
import glob
import pandas as pd
import pandas.io.sql
import pyodbc
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
import datetime 

filenames = glob.glob("I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Meier_Impraegnieranlage\Datensicherung MMC HMI Panel_08.04.2021\SAVELOG/*.csv")
dfs = []
for filename in filenames:
    p = pd.read_csv(filename, sep=";" ) 
    p['Datum']= pd.to_datetime(p['TimeString']).dt.date
    p['Zeit'] = pd.to_datetime(p['TimeString']).dt.time
    p['VarValue']=p['VarValue'].str.replace(',','.')
    p['Time_ms']=p['Time_ms'].str.replace(',','.')
    p['VarValue'] = pd.to_numeric(p['VarValue'], errors = 'ignore')
    p['Time_ms'] = pd.to_numeric(p['Time_ms'], errors = 'ignore')
    dfs.append(p)
big_frame = pd.concat(dfs, ignore_index=True)

server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()

for row in big_frame.itertuples():
    try:
      cursor.execute('''
                INSERT INTO dbo.Impreagnieranlage1 (Varname,TimeString,VarValue,Validity,Time_ms,Date1,Time1)
                VALUES (?,?,?,?,?,?,?)''',
                row.VarName, row.TimeString, row.VarValue, row.Validity, row.Time_ms, row.Datum, row.Zeit)

      conn.commit()
    except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open(V.Meier_Impraegnieranlage + filename1+ ".txt", "a")
            f.write(str(Argument))
            f.write(f'{row}\n')
            f.close()
conn.close()