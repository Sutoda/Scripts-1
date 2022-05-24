# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 08:31:39 2022

@author: ibraham
"""


import glob
import os
import pandas as pd
import pandas.io.sql
import pyodbc
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
import datetime


filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Meier_Impraegnieranlage\Statorplatz 01\_PROZESSDATEN_Gesammelt/*.csv")
filenames.sort(key=os.path.getctime)
Dfs=[]
pd.set_option('display.max_columns', None)
for filename in filenames:
    p = pd.read_csv(filename, sep=";" )
    p['Materialnummer'] = os.path.basename(filename).split(".")[0].split("_")[0]
    p['Datum'] = pd.to_datetime( os.path.basename(filename).split(".")[0].partition("_")[2], format = '%d_%m_%Y')
    Dfs.append(p)
dfs = pd.concat(Dfs, ignore_index=True)
dfs['Solltemperatur_Stromwärme'] = pd.to_numeric(dfs['Solltemperatur_Stromwärme'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Isttemperatur_Stromwärme'] = pd.to_numeric(dfs['Isttemperatur_Stromwärme'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Iststrom_Stromwärme'] = pd.to_numeric(dfs['Iststrom_Stromwärme'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Isttemperatur_Ofen'] = pd.to_numeric(dfs['Isttemperatur_Ofen'] .astype(str).str.replace(',', '.'), downcast='float')
server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
for row in dfs.itertuples():
    try:
      cursor.execute('''INSERT INTO dbo.Impreagnieranlage_Prozessdaten (Solltemperatur_Stromwärme, Isttemperatur_Stromwärme, Iststrom_Stromwärme, Isttemperatur_Ofen, Materialnummer, 
                   Datum)VALUES (?, ?, ?, ?, ?, ?)''', row.Solltemperatur_Stromwärme, row.Isttemperatur_Stromwärme,  row.Iststrom_Stromwärme, 
                                                      row.Isttemperatur_Ofen,  row.Materialnummer,  row.Datum)

      conn.commit()
    except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open(V.Impreagnieranlage_Prozessdaten01 + filename1+ ".txt", "a")
            f.write(str(Argument))
            f.write(f'{row}\n')
            f.close()

conn.close()      
