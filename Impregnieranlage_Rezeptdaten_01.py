# -*- coding: utf-8 -*-
"""
Created on Wed Mar 30 08:26:34 2022

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

filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Meier_Impraegnieranlage\Statorplatz 01\_REZEPTDATEN_Gesammelt/*.csv")
filenames.sort(key=os.path.getctime)
Dfs=[]

for filename in filenames:
    p = pd.read_csv(filename, sep=";" )
    p['Materialnummer'] = os.path.basename(filename).split(".")[0].split("_")[0]
    p['Zeitstempel'] = os.path.basename(filename).split(".")[0].partition("_")[2]
    
    Dfs.append(p)
dfs = pd.concat(Dfs, ignore_index=True)
dfs.columns = ['Anlagentyp', 'Anlagennummer', 'Statorplatz', 'Rezepturdatensatz', 'Querschnitt', 'Kaltwiderstand',
    'Toleranz_Kaltwiderstand', 'Stromdichte_Vorwärmen1', 'Temperatur_Vorwärmen1', 'Sollzeit_Vorwärmen1',  
    'Stromdichte_Vorwärmen2', 'Temperatur_Vorwärmen2','Sollzeit_Vorwärmen2', 'Stromdichte_Vorwärmen3',  
    'Temperatur_Vorwärmen3', 'Sollzeit_Vorwärmen3', 'Stromdichte_Heizen_unter_Harz1', 'Temperatur_Heizen_unter_Harz1',  
    'Sollzeit_Tränken1', 'Stromdichte_Heizen_unter_Harz2', 'Temperatur_Heizen_unter_Harz2', 'Sollzeit_Tränken2',  
    'Stromdichte_Abtropfen', 'Temperatur_Abtropfen', 'Sollzeit_Abtropfen', 'Stromdichte_Härten1', 'Temperatur_Härten1',  
    'Sollzeit_Härten1', 'Stromdichte_Härten2', 'Temperatur_Härten2', 'Sollzeit_Härten2', 'Be_Entladeposition', 'Kontaktierposition',
    'Eintauchposition', 'Vorwärmposition', 'Austachposition', 'Eintauchgeschwindigkeit', 'Austauchgeschwindigkeit',
    'Position_Absaugkabine', 'Verfahrgeschwindigkeit_allgemein', 'Ofenzeit', 'Materialnummer', 'Zeitstempel']
     
dfs['Querschnitt'] = pd.to_numeric(dfs['Querschnitt'] .astype(str).str.replace(',', '.'), downcast='float')   
dfs['Kaltwiderstand'] = pd.to_numeric(dfs['Kaltwiderstand'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Toleranz_Kaltwiderstand'] = pd.to_numeric(dfs['Toleranz_Kaltwiderstand'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Stromdichte_Vorwärmen1'] = pd.to_numeric(dfs['Stromdichte_Vorwärmen1'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Temperatur_Vorwärmen1'] = pd.to_numeric(dfs['Temperatur_Vorwärmen1'] .astype(str).str.replace(',', '.'), downcast='float')  
dfs['Stromdichte_Vorwärmen2'] = pd.to_numeric(dfs['Stromdichte_Vorwärmen2'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Temperatur_Vorwärmen2'] = pd.to_numeric(dfs['Temperatur_Vorwärmen2'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Stromdichte_Vorwärmen3'] = pd.to_numeric(dfs['Stromdichte_Vorwärmen3'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Temperatur_Vorwärmen3'] = pd.to_numeric(dfs['Temperatur_Vorwärmen3'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Stromdichte_Heizen_unter_Harz1'] = pd.to_numeric(dfs['Stromdichte_Heizen_unter_Harz1'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Temperatur_Heizen_unter_Harz1'] = pd.to_numeric(dfs['Temperatur_Heizen_unter_Harz1'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Stromdichte_Heizen_unter_Harz2'] = pd.to_numeric(dfs['Stromdichte_Heizen_unter_Harz2'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Temperatur_Heizen_unter_Harz2'] = pd.to_numeric(dfs['Temperatur_Heizen_unter_Harz2'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Stromdichte_Abtropfen'] = pd.to_numeric(dfs['Stromdichte_Abtropfen'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Temperatur_Abtropfen'] = pd.to_numeric(dfs['Temperatur_Abtropfen'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Stromdichte_Härten1'] = pd.to_numeric(dfs['Stromdichte_Härten1'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Temperatur_Härten1'] = pd.to_numeric(dfs['Temperatur_Härten1'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Stromdichte_Härten2'] = pd.to_numeric(dfs['Stromdichte_Härten2'] .astype(str).str.replace(',', '.'), downcast='float')
dfs['Temperatur_Härten2'] = pd.to_numeric(dfs['Temperatur_Härten2'] .astype(str).str.replace(',', '.'), downcast='float') 
dfs['Zeitstempel'] = pd.to_datetime(dfs['Zeitstempel'].astype(str).str.replace('_', '-'),format='%d-%m-%Y')
server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
for row in dfs.itertuples():
    try:
     cursor.execute('''
                INSERT INTO dbo.Impreagnieranlage_Rezeptdaten (Anlagentyp, Anlagennummer, Statorplatz, Rezepturdatensatz, Querschnitt, Kaltwiderstand,
    Toleranz_Kaltwiderstand, Stromdichte_Vorwärmen1, Temperatur_Vorwärmen1, Sollzeit_Vorwärmen1,  
    Stromdichte_Vorwärmen2, Temperatur_Vorwärmen2,Sollzeit_Vorwärmen2, Stromdichte_Vorwärmen3,  
    Temperatur_Vorwärmen3, Sollzeit_Vorwärmen3, Stromdichte_Heizen_unter_Harz1, Temperatur_Heizen_unter_Harz1,  
    Sollzeit_Tränken1, Stromdichte_Heizen_unter_Harz2, Temperatur_Heizen_unter_Harz2, Sollzeit_Tränken2,  
    Stromdichte_Abtropfen, Temperatur_Abtropfen, Sollzeit_Abtropfen, Stromdichte_Härten1, Temperatur_Härten1,  
    Sollzeit_Härten1, Stromdichte_Härten2, Temperatur_Härten2, Sollzeit_Härten2, Be_Entladeposition, Kontaktierposition,
    Eintauchposition, Vorwärmposition, Austachposition, Eintauchgeschwindigkeit, Austauchgeschwindigkeit,
    Position_Absaugkabine, Verfahrgeschwindigkeit_allgemein, Ofenzeit, Materialnummer, Zeitstempel)VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 
                                                          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                 row.Anlagentyp, row.Anlagennummer, row.Statorplatz, row.Rezepturdatensatz, row.Querschnitt, row.Kaltwiderstand, row.Toleranz_Kaltwiderstand, 
                 row.Stromdichte_Vorwärmen1, row.Temperatur_Vorwärmen1, row.Sollzeit_Vorwärmen1, row.Stromdichte_Vorwärmen2, row.Temperatur_Vorwärmen2, row.Sollzeit_Vorwärmen2, 
                 row.Stromdichte_Vorwärmen3, row.Temperatur_Vorwärmen3, row.Sollzeit_Vorwärmen3, row.Stromdichte_Heizen_unter_Harz1, row.Temperatur_Heizen_unter_Harz1, 
                 row.Sollzeit_Tränken1, row.Stromdichte_Heizen_unter_Harz2, row.Temperatur_Heizen_unter_Harz2, row.Sollzeit_Tränken2, row.Stromdichte_Abtropfen, 
                 row.Temperatur_Abtropfen, row.Sollzeit_Abtropfen, row.Stromdichte_Härten1, row.Temperatur_Härten1, row.Sollzeit_Härten1, row.Stromdichte_Härten2,
                 row.Temperatur_Härten2, row.Sollzeit_Härten2, row.Be_Entladeposition, row.Kontaktierposition, row.Eintauchposition, row.Vorwärmposition, row.Austachposition, 
                 row.Eintauchgeschwindigkeit, row.Austauchgeschwindigkeit, row.Position_Absaugkabine, row.Verfahrgeschwindigkeit_allgemein, row.Ofenzeit, 
                 row.Materialnummer, row.Zeitstempel )

     conn.commit()
    except Exception as Argument:
            filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
            f = open(V.Impreagnieranlage_Rezeptdaten_01 + filename1+ ".txt", "a")
            f.write(str(Argument))
            f.write(f'{row}\n')
            f.close()

conn.close()      
  