# -*- coding: utf-8 -*-
"""
Created on Wed Feb 16 08:42:44 2022

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

filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Vision_Control/Messdaten/*.csv")

Dfs=[]

for filename in filenames:
    p = pd.read_csv(filename, sep='\n', header=None)[0].str.split(';', expand=True)# erstmal  wird die Zeile gelesen dann wird die Zeile nach Semikolon getrennt
    DIR = os.path.dirname(filename).split("/")[0] # Gibt das erste Teil des Verzeichnis von filename zurück, gesplitet nach /, wieder das erste Teil.
    FILE = os.path.basename(filename).split(".")[0]# Gibt das letzte Teil des Verzeichnis von filename zurück, gesplitet nach ., wieder das erste Teil
    w = []; x = []; y = []; z = []; A = []# eine Menge aus leeren Listen
    
    for i in range(len(p)): # für die Länge des Dataframes p werden die Elemente des Headers aufgefasst 
        w.append(p.iloc[0][0])# Z.B in die Liste w wird zeile 0, Spalte 0 aufgefasst, w hat die Länge von p
        x.append(p.iloc[0][1])
        y.append(p.iloc[0][2])
        z.append(p.iloc[1][0])
        A.append(DIR+ "\Bilder\\" +FILE)# in A wird das Verzeichnis der Bilder aufgefasst
     
   
   
    
    # Die oben erstellten Listen bilden ein Paar Spalten des Dataframes 
    p['Materialnummer'] = x
    p['Serialnummer'] = y
    p['Uhrzeit'] = w
    p['Bearbeiter'] = z
    p['Bild']= A
    
    Dfs.append(p)  # p's werden zu einer Liste aus Dataframes hinzugefügt       
df = pd.concat(Dfs, ignore_index=True) # aus der Liste der Dataframes wird dann ein großes Dataframe

df.columns = ['Id','Winkel','Status1', 'Materialnummer', 'Serialnummer', 'Uhrzeit', 'Bearbeiter', 'Bild','Fehlermerkmal', 'Ueberhang','PosX','PosY' ]
df.loc[df['Id'] == 'ResultsSideA', 'ResultsSide'] = 'A' # in der Zeile wo die Spalte Id gleich ResultsSideA ist wird in die Spalte ResultsSide A eingetragen
df.loc[df['Id'] == 'ResultsSideB', 'ResultsSide'] = 'B' # das gleiche für B
df['ResultsSide'] = df['ResultsSide'].fillna(0) # die restlichen Zeilen der Spalte ResultsSide werden mit null ausgfüllt

for i in df.index: # alle Zeilen des großenDataframes werden duchgegangen
    if i > 0:
     if df['ResultsSide'][i] == 0: # wenn in einer Zeile die Spalte ResultsSide gleich null ist,
        df['ResultsSide'][i] = df['ResultsSide'][i-1] # wird der Eintrag von oberen Zeile dafür übernommen
     else:
         df['ResultsSide'][i] = df['ResultsSide'][i] 
        
df.loc[df['ResultsSide'] == 'A', 'Bild'] = df['Bild']+'_A.jpg'
# wo die Spalte ResultsSide gleich A ist, wird in die Spalte Bild dem Verzeichnis '_A.jpg' hinzugefügt
df.loc[df['ResultsSide'] == 'B', 'Bild'] = df['Bild']+'_B.jpg'  

df['PosY'] = df['PosY'].fillna(0)# die leeren Zellen werden mit nullen ausgefüllt
#um die unnötige Zeilen zu eleminieren werden die Indexe der Zeilen in Variablen festgehalten

index0 = df[ df['PosY'] == 0].index # index0 ist die Liste aller Zeilen-indexes, wo die Spalte PosY den Wert null hat
# Somit werden alle Zeilen erfasst, die deren Inhalt zu Spalten des Dataframes geworden sind
df.drop(index0 , inplace=True) #  Alle Zeilen in der Liste werden gelöscht ()
index1 = df[ df['PosY'] == 'PosY'].index # Somit werden alle Zeilen erfass, die die Spalten bezeichnen
df.drop(index1 , inplace=True)

# Elemente die numerisch sein sollten sind aber als string gespeichert,  werden in float umgewandelt
df['Winkel'] = pd.to_numeric(df['Winkel'], downcast='float')
df['Ueberhang'] = pd.to_numeric(df['Ueberhang'], downcast='float')
df['PosX'] = pd.to_numeric(df['PosX'], downcast='float')
df['PosY'] = pd.to_numeric(df['PosY'], downcast='float')
df['Uhrzeit'] = pd.to_datetime(df['Uhrzeit'])

server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
for row in df.itertuples():
    try:
        cursor.execute('''
                INSERT INTO dbo.Vision_Controll (Id, Winkel, Status1,Materialnummer, Serialnummer, 
                                                Uhrzeit, Bearbeiter, Bild, Fehlermerkmal, Ueberhang, PosX, PosY, ResultsSide)VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)''',
                   row.Id, row.Winkel, row.Status1, row.Materialnummer, row.Serialnummer, row.Uhrzeit, row.Bearbeiter, 
                   row.Bild,row.Fehlermerkmal, row.Ueberhang, row.PosX, row.PosY, row.ResultsSide)

        conn.commit()
    except Exception as Argument:
              filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
              f = open(V.Vision_Control + filename1+ ".txt", "a")
              f.write(str(Argument))
              f.write(f'{row}\n')
              f.close()
    
conn.close()
