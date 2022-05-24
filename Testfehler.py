# -*- coding: utf-8 -*-
"""
Created on Tue May 17 09:52:44 2022

@author: ibraham
"""


import glob # das Glob-Modul wird verwendet, um Dateien/Pfadnamen abzurufen, die einem bestimmten Muster entsprechen
import pandas as pd # stellt spezielle Funktionen und Datenstrukturen zur Verfügung für die Manipulation von numerischen Tabell und Zeit-Serien
import pyodbc # vereinfacht den Zugriff auf ODBC-Datenbanken
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
from datetime import datetime # Das Modul stellt Klassen zur Manipulation von Datum und Uhrzeit bereit.


filenames = glob.glob(r"I:\Arbeitsgruppen\Traceability\Sutoda-Ibraham\Neuer Ordner/*.csv")
#filenames ist eine Liste der Dateien in dem oben angegebenen pfad

dfs = []# dfs ist eine Leere Liste
for filename in filenames:
   dfs.append(pd.read_csv(filename))
# alle dateien im in der Liste 'filenames' werden als kleines Data Frame gelesen und in der Liste 'dfs' hinzugefügt.
big_frame = pd.concat(dfs, ignore_index=True)
# Die Inhalte der Liste 'dfs' werden alle in ein großes Data Frame zusammengefasst und neu indiziert.


big_frame.drop(['Date,"Time","Program","Pre-Connection Pulse","Pre-Connection Power","Pre-Connection Pressure","Ramp Time","Pulse Interval","Pulse","Connection Time","Power","Pressure","Mode","Start Delay","Connection Interval","Trimming cut (1:ON - 0:OFF)","End Rotation","Z Axis Length ","Blow Air Time","Voltage Read","Current Calculate","Current Read","Current Delta","Pre-Connection Current Read","Movement Set","Movement Read","Movement Delta","Force Set","Force Read","Force Delta","Pre-Connection Force Read","Connection Number","Cycle Number","Cycle Position","Stator Code / Bar Code","Quality"'], axis = 1, inplace = True)
# Hier wurde eine unnötige Spalte aus big_frame gelöscht. Ein Fehler, der aus CSV- Dateien übernommen wurde, wurde korrigiert.
big_frame.columns = ["Date1","Time1","Program","PreConnectionPulse","PreConnectionPower","PreConnectionPressure","RampTime","PulseInterval","Pulse",
"ConnectionTime","Power1","Pressure","Mode1","StartDelay","ConnectionInterval","Trimmingcut","EndRotation","ZAxisLength","BlowAirTime","VoltageRead",
"CurrentCalculate","CurrentRead","CurrentDelta","PreConnectionCurrentRead","MovementSet","MovementRead","MovementDelta","ForceSet","ForceRead","ForceDelta",
"PreConnectionForceRead","ConnectionNumber","CycleNumber","CyclePosition","StatorCode_BarCode","Quality"]
# Hier wurde noch mal und genau die Spalten benannt, damit bei der Übertragung in der Daten Bank keine Probleme auftreten, SQL Daten Bank 
# akzepitiert nicht Leerzeichen oder - 
big_frame = big_frame.fillna(0)
big_frame['Date1'] = big_frame['Date1'].astype('datetime64[D]').values
big_frame ['Time1']= pd.to_datetime(big_frame['Time1'], errors='raise', dayfirst=False, yearfirst=False, utc=None, 
                                    format= '%H:%M:%S', exact=True, unit=None, infer_datetime_format=True, origin='unix', 
                                    cache=False).dt.time
# alle nan-Werte werden mit null ersetzt, damit bei der Übertragung der Daten in der Daten Bank keine Probleme auftreten
big_frame.iloc[1:7] = 'rosa'
server = 'ltp077' # Severname
db = 'TraceabilityTest' # Name der Daten Bank
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
# eine sichere Verbindung mit der Daten Bank aufbauen 

for row in big_frame.itertuples(): # Durchlaufen alle DataFrame-Zeilen als sogenannte Tupel.
    try: # wrid versucht  alle Zeilen der big_frame in der Tabelle in der richtigen Spalte und in der richtigen reihenfolge zu übertragen
       conn.execute('''INSERT INTO dbo.Schweissanlage(Date1, Time1, Program, PreConnectionPulse, PreConnectionPower,
                  PreConnectionPressure, RampTime, PulseInterval, Pulse, ConnectionTime, Power1, Pressure, Mode1,
                  StartDelay, ConnectionInterval, Trimmingcut, EndRotation, ZAxisLength, BlowAirTime, VoltageRead,
                  CurrentCalculate, CurrentRead, CurrentDelta, PreConnectionCurrentRead, MovementSet, MovementRead,
                  MovementDelta,ForceSet,ForceRead,ForceDelta, PreConnectionForceRead, ConnectionNumber,CycleNumber,
                  CyclePosition, StatorCode_BarCode,Quality)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  row.Date1, row.Time1, row.Program, row.PreConnectionPulse, row.PreConnectionPower, row.PreConnectionPressure,
                  row.RampTime, row.PulseInterval, row.Pulse, row.ConnectionTime, row.Power1, row.Pressure, row.Mode1, row.StartDelay,
                  row.ConnectionInterval, row.Trimmingcut, row.EndRotation, row.ZAxisLength, row.BlowAirTime, row.VoltageRead,
                  row.CurrentCalculate, row.CurrentRead, row.CurrentDelta, row.PreConnectionCurrentRead, row.MovementSet,
                  row.MovementRead, row.MovementDelta, row.ForceSet, row.ForceRead, row.ForceDelta, row.PreConnectionForceRead,
                  row.ConnectionNumber, row.CycleNumber, row.CyclePosition, row.StatorCode_BarCode, row.Quality) 
       conn.commit() # commit wird verwendet, um die Datenbank anzuweisen, alle Änderungen in der aktuellen Transaktion zu speichern.
    except Exception as Argument: # Die Exception hat ein Argument, der zusätzliche Informationen über das Problem liefert
          filename1 = datetime.now().strftime("_%Y_%m_%d-%H_%M_%S") # filename1 erfasst das aktuelle Datum und die Uhrzeit 
          f = open(V.CASTECH_Schweissanlage + filename1+ ".txt", "a") # f ist der Pfad von einer Textdatei
          f.write(str(Argument)) # in f wird die Uhrsache des Exceptions (das Argument) geschrieben
          f.write(f'{row}\n')# in f wird die Zeile die das Exception verursacht geschrieben
          f.close()# f wird geschlossen

conn.close()# die Verbindung mit der Datenbank wird geschlossen