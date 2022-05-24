# -*- coding: utf-8 -*-
"""
Created on Tue Feb  8 11:05:43 2022

@author: ibraham
"""

import glob
import pandas as pd
import pyodbc
import datetime
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V


filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\CASTECH_Schweissanlage\Messdaten/*.csv")

dfs = []
for filename in filenames:
   dfs.append(pd.read_csv(filename))
big_frame = pd.concat(dfs, ignore_index=True)

big_frame.drop(['Date,"Time","Program","Pre-Connection Pulse","Pre-Connection Power","Pre-Connection Pressure","Ramp Time","Pulse Interval","Pulse","Connection Time","Power","Pressure","Mode","Start Delay","Connection Interval","Trimming cut (1:ON - 0:OFF)","End Rotation","Z Axis Length ","Blow Air Time","Voltage Read","Current Calculate","Current Read","Current Delta","Pre-Connection Current Read","Movement Set","Movement Read","Movement Delta","Force Set","Force Read","Force Delta","Pre-Connection Force Read","Connection Number","Cycle Number","Cycle Position","Stator Code / Bar Code","Quality"'], axis = 1, inplace = True)
big_frame.columns = ["Date1","Time1","Program","PreConnectionPulse","PreConnectionPower","PreConnectionPressure","RampTime","PulseInterval","Pulse",
"ConnectionTime","Power1","Pressure","Mode1","StartDelay","ConnectionInterval","Trimmingcut","EndRotation","ZAxisLength","BlowAirTime","VoltageRead",
"CurrentCalculate","CurrentRead","CurrentDelta","PreConnectionCurrentRead","MovementSet","MovementRead","MovementDelta","ForceSet","ForceRead","ForceDelta",
"PreConnectionForceRead","ConnectionNumber","CycleNumber","CyclePosition","StatorCode_BarCode","Quality"]
big_frame = big_frame.fillna(0)
big_frame['Date1'] = big_frame['Date1'].astype('datetime64[D]').values
big_frame ['Time1']= pd.to_datetime(big_frame['Time1'], errors='raise', dayfirst=False, yearfirst=False, utc=None, 
                                    format= '%H:%M:%S', exact=True, unit=None, infer_datetime_format=True, origin='unix', 
                                    cache=False).dt.time

server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
for row in big_frame.itertuples():
    try:
     cursor.execute('''INSERT INTO dbo.Schweissanlage(Date1, Time1, Program, PreConnectionPulse, PreConnectionPower,
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
     conn.commit()
    except Exception as Argument: 
          filename1 = datetime.now().strftime("_%Y_%m_%d-%H_%M_%S")  
          f = open(V.CASTECH_SchweissanlageSchweissanlage + filename1+ ".txt", "a") 
          f.write(str(Argument)) 
          f.write(f'{row}\n')
          f.close()
conn.close()