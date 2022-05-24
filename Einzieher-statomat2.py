# -*- coding: utf-8 -*-
"""
Created on Mon Dec  6 08:59:38 2021

@author: ibraham
"""
import glob
import pandas as pd
import pandas.io.sql
import pyodbc
import sys
import datetime
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V

DF= []
filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Einzieher_Statomat/*.csv")

for filename in filenames:

    p= pd.read_csv(filename, sep='\n', encoding='ISO 8859-1',  engine='python', header=None)[0].str.split(';', expand=True)
    Df = pd.DataFrame(p.tail(-120))
    for i in range(3,18):
        del Df[i]
    Df.columns = ['time(s)', 'X_Absolute', 'Y'] 

    a = [];b = []; c = []; d = []; e = []; f = []; g = []; h = []; I = []; j = []; k = []; l = []; m = []; n = []; o = []; P = [];
    q = []; r = []; s = []; t = []; u = []; v = []; w = []; x = []; y = []; z = []; aa = []; bb = []; cc = []; dd = []; ee = [];
    ff = []; gg = []; hh = []; ii = []; jj = []; kk = []; ll = []; mm = []; nn = []; oo= []; pp = []; qq = []; rr = []; ss = [];
    for i in range(len(Df)):
        a.append(p.iloc[1][1])
        b.append(p.iloc[6][1])
        c.append(p.iloc[7][1])
        d.append(p.iloc[11][1])
        e.append(p.iloc[12][1])
        f.append(p.iloc[21][1])
        g.append(p.iloc[21][4])
        h.append(p.iloc[20][1])
        I.append(p.iloc[20][4])
        j.append(p.iloc[19][1])
        k.append(p.iloc[19][4])
        l.append(p.iloc[18][1])
        m.append(p.iloc[18][4])
        n.append(p.iloc[17][1])
        o.append(p.iloc[17][4])
        P.append(p.iloc[22][4])
        q.append(p.iloc[23][4])
        r.append(p.iloc[24][4])
        s.append(p.iloc[40][1])
        t.append(p.iloc[41][1])
        u.append(p.iloc[42][1])
        v.append(p.iloc[43][1])
        w.append(p.iloc[49][1])
        x.append(p.iloc[50][1])
        y.append(p.iloc[51][1])
        z.append(p.iloc[55][1])
        aa.append(p.iloc[60][1])
        bb.append(p.iloc[62][1])
        cc.append(p.iloc[63][1])
        dd.append(p.iloc[64][1])
        ee.append(p.iloc[69][1])
        ff.append(p.iloc[71][1])
        gg.append(p.iloc[73][1])
        hh.append(p.iloc[74][1])
        ii.append(p.iloc[77][1])
        jj.append(p.iloc[79][1])
        kk.append(p.iloc[80][1])
        ll.append(p.iloc[81][1])
        mm.append(p.iloc[83][1])
        nn.append(p.iloc[86][1])
        oo.append(p.iloc[89][1])
        pp.append(p.iloc[91][1])
        qq.append(p.iloc[92][1])
        rr.append(p.iloc[97][2])
        ss.append(p.iloc[99][1])
    Columns = ['Networkname' ,'Date1' ,'Time1' ,'Part_Serial_Number' ,'Measuring_Program_Name' ,'Peaky_in_N' ,'YMINX_in_mm' ,'Peakx_in_mm' ,'XMAXY_in_N' ,
                  'RefX_in_mm' ,'XMAXX_in_mm' ,'BlockY_in_N' , 'XMINY_in_N' ,'BlockX_in_mm' ,'XMINX_in_mm' ,'YMINY_in_N' ,'YMAXX_in_mm' ,'YMAX_NoYin_N' ,
                  'Device_Type' ,'Device_Number' ,'Device_Serial_Number' ,'Firmware_version' ,'IP_Adress' ,'Network_name' ,'Profibus_Adress' ,
                  'Station_name' ,'SensorX_Type' ,'X_Used_Range_From_in_mm' ,'X_Used_Range_to_in_mm' ,'X_Decimal_places' ,'X_Filter_in_Hz' ,
                  'X_Tolerance_in_mm' ,'X_Zoom_From_in_mm' ,'X_Zoom_to_in_mm' ,'Sensor_Y_Type' ,'Y_Used_Range_From_in_N' ,'Y_Used_Range_to_in_N' ,
                  'Y_Decimal_places' ,'Y_Senstivity' ,'Y_Filter_in_Hz' ,'Y_Tolerance_in_N' ,'Y_Zoom_From_in_N' ,'Y_Zoom_to_in_N' ,'Threshold_X_in_mm' ,
                  'Cycle_time_out_in_s']
    df = pd.DataFrame(list(zip(a,b,c,d,e,f,g,h,I,j,k,l,m,n,o,P,q,r,s,t,u,v,w,x,y,z,aa,bb,cc,dd,ee,ff,gg,hh,ii,jj,kk,ll,mm,nn,oo,pp,qq,
                           rr,ss)), columns = Columns)
    Df.index = df.index
    Dataframe = df.join(Df, how = 'inner')
    
    DF.append(Dataframe)
dfs = pd.concat(DF, ignore_index=True) 
 
#dfs['SensorX Type'] = dfs['SensorX Type'].str.split('+').str[1]  
dfs['X_Filter_in_Hz'] = dfs['X_Filter_in_Hz'].map(lambda x: x.rstrip(' Hz '))
dfs['Y_Filter_in_Hz'] = dfs['Y_Filter_in_Hz'].map(lambda x: x.rstrip(' Hz '))
dfs['Peaky_in_N'] = dfs['Peaky_in_N'].str.replace(',','.')
dfs['Peaky_in_N'] = pd.to_numeric(dfs['Peaky_in_N'], errors = 'ignore')
dfs['YMINX_in_mm' ]= dfs['YMINX_in_mm' ].str.replace(',','.')
dfs['YMINX_in_mm' ] = pd.to_numeric(dfs['YMINX_in_mm' ], errors = 'ignore')
dfs['Peakx_in_mm']=dfs['Peakx_in_mm'].str.replace(',','.')
dfs['Peakx_in_mm'] = pd.to_numeric(dfs['Peakx_in_mm'], errors = 'ignore')
dfs['XMAXY_in_N']=dfs['XMAXY_in_N'].str.replace(',','.')
dfs['XMAXY_in_N'] = pd.to_numeric(dfs['XMAXY_in_N'], errors = 'ignore')
dfs['RefX_in_mm']=dfs['RefX_in_mm'].str.replace(',','.')
dfs['RefX_in_mm'] = pd.to_numeric(dfs['RefX_in_mm'], errors = 'ignore')
dfs['XMAXX_in_mm']=dfs['XMAXX_in_mm'].str.replace(',','.')
dfs['XMAXX_in_mm'] = pd.to_numeric(dfs['XMAXX_in_mm'], errors = 'ignore')
dfs['BlockY_in_N']=dfs['BlockY_in_N'].str.replace(',','.')
dfs['BlockY_in_N'] = pd.to_numeric(dfs['BlockY_in_N'], errors = 'ignore')
dfs['XMINY_in_N']=dfs['XMINY_in_N'].str.replace(',','.')
dfs['XMINY_in_N'] = pd.to_numeric(dfs['XMINY_in_N'], errors = 'ignore')
dfs['BlockX_in_mm']=dfs['BlockX_in_mm'].str.replace(',','.')
dfs['BlockX_in_mm'] = pd.to_numeric(dfs['BlockX_in_mm'], errors = 'ignore')
dfs['XMINX_in_mm']=dfs['XMINX_in_mm'].str.replace(',','.')
dfs['XMINX_in_mm'] = pd.to_numeric(dfs['XMINX_in_mm'], errors = 'ignore')
dfs['YMINY_in_N']=dfs['YMINY_in_N'].str.replace(',','.')
dfs['YMINY_in_N'] = pd.to_numeric(dfs['YMINY_in_N'], errors = 'ignore')
dfs['YMAXX_in_mm']=dfs['YMAXX_in_mm'].str.replace(',','.')
dfs['YMAXX_in_mm'] = pd.to_numeric(dfs['YMAXX_in_mm'], errors = 'ignore')
dfs['YMAX_NoYin_N']=dfs['YMAX_NoYin_N'].str.replace(',','.')
dfs['YMAX_NoYin_N'] = pd.to_numeric(dfs['YMAX_NoYin_N'], errors = 'ignore')
dfs['X_Used_Range_From_in_mm']=dfs['X_Used_Range_From_in_mm'].str.replace(',','.')
dfs['X_Used_Range_From_in_mm'] = pd.to_numeric(dfs['X_Used_Range_From_in_mm'], errors = 'ignore')
dfs['X_Used_Range_to_in_mm']=dfs['X_Used_Range_to_in_mm'].str.replace(',','.')
dfs['X_Used_Range_to_in_mm'] = pd.to_numeric(dfs['X_Used_Range_to_in_mm'], errors = 'ignore')
dfs['X_Tolerance_in_mm']=dfs['X_Tolerance_in_mm'].str.replace(',','.')
dfs['X_Tolerance_in_mm'] = pd.to_numeric(dfs['X_Tolerance_in_mm'], errors = 'ignore')
dfs['X_Zoom_From_in_mm']=dfs['X_Zoom_From_in_mm'].str.replace(',','.')
dfs['X_Zoom_From_in_mm'] = pd.to_numeric(dfs['X_Zoom_From_in_mm'], errors = 'ignore')
dfs['X_Zoom_to_in_mm']=dfs['X_Zoom_to_in_mm'].str.replace(',','.')
dfs['X_Zoom_to_in_mm'] = pd.to_numeric(dfs['X_Zoom_to_in_mm'], errors = 'ignore')
dfs['Y_Used_Range_From_in_N']=dfs['Y_Used_Range_From_in_N'].str.replace(',','.')
dfs['Y_Used_Range_From_in_N'] = pd.to_numeric(dfs['Y_Used_Range_From_in_N'], errors = 'ignore')
dfs['Y_Used_Range_to_in_N']=dfs['Y_Used_Range_to_in_N'].str.replace(',','.')
dfs['Y_Used_Range_to_in_N'] = pd.to_numeric(dfs['Y_Used_Range_to_in_N'], errors = 'ignore')
dfs['Y_Senstivity']=dfs['Y_Senstivity'].str.replace(',','.')
dfs['Y_Senstivity'] = pd.to_numeric(dfs['Y_Senstivity'], errors = 'ignore')
dfs['Y_Tolerance_in_N']=dfs['Y_Tolerance_in_N'].str.replace(',','.')
dfs['Y_Tolerance_in_N'] = pd.to_numeric(dfs['Y_Tolerance_in_N'], errors = 'ignore')
dfs['Y_Zoom_From_in_N']=dfs['Y_Zoom_From_in_N'].str.replace(',','.')
dfs['Y_Zoom_From_in_N'] = pd.to_numeric(dfs['Y_Zoom_From_in_N'], errors = 'ignore')
dfs['Y_Zoom_to_in_N']=dfs['Y_Zoom_to_in_N'].str.replace(',','.')
dfs['Y_Zoom_to_in_N'] = pd.to_numeric(dfs['Y_Zoom_to_in_N'], errors = 'ignore')
dfs['Threshold_X_in_mm']=dfs['Threshold_X_in_mm'].str.replace(',','.')
dfs['Threshold_X_in_mm'] = pd.to_numeric(dfs['Threshold_X_in_mm'], errors = 'ignore')
dfs['Cycle_time_out_in_s']=dfs['Cycle_time_out_in_s'].str.replace(',','.')
dfs['Cycle_time_out_in_s'] = pd.to_numeric(dfs['Cycle_time_out_in_s'], errors = 'ignore')
dfs['time2_in_s']= dfs['time(s)'].str.replace(',','.')
dfs['time2_in_s'] = pd.to_numeric(dfs['time2_in_s'], errors = 'ignore')
dfs['X_Absolute']=dfs['X_Absolute'].str.replace(',','.')
dfs['X_Absolute'] = pd.to_numeric(dfs['X_Absolute'], errors = 'ignore')
dfs['Y']=dfs['Y'].str.replace(',','.')
dfs['Y'] = pd.to_numeric(dfs['Y'], errors = 'ignore')
dfs['Y_Filter_in_Hz'] = pd.to_numeric(dfs['Y_Filter_in_Hz'], errors = 'ignore')
dfs['X_Filter_in_Hz'] = pd.to_numeric(dfs['X_Filter_in_Hz'], errors = 'ignore')



server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
for row in dfs.itertuples():
  try:
    cursor.execute('''
                  INSERT INTO dbo.Einzieher_Statomat(
                  Networkname,Date1,Time1,Part_Serial_Number,Measuring_Program_Name, Peaky_in_N,YMINX_in_mm,Peakx_in_mm,XMAXY_in_N,
                  RefX_in_mm,XMAXX_in_mm,BlockY_in_N, XMINY_in_N,BlockX_in_mm,XMINX_in_mm,YMINY_in_N,YMAXX_in_mm,YMAX_NoYin_N,
                  Device_Type,Device_Number,Device_Serial_Number,Firmware_version,IP_Adress,Network_name,Profibus_Adress,
                  Station_name,SensorX_Type,X_Used_Range_From_in_mm,X_Used_Range_to_in_mm,X_Decimal_places,X_Filter_in_Hz,
                  X_Tolerance_in_mm,X_Zoom_From_in_mm,X_Zoom_to_in_mm,Sensor_Y_Type,Y_Used_Range_From_in_N,Y_Used_Range_to_in_N,
                  Y_Decimal_places,Y_Senstivity,Y_Filter_in_Hz,Y_Tolerance_in_N,Y_Zoom_From_in_N,Y_Zoom_to_in_N,Threshold_X_in_mm,
                  Cycle_time_out_in_s,time2_in_s,X_Absolute, Y)
                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
                          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  row.Networkname, row.Date1, row.Time1, row.Part_Serial_Number, row.Measuring_Program_Name, row.Peaky_in_N, row.YMINX_in_mm, row.Peakx_in_mm,  row.XMAXY_in_N, row.
                  RefX_in_mm, row.XMAXX_in_mm, row.BlockY_in_N,  row.XMINY_in_N, row.BlockX_in_mm, row.XMINX_in_mm, row.YMINY_in_N, row.YMAXX_in_mm, row.YMAX_NoYin_N,  row.
                  Device_Type, row.Device_Number, row.Device_Serial_Number, row.Firmware_version, row.IP_Adress, row.Network_name, row.Profibus_Adress,  row.
                  Station_name, row.SensorX_Type, row.X_Used_Range_From_in_mm, row.X_Used_Range_to_in_mm, row.X_Decimal_places, row.X_Filter_in_Hz,  
                  row.X_Tolerance_in_mm, row.X_Zoom_From_in_mm, row.X_Zoom_to_in_mm, row.Sensor_Y_Type, row.Y_Used_Range_From_in_N, row.Y_Used_Range_to_in_N,  row.
                  Y_Decimal_places, row.Y_Senstivity, row.Y_Filter_in_Hz, row.Y_Tolerance_in_N, row.Y_Zoom_From_in_N, row.Y_Zoom_to_in_N, row.Threshold_X_in_mm,  row.
                  Cycle_time_out_in_s,  row.time2_in_s,  row.X_Absolute,  row.Y )
    
    conn.commit()
  except Exception as Argument:
              filename1 = datetime.now().strftime("_%Y_%m_%d-%H_%M_%S")
              f = open(V.Einzieher_Statomat + filename1+ ".txt", "a")
              f.write(str(Argument))
              f.write(f'{row}\n')
              f.close()
conn.close()     


 






