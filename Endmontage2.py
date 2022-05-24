import glob
import pandas as pd
import pandas.io.sql
import pyodbc
import sys
sys.path.append(r"C:\Users\ibraham\Variablen.py")
import Variablen as V
from datetime import datetime

Dfs = []
filenames = glob.glob(r"I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\Endmontage_Hoering\Speicherkarte 21042021/*csv")
for filename in filenames:
    p= pd.read_csv(filename, sep='\n', encoding='ISO 8859-1',  engine='python', header=None)[0].str.split(';', expand=True)
    a = []; b = []; c = []; d = []; e = []; f = [];
    for i in range(0,6):
        a.append(p.iloc[0][0])
        b.append(p.iloc[1][0])
        c.append(p.iloc[2][0])
        d.append(p.iloc[3][0])
        e.append(p.iloc[4][0])
        f.append(p.iloc[5][0])
    g = [p.iloc[7][0], p.iloc[8][0], p.iloc[9][0], p.iloc[10][0], p.iloc[11][0]]   
    h = [p.iloc[7][1], p.iloc[8][1], p.iloc[9][1], p.iloc[10][1], p.iloc[11][1]] 
    j = [p.iloc[7][2], p.iloc[8][2], p.iloc[9][2], p.iloc[10][2], p.iloc[11][2]]
    l = [0, 0, p.iloc[13][1],p.iloc[14][1], 0]
    m = [0, 0, p.iloc[13][2],p.iloc[14][2], 0]
    n = [0, 0, p.iloc[13][3],p.iloc[14][3], 0]
    o = [0, 0, p.iloc[13][4],p.iloc[14][4], 0]
    
    Columns = ['Bezeichnung', 'Materialnummer', 'Seriennummer', 'Typ', 'Laenge', 'Bearbeiter','Erstellungszeitpunkt', 'material_nr',
           'serien_nr','Weg', 'Kraft_bei_Abschaltung', 'Auswertung_Weg_Ok','Auswertung_Kontrollpunkt_Ok']
    df = pd.DataFrame(list(zip(g,a,b,c,d,e,f,h,j,l,m,n,o)), columns = Columns)
    
    df.drop(df.loc[df['Erstellungszeitpunkt']=='LogValX'].index, inplace=True)
    df['Materialnummer'] = df['Materialnummer'].map(lambda x: x.lstrip('Materialnummer:'))
    df['Seriennummer'] = df['Seriennummer'].map(lambda x: x.lstrip('Seriennummer:'))
    df['Typ'] = df['Typ'].map(lambda x: x.lstrip('Typ:'))
    df['Laenge'] = df['Laenge'].map(lambda x: x.lstrip('Länge:'))
    df['Laenge'] = pd.to_numeric(df['Laenge'],errors='ignore')
    df['Bearbeiter'] = df['Bearbeiter'].map(lambda x: x.lstrip('Bearbeiter:'))
    df['Erstellungszeitpunkt'] = df['Erstellungszeitpunkt'].map(lambda x: x.lstrip(' Erstellungszeitpunkt: '))
    df['AM_PM'] = df['Erstellungszeitpunkt'].astype(str).str.split(" ").str[3]
    df['Weg'] = pd.to_numeric(df['Weg'], errors = 'ignore')
    df['Kraft_bei_Abschaltung'] = pd.to_numeric(df['Kraft_bei_Abschaltung'], errors = 'ignore')
    df['Erstellungszeitpunkt'] = df['Erstellungszeitpunkt'].map(lambda x: x.rstrip(' AM '))
    df['Erstellungszeitpunkt'] = df['Erstellungszeitpunkt'].map(lambda x: x.rstrip(' PM '))
    df['Erstellungszeitpunkt'] = pd.to_datetime(df['Erstellungszeitpunkt'], errors = 'ignore') 
    
    #df['Datum'] = [d.date() for d in df['Erstellungszeitpunkt']]
    #df['Zeit'] = [d.time() for d in df['Erstellungszeitpunkt']]
    #df['AM/PM'] = df['AM_PM']
    #del df['Erstellungszeitpunkt'] 
    #del df['AM_PM']
    Dfs.append(df)
dfs = pd.concat(Dfs, ignore_index=True)
dfs = dfs.fillna(0)
print(dfs.dtypes)
server = 'ltp077'
db = 'TraceabilityTest'
conn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER=' + server + ';DATABASE=' + db + ';Trusted_Connection=yes')
cursor = conn.cursor()
    
for row in dfs.itertuples():
 try:
    cursor.execute('''
                  INSERT INTO dbo.Endmontage(
                  Bezeichnung, Materialnummer, Seriennummer, Typ, Laenge, Bearbeiter, Erstellungszeitpunkt, material_nr,
                     serien_nr,Weg, Kraft_bei_Abschaltung, Auswertung_Weg_Ok,Auswertung_Kontrollpunkt_Ok, AM_PM)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)''',
                  row.Bezeichnung, 
                  row.Materialnummer, 
                  row.Seriennummer, 
                  row.Typ, 
                  row.Laenge,
                  row.Bearbeiter,
                  row.Erstellungszeitpunkt,
                  row.material_nr,
                  row.serien_nr,
                  row.Weg,
                  row.Kraft_bei_Abschaltung,
                  row.Auswertung_Weg_Ok,
                  row.Auswertung_Kontrollpunkt_Ok,
                  row.AM_PM)
    conn.commit()
 except Exception as Argument:
              filename1 = datetime.now().strftime("%Y_%m_%d-%H_%M_%S")
              f = open(V.Endmontage_Hoering + filename1+ ".txt", "a")
              f.write(str(Argument))
              f.write(f'{row}\n')
              f.close()
    
conn.close()
