# -*- coding: utf-8 -*-
"""
Created on Mon May 16 14:52:05 2022

@author: ibraham
"""

import smtplib
import os
import glob

path = "I:\Arbeitsgruppen\Traceability\Sutoda-Ibraham\Fehlerbehandlung"
dir = os.listdir(path)
filenames = glob.glob(path)

url = 'sch-exc-01'
conn = smtplib.SMTP(url,587)
conn.starttls()
user,password = ('zapigroup\intern','asdfgh')
conn.login(user,password)


msg = 'From: intern@schabmueller.de\nTo: Sutoda.Ibraham@schabmueller.de\nSubject: Probleme bei der Daten?bertragung\n\n{}'
for filename in filenames:
    txt = 'folgende Dateien wurden geschrieben' + filename
if len(dir) != 0: 
   conn.sendmail('intern@schabmueller.de','Sutoda.Ibraham@schabmueller.de',msg.format(txt))


