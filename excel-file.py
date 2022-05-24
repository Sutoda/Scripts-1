# -*- coding: utf-8 -*-
"""
Created on Tue May  3 09:11:55 2022

@author: ibraham
"""

import pandas as pd
import xlsxwriter
import csv



filename = 'I:\Arbeitsgruppen\Prozessdatendokumentation\PM_Fertigung\TSS-214\CASTECH_Schweissanlage\Messdaten\WELDING_2021-11-24.CSV'
df = pd.read_csv(filename)


df.to_csv('data.csv')
wb = xlsxwriter.Workbook('I:\Arbeitsgruppen\Traceability\Sutoda-Ibraham\ExcelOrdner\mein.xlsx')
Sh = wb.add_worksheet('file')
with open(r'data.csv') as f:
          reader = csv.reader(f)
          for r, row in enumerate(reader):
              for c, val in enumerate(row):
                  Sh.write(r, c, val)
wb.close()