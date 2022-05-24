# -*- coding: utf-8 -*-
"""
Created on Thu Mar 31 16:08:47 2022

@author: ibraham
"""

import smtplib
from email.mime.text import MIMEText
Text = 'an exception is occured'
mail = MIMEText(Text)
mail['Subject'] = 'Anlage'
mail['From'] = 'Absender Email-Adresse'
mail['To'] = 'Empfänger Email-Adresse'
sender = smtplib.SMTP(Host='',Port=0,local_hostname=Keine,[Zeitüberschreitung,]Quelladresse=Keine)
sender = smtplib.SMTP("smtp.gmail.com", 465)
sender.ehlo()
sender.starttsl()
sender.ehlo()
sender.login('Absender Email-Adresse', 'Password')
sender.send_message(mail)
sender.close()