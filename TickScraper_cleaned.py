#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 17 21:34:10 2018

@author: kristijan
"""

from ib_insync import *
from time import sleep
from datetime import datetime, time, timedelta
import pytz
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

#################################
#1. Get Data (Scrape or Load)   #
#################################

#Parameters 
Gateway=0
Live =0
symbol = 'GC'
dateTo =  datetime.now().strftime("%Y%m%d %H:%M:%S")
dateFrom = '20171020 9:30:00'

if Gateway==0:
    port = 7497
    if Live==1:
        port = 7496
elif Gateway==1:
    port = 4002
    if Live==1:
        port = 4001
clientId = 1

util.startLoop()
ib = IB()
ib.connect("127.0.0.1", port, clientId)

#contract = Stock(symbol, exchange='SMART', currency='USD')#, primaryExchange = 'NASDAQ')
contract = ContFuture(symbol, exchange='NYMEX')
est=pytz.timezone('America/New_York')

allTicks = []
while dateTo>dateFrom:    
    ticks = ib.reqHistoricalTicks(contract, '', dateTo, 1000,
                                  'BID_ASK', useRth=True,ignoreSize=False) #use True for less data
    #pause while IB does regular update
    #while (time(23,30) <= datetime.now().time()) or (
    #        datetime.now().time() <= time(0,50)):
    #    sleep(10)
        
    allTicks = ticks[1:] + allTicks
    dateTo = allTicks[0].time.astimezone(est).strftime("%Y%m%d %H:%M:%S")
    print(dateTo)
    
allData = pd.DataFrame([[l.time.astimezone(est).replace(tzinfo=None),
                         l.priceBid,
                         l.priceAsk,
                         l.sizeBid,
                         l.sizeAsk] for l in allTicks],
        columns=['time','bid_p','ask_p','bid_v','ask_v'])
allData.set_index("time", inplace=True) #replace index with datetime

ib.disconnect()
#Save data
savelocData = '/Users/kristijan/Desktop/Backup Data/'
interval = 'TICK_ALL'
firstDay = datetime.strftime(allData.index[0],'%Y%m%d')
lastDay = datetime.strftime(allData.index[-1],'%Y%m%d')
#allData.to_pickle(savelocData+symbol+' '+interval+' '+firstDay+' to '+lastDay)
allData.to_hdf(savelocData+symbol+' '+interval+' '+firstDay+' to '+lastDay,
               key='df', complevel=9, complib='zlib') #loss-less compression, 11x smaller
#slightly better if you store as a multindex (extra 4%), but takes time
#allData.index = pd.MultiIndex.from_arrays([allData.index.date, allData.index.time], names=['Date','Time'])


#################################
#2. Import/Clean Data           #
#################################
'''
import os

fileName = [s for s in os.listdir(savelocData) if s.startswith(symbol+' '+interval)]
#allData = pd.read_pickle(savelocData+''.join(fileName))
allData = pd.read_hdf(savelocData+''.join(fileName),'df')
'''
DTI = pd.DatetimeIndex(allData.index)
#allData['year'] = DTI.year
allData['month'] = DTI.month
allData['day'] = DTI.day
allData['weekday'] = DTI.dayofweek #Monday = 0, Sunday = 6
#allData['week'] = DTI.weekofyear 
allData['hour'] = DTI.hour
allData['MOD'] = DTI.hour*60+DTI.minute
#allData['MOW'] = allData.weekday*60*24+allData.MOD #minutes from Monday morning midnight
allData['SOD'] = DTI.hour*3600+DTI.minute*60+DTI.second


allData['imbalance'] = allData.bid_v*allData.bid_p-allData.ask_v*allData.ask_p  
allData['imbalance%'] = allData.imbalance/(allData.bid_v*allData.bid_p+allData.ask_v*allData.ask_p)
allData['mid'] = 0.5*(allData.bid_p+allData.ask_p)


#################################
#3. GOLD 10am FIX               #
#################################

allData['spread'] = allData.ask_p-allData.bid_p
focal = allData[(allData.day==23)&(allData.MOD<630)]
fix = focal.index[focal.SOD==36000][0]

fig=plt.figure()
plt.plot(focal.mid)
plt.axvline(x=fix,color='r')
plt.title('Mid before/after 3pm London Fix')

fig=plt.figure()
plt.plot(focal.bid_v)
plt.plot(focal.ask_v)
plt.axvline(x=fix,color='r')
plt.legend(['Bid','Ask'])
plt.title('Level 1 volume before/after 3pm London Fix')

fig=plt.figure()
plt.plot(focal.ask_p-focal.bid_p)
plt.axvline(x=fix,color='r')
plt.title('Spread before/after 3pm London Fix')

fig=plt.figure()
plt.plot(focal.bid_v-focal.ask_v)
plt.axvline(x=fix,color='r')
plt.title('Level 1 VOI before/after 3pm London Fix')


