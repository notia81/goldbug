#!/usr/bin/env python3
from ib_insync import *
from time import sleep
from datetime import datetime, time, timedelta
import pytz
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

Gateway=0
Live =0
symbol = 'GC'

if Gateway==0:
    port = 7497
    if Live==1:
        port = 7496
elif Gateway==1:
    port = 4002
    if Live==1:
        port = 4001
clientId = 3

#util.startLoop()
ib = IB()
ib.connect("127.0.0.1", 4002, clientId)

#only need data from 13:00 to 17:00 London time daily. Save each month, append a hdf5 with the year-month as key.
#contract = ContFuture('GC', exchange='NYMEX')
#est=pytz.timezone('America/New_York')
#allTicks = []
def pull_date(dateFrom, est, contract):
    dateFrom = dateFrom + pd.Timedelta('7 hours')
    dateTo = dateFrom + pd.Timedelta('6 hours')
    origdateTo = dateTo.tz_localize(est).strftime("%H:%M:%S")
    origdateFrom = dateFrom.tz_localize(est).strftime("%H:%M:%S")
    dateFrom = dateFrom.tz_localize(est).strftime("%Y%m%d %H:%M:%S")
    dateTo = dateTo.tz_localize(est).strftime("%Y%m%d %H:%M:%S")
    print("grabbing data from {} to {}".format(dateFrom, dateTo))
    allTicks = []
    while dateTo>=dateFrom:
        try:
            ticks = ib.reqHistoricalTicks(contract, '', dateTo, 1000,
                                          'BID_ASK', useRth=True,ignoreSize=False) #use True for less data
            allTicks = ticks[1:] + allTicks
            dateTo = allTicks[0].time.astimezone(est).strftime("%Y%m%d %H:%M:%S")
        except:
             print('filed at {} and 1000 tickets'.format(dateTo))
           
    
    allData = pd.DataFrame([[l.time.astimezone(est).replace(tzinfo=None), l.priceBid, l.priceAsk, l.sizeBid,l.sizeAsk] 
                            for l in allTicks],
                           columns=['time','bid_p','ask_p','bid_v','ask_v'])
    allData.set_index("time", inplace=True) #replace index with datetime
    return(allData.between_time(origdateFrom, origdateTo))
    


datestopull = pd.bdate_range(start = '2018-02-01', end='2018-11-01')
contract = ContFuture('GC', exchange='NYMEX')
est=pytz.timezone('America/New_York')

savelocData = "/home/cl/google drive/goldbug/"
interval = 'TICK_ALL'
symbol = 'GC_contfut'

big_data = []
write_month = str(datestopull[0].year) + '-' + str(datestopull[0].month)
for date_index in range(0, len(datestopull)):
    currentmonth = str(datestopull[date_index].year) + '-' + str(datestopull[date_index].month)
    if (currentmonth != write_month):
        write_data = pd.concat(big_data)
        write_data.to_hdf(savelocData+symbol+'_'+interval+'_'+write_month,
               key='df', complevel=9, complib='zlib')
        big_data = []
        write_month = currentmonth
    big_data = big_data + [pull_date(datestopull[date_index], est, contract)]
    
write_data = pd.concat(big_data)
write_data.to_hdf(savelocData+symbol+'_'+interval+'_'+ write_month,
                key='df', complevel=9, complib='zlib')
big_data = []

