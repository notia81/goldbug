#!/usr/bin/env python3
from ib_insync import *
from time import sleep
from datetime import datetime, time, timedelta
import pytz
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

class DataPull(object):
    """
        Class for pulling data given range.
    """
    def __init__(self, clientId, Live, symbol, Gateway, ibip):
        self.clientId = clientId
        self.Live = Live
        self.symbol = symbol
        self.Gateway = Gateway
        self.ibip = ibip
        self.big_data = []
        self.ib = self.ibConnect()

    def ibConnect(self):
        Gateway= self.Gateway
        Live = self.Live
        symbol = self.symbol
        if Gateway==0:
            port = 7497
            if Live==0:
                port = 7496
        elif Gateway==1:
            port = 4001
            if Live == 0:
                port = 4002
        clientId = self.clientId
        ib = IB()
        ib.connect(self.ibip, port, clientId)
        return(ib)

    def pull_1000(self, dateFrom, est):
        contract = ContFuture('GC', exchange='NYMEX')
        dateFrom = dateFrom + pd.Timedelta('1 hours')
        dateTo = dateFrom + pd.Timedelta('0.1 hours')
        origdateTo = dateTo.tz_localize(est).strftime("%H:%M:%S")
        origdateFrom = dateFrom.tz_localize(est).strftime("%H:%M:%S")
        dateFrom = dateFrom.tz_localize(est).strftime("%Y%m%d %H:%M:%S")
        dateTo = dateTo.tz_localize(est).strftime("%Y%m%d %H:%M:%S")
        print("grabbing data from {} to {}".format(dateFrom, dateTo))
        allTicks = []
        while dateTo>=dateFrom:
            try:
                ticks = self.ib.reqHistoricalTicks(contract, '', dateTo, 1000,
                                              'BID_ASK', useRth=False,ignoreSize=False) #use True for less data
                allTicks = ticks[1:] + allTicks
                dateTo = allTicks[0].time.astimezone(est).strftime("%Y%m%d %H:%M:%S")
            except:
                 print('filed at {} and 1000 tickets'.format(dateTo))
               
        
        allData = pd.DataFrame([[l.time.astimezone(est).replace(tzinfo=None), l.priceBid, l.priceAsk, l.sizeBid,l.sizeAsk] 
                                for l in allTicks],
                               columns=['time','bid_p','ask_p','bid_v','ask_v'])
        allData.set_index("time", inplace=True) #replace index with datetime
        return(allData.between_time(origdateFrom, origdateTo))
    
    def pull_range(self, start, end):
        symbol = self.symbol
        datestopull = pd.bdate_range(start = '2018-02-01', end='2018-11-01')
        contract = ContFuture('GC', exchange='NYMEX')
        est=pytz.timezone('America/New_York')
        
        for date_index in range(0, len(datestopull)):
            self.big_data = self.big_data + [self.pull_1000(datestopull[date_index], est, contract)]
    
    def write_data(self, savelocData, interval, write_month):
        write_data = pd.concat(self.big_data)
        write_data.to_hdf(savelocData+symbol+'_'+interval+'_'+ write_month + '.hdf5', key='df', complevel=9, complib='zlib')
        return(1)
    def print_data(self):
        print(self.big_data)
    def disconnect(self):
        print(self.ib.disconnect())