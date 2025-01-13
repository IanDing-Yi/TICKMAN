import pandas as pd
import numpy as np
import random as r
from datetime import datetime, date, time, timezone
from multiprocessing.dummy import Pool as ThreadPool
import time

# thread pool executor
class MultiThreadExecutor():
    def __init__(self, op):
        self.op = op
    
    def opWrap(self, params):
        # Initialization
        droppingInds = self.op(params)
        return droppingInds

    def calculateParallel(self, paraList, threads=2):
        pool = ThreadPool(threads)
        droppingInds = pool.map(self.opWrap, paraList)
        pool.close()
        pool.join()
        return droppingInds

class StockAnalysis:
    
    def __init__(self, onExcDF_stockDict_path, offExcDF_stockDict_path, __randomState = 1, nb_workers = 14):
        self.onExcDF_stockDict = self.loadTradeData(onExcDF_stockDict_path)
        self.offExcDF_stockDict = self.loadTradeData(offExcDF_stockDict_path)
        
        self.onExcDF_stockDict, self.offExcDF_stockDict = self.filtering(self.onExcDF_stockDict, 
                                                                         self.offExcDF_stockDict)
        
        self.timeInt = pd.to_datetime(['09:30:00',
                                       '10:00:00',
                                       '10:30:00',
                                       '11:00:00',
                                       '11:30:00',
                                       '12:00:00',
                                       '12:30:00',
                                       '13:00:00',
                                       '13:30:00',
                                       '14:00:00',
                                       '14:30:00',
                                       '15:00:00',  
                                       '15:30:00',
                                       '16:00:00'],
                                       format='%H:%M:%S').time
        
        self.maxDate, self.minDate = self.max_min_dates(self.onExcDF_stockDict, self.offExcDF_stockDict)
        
        # pre-count numbers
        self.nb_off_retail = 816180153
        self.nb_off = 3176944621

        self.bjzz2 = 0.3
        # old sampRas = [0.5, 0.6, 0.7]
        self.sampRas = [0.45]
        self.sampRb = round(self.computeRb(self.nb_off_retail, self.nb_off), 2)
        self.bjzz1 = self.computeBjzz1(self.sampRb, self.sampRas, self.bjzz2)
        
        # print("self.sampRb", "self.bjzz1")
        # print(self.sampRb, self.bjzz1)
        
        self.__randomState = __randomState
        self.nb_workers = nb_workers

        
    @staticmethod
    def max_min_dates(onExcDF_stockDict, offExcDF_stockDict):
        maxs = []
        mins = []

        maxs.append(max(onExcDF_stockDict.Time))
        mins.append(min(onExcDF_stockDict.Time))

        maxs.append(max(offExcDF_stockDict.Time))
        mins.append(min(offExcDF_stockDict.Time))

        maxDate = max(maxs).date()
        minDate = min(mins).date()

        return (maxDate, minDate)
    
    @staticmethod
    def filtering(onExcDF_stockDict, offExcDF_stockDict):

        # t0 = time.time()
        # remove any trade before 9:30 and after 16:00
        # remove first 15 min and end 15 min trades during regular trading time

        timeStart = pd.to_datetime('09:45:00').time()
        timeEnd = pd.to_datetime('15:45:00').time()

        onExcDF_stockDict = onExcDF_stockDict[
            (timeStart <= onExcDF_stockDict.Time.dt.time) &
            (onExcDF_stockDict.Time.dt.time < timeEnd) &
            (onExcDF_stockDict.SIZE > 0) &
            (onExcDF_stockDict.QSPREAD > 0) &
            (onExcDF_stockDict.BID_PRICE > 0.1) &
            (onExcDF_stockDict.ASK_PRICE < 999998) &
            (onExcDF_stockDict.MID_POINT_1MIN_LATER != 4999.50) &
            (onExcDF_stockDict.MID_POINT_1MIN_LATER > 0)]
        onExcDF_stockDict.reset_index(drop=True)
        
        offExcDF_stockDict = offExcDF_stockDict[
            (timeStart <= offExcDF_stockDict.Time.dt.time)&
            (offExcDF_stockDict.Time.dt.time < timeEnd)&
            (offExcDF_stockDict.SIZE > 0) &
            (offExcDF_stockDict.QSPREAD > 0) &
            (offExcDF_stockDict.BID_PRICE > 0.1) &
            (offExcDF_stockDict.ASK_PRICE < 999998) &
            (offExcDF_stockDict.MID_POINT_1MIN_LATER != 4999.50) &
            (offExcDF_stockDict.MID_POINT_1MIN_LATER > 0) &
            (offExcDF_stockDict.MID_POINT_1MIN_LATER >= offExcDF_stockDict.MID_POINT*0.6)]
        offExcDF_stockDict.reset_index(drop=True)

        return (onExcDF_stockDict, offExcDF_stockDict)
    
    @staticmethod
    def loadTradeData(filePath):
        excDF = pd.read_csv(filePath, usecols=[i for i in range(1, 21)],
                            dtype = {"Symbol": str, 
                                     "Time": str, 
                                     "RSPREAD": float, 
                                     "MID_POINT_1MIN_LATER": float, 
                                     "PRICE": np.float64, 
                                     "QSPREAD": np.float64, 
                                     "ESPREAD": np.float64, 
                                     "BID_PRICE": np.float64, 
                                     "ASK_PRICE": np.float64, 
                                     "MID_POINT": np.float64, 
                                     "SIZE": np.int64, 
                                     "RETAIL": np.int64, 
                                     "BUYSELLFLAG": np.int64, 
                                     "VALID_QUOTE": np.int64, 
                                     "ASK_SIZE": np.int64, 
                                     "BID_SIZE": np.int64, 
                                     "RPI": str, 
                                     "COND_CODE": np.float64, 
                                     "DELETED_TIME": str, 
                                     "TICK_STATUS": np.int64})

        excDF['Time'] = pd.to_datetime(excDF['Time'], format="%Y/%m/%d %H:%M:%S.%f") # Replace with date format

        # remove dup
        excDF.drop_duplicates(ignore_index = True)
        excDF.fillna(0, inplace=True)

        return excDF
    
    @staticmethod
    def computeRb(nb_off_retail, nb_off):
        return nb_off_retail/nb_off

    @staticmethod
    def computeBjzz1(sampRb, sampRas = [0.5, 0.6, 0.7], bjzz2 = 0.3):
        bjzz1 = 0
        for sampRa in sampRas:
    #         print((sampRb - bjzz2*sampRa) / (1-sampRa))
            bjzz1 += (sampRb - bjzz2*sampRa) / (1-sampRa)

        bjzz1 /= len(sampRas)
        return round(bjzz1, 2)

    @staticmethod
    def old_computeRa(bjzz1, bjzz2, Rb):
        # based on Rb for each stock
        if Rb <= 0.125:
            return Rb
        elif Rb > 0.125 and Rb <= 0.3:
            return (Rb - bjzz1) / (bjzz2 - bjzz1)
        else:
            return 1

    @staticmethod
    def computeRa(bjzz1, bjzz2, Rb, verbose=False):
        # based on Rb for each stock
        if verbose:
            print("----bjzz1, bjzz2, Rb----"+str(bjzz1)+" "+str(bjzz2)+" "+str(Rb)+'----')
        if Rb <= 0.247:
            if verbose:
                print("computeRa: branch 1")
            return Rb
        elif Rb > 0.247 and Rb <= 0.3:
            if verbose:
                print("computeRa: branch 2: " + str((Rb - bjzz1) / (bjzz2 - bjzz1)) + " " + str(min(0.8, ((Rb - bjzz1) / (bjzz2 - bjzz1)))))
                
            return min(0.8, ((Rb - bjzz1) / (bjzz2 - bjzz1)) )
        else:
            if verbose:
                print("computeRa: branch 3")
            return 0.8

    @staticmethod
    def computeFlipRate(Ra, Rb):
        if Ra <= Rb:
            return 0
        elif Rb == 1:
            return 0
        else:
            return (Ra - Rb) / (1 - Rb)
        
    @staticmethod
    def agg_30min(offExcDF_stockDict):
        DF_off_retail = offExcDF_stockDict[offExcDF_stockDict.reasigned_retail == 1]
        DF_off_non_retail = offExcDF_stockDict[offExcDF_stockDict.reasigned_retail == 0]
        
        DF_off_retail = DF_off_retail.groupby(pd.Grouper(key='Time', freq='30min'), observed=True)
        DF_off_retail = DF_off_retail.agg(Symbol = ('Symbol', 'min'),
                                          TRADES = ('Symbol', 'count'), 
                                          PRICE = ('PRICE', 'mean'), 
                                          DEPTH = ('DEPTH', 'mean'), 
                                          MEAN_SIZE = ('SIZE', 'mean'),
                                          VOL = ('SIZE', 'sum'),
                                          MEAN_DOLLAR_SIZE = ('DOLLAR_SIZE', 'mean'),
                                          DOLLAR_VOL = ('DOLLAR_SIZE', 'sum'),
                                          QSPREAD_PERCENTAGE = ('QSPREAD_PERCENTAGE', 'mean'),
                                          ESPREAD_PERCENTAGE = ('ESPREAD_PERCENTAGE', 'mean'),
                                          RSPREAD_PERCENTAGE = ('RSPREAD_PERCENTAGE', 'mean'),
                                          PRICEIMPROVEMENT_PERCENTAGE = ('PRICEIMPROVEMENT_PERCENTAGE', 'mean'),
                                          PRICEIMPACT_PERCENTAGE = ('PRICEIMPACT_PERCENTAGE', 'mean'),
                                          Is_Retail = ('reasigned_retail', 'min')).reset_index()
        
        DF_off_non_retail = DF_off_non_retail.groupby(pd.Grouper(key='Time', freq='30min'), observed=True)
        DF_off_non_retail = DF_off_non_retail.agg(Symbol = ('Symbol', 'min'),
                                                  TRADES = ('Symbol', 'count'), 
                                                  PRICE = ('PRICE', 'mean'), 
                                                  DEPTH = ('DEPTH', 'mean'), 
                                                  MEAN_SIZE = ('SIZE', 'mean'),
                                                  VOL = ('SIZE', 'sum'),
                                                  MEAN_DOLLAR_SIZE = ('DOLLAR_SIZE', 'mean'),
                                                  DOLLAR_VOL = ('DOLLAR_SIZE', 'sum'),
                                                  QSPREAD_PERCENTAGE = ('QSPREAD_PERCENTAGE', 'mean'),
                                                  ESPREAD_PERCENTAGE = ('ESPREAD_PERCENTAGE', 'mean'),
                                                  RSPREAD_PERCENTAGE = ('RSPREAD_PERCENTAGE', 'mean'),
                                                  PRICEIMPROVEMENT_PERCENTAGE = ('PRICEIMPROVEMENT_PERCENTAGE', 'mean'),
                                                  PRICEIMPACT_PERCENTAGE = ('PRICEIMPACT_PERCENTAGE', 'mean'),
                                                  Is_Retail = ('reasigned_retail', 'min')).reset_index()
        
        df_agg = pd.concat([DF_off_retail, DF_off_non_retail])
        return df_agg[df_agg.TRADES > 0]
        
    def Compute(self, storePath):
        #test
#         print(0)
#         print("elapsed: ", time.time()-t0, "sec")
        def index_on_off_retail_not_same_time_30min(datetimeMap_30min_stock):
            dayBegin = datetimeMap_30min_stock[0]
            dayEnd = datetimeMap_30min_stock[1]

            onRetail = self.onExcDF_stockDict[(dayBegin <= self.onExcDF_stockDict.Time) &
                                              (self.onExcDF_stockDict.Time < dayEnd) &
                                              (abs(self.onExcDF_stockDict.RETAIL) > 0)]
            offRetail = self.offExcDF_stockDict[(dayBegin <= self.offExcDF_stockDict.Time) &
                                                (self.offExcDF_stockDict.Time < dayEnd) &
                                                (abs(self.offExcDF_stockDict.RETAIL) > 0)]

            on_droppingData = None
            off_droppingData  = None
            if len(onRetail) == 0 or len(offRetail) == 0:
                on_droppingData = self.onExcDF_stockDict[(dayBegin <= self.onExcDF_stockDict.Time) & 
                                                         (self.onExcDF_stockDict.Time < dayEnd)].index
                off_droppingData = self.offExcDF_stockDict[(dayBegin <= self.offExcDF_stockDict.Time) & 
                                                           (self.offExcDF_stockDict.Time < dayEnd)].index

            return (on_droppingData, off_droppingData)

        # every 30 min interval for each stock
        datetimeMap_30min_on_off_stock = []
        # each day
        curDate = self.minDate
        while curDate <= self.maxDate:
            # each time interval
            for start, end in zip(self.timeInt, self.timeInt[1:]):
                # display test: print(start, end)
                dayBegin = pd.Timestamp.combine(curDate, start)
                dayEnd = pd.Timestamp.combine(curDate, end)
                # display test: print(type(dayBegin), dayEnd)
                datetimeMap_30min_on_off_stock.append([dayBegin, dayEnd])
            curDate += pd.Timedelta(days=1)
        
        # Parallel run
        mte = MultiThreadExecutor(index_on_off_retail_not_same_time_30min)
        on_off_indexes = mte.calculateParallel(datetimeMap_30min_on_off_stock, self.nb_workers)
        del mte
        
        #test
#         print("0.-1")
#         print("elapsed: ", time.time()-t0, "sec")
        
        on_droppingDataInds = []
        off_droppingDataInds = []
        
        for dropping_inds in on_off_indexes:
            (on_droppingData, off_droppingData) = dropping_inds
            if on_droppingData is not None:
                on_droppingDataInds += on_droppingData.tolist()
            if off_droppingData is not None:
                off_droppingDataInds += off_droppingData.tolist()
                
        self.onExcDF_stockDict.drop(on_droppingDataInds, inplace=True)
        self.offExcDF_stockDict.drop(off_droppingDataInds, inplace=True)
        
        #test
#         print(1)
#         print("elapsed: ", time.time()-t0, "sec")
        
        self.onExcDF_stockDict.reset_index(drop=True)

        self.onExcDF_stockDict['reasigned_retail'] = [0]*len(self.onExcDF_stockDict)
        self.onExcDF_stockDict.loc[self.onExcDF_stockDict[abs(self.onExcDF_stockDict.RETAIL) > 0].index, 'reasigned_retail'] = 1
        self.onExcDF_stockDict["DEPTH"] = self.onExcDF_stockDict["ASK_SIZE"] + self.onExcDF_stockDict["BID_SIZE"]
        self.onExcDF_stockDict["QSPREAD_PERCENTAGE"] = self.onExcDF_stockDict["QSPREAD"] / self.onExcDF_stockDict["MID_POINT"]
        self.onExcDF_stockDict["ESPREAD_PERCENTAGE"] = self.onExcDF_stockDict["ESPREAD"] / self.onExcDF_stockDict["MID_POINT"]
        self.onExcDF_stockDict["RSPREAD_PERCENTAGE"] = self.onExcDF_stockDict["RSPREAD"] / self.onExcDF_stockDict["MID_POINT"]
        self.onExcDF_stockDict["PRICEIMPROVEMENT_PERCENTAGE"] = (self.onExcDF_stockDict["QSPREAD"] - self.onExcDF_stockDict["ESPREAD"]) / self.onExcDF_stockDict["MID_POINT"]
        self.onExcDF_stockDict["PRICEIMPACT_PERCENTAGE"] = (self.onExcDF_stockDict["ESPREAD"] - self.onExcDF_stockDict["RSPREAD"]) / self.onExcDF_stockDict["MID_POINT"]
        self.onExcDF_stockDict["DOLLAR_SIZE"] = self.onExcDF_stockDict["PRICE"] * self.onExcDF_stockDict["SIZE"]
        
        self.offExcDF_stockDict.reset_index(drop=True)
        self.offExcDF_stockDict['reasigned_retail'] = [0]*len(self.offExcDF_stockDict)
        self.offExcDF_stockDict.loc[self.offExcDF_stockDict[abs(self.offExcDF_stockDict.RETAIL) > 0].index, 'reasigned_retail'] = 1
        self.offExcDF_stockDict["DEPTH"] = self.offExcDF_stockDict["ASK_SIZE"] + self.offExcDF_stockDict["BID_SIZE"]
        self.offExcDF_stockDict["QSPREAD_PERCENTAGE"] = self.offExcDF_stockDict["QSPREAD"] / self.offExcDF_stockDict["MID_POINT"]
        self.offExcDF_stockDict["ESPREAD_PERCENTAGE"] = self.offExcDF_stockDict["ESPREAD"] / self.offExcDF_stockDict["MID_POINT"]
        self.offExcDF_stockDict["RSPREAD_PERCENTAGE"] = self.offExcDF_stockDict["RSPREAD"] / self.offExcDF_stockDict["MID_POINT"]
        self.offExcDF_stockDict["PRICEIMPROVEMENT_PERCENTAGE"] = (self.offExcDF_stockDict["QSPREAD"] - self.offExcDF_stockDict["ESPREAD"]) / self.offExcDF_stockDict["MID_POINT"]
        self.offExcDF_stockDict["PRICEIMPACT_PERCENTAGE"] = (self.offExcDF_stockDict["ESPREAD"] - self.offExcDF_stockDict["RSPREAD"]) / self.offExcDF_stockDict["MID_POINT"]
        self.offExcDF_stockDict["DOLLAR_SIZE"] = self.offExcDF_stockDict["PRICE"] * self.offExcDF_stockDict["SIZE"]
        
        #test
#         print(2)
#         print("elapsed: ", time.time()-t0, "sec")
        
        # temp for debug
#         df_aggregated = self.agg_30min(self.offExcDF_stockDict)
#         df_aggregated = pd.DataFrame.from_dict(df_aggregated).sort_values(["Time", "Is_Retail"], 
#                                                                           ascending=[True, True])
#         df_aggregated.to_csv(storePath + df_aggregated.Symbol.iloc[0].replace("::", "__") + "_agg30min_noflip.csv", 
#                              index = False)
        
        # for each day:
        #     compute change rate
        #     return change index
        def random_flip_per_stock_daily(begin_end_datetime_stock_symbol):
            dayBegin = begin_end_datetime_stock_symbol[0]
            dayEnd = begin_end_datetime_stock_symbol[1]
            
            DFdaily_off = self.offExcDF_stockDict #[DFdaily_cond_off]
            DFdaily_cond_off = (dayBegin <= DFdaily_off.Time) & (DFdaily_off.Time < dayEnd)
            DFdaily_off = DFdaily_off[DFdaily_cond_off]
                            
            DFdaily_off_nonRetail = DFdaily_off[abs(DFdaily_off.RETAIL) == 0]
            
            dailyTotal_off = len(DFdaily_off)

            if dailyTotal_off == 0:
                return ([])

            # compute change rate #### question: zero at day?
            nb_off_retail = len(DFdaily_off[abs(DFdaily_off.RETAIL) > 0])

        #     if nb_off_retail == 0:
        #         return (symb, [])

            try:
                stockRb = self.computeRb(nb_off_retail, dailyTotal_off)
            except Exception as err:
                print(err)
                print(dayBegin)
                print(dayEnd)
                print("stockRb")

            try:
                # verbose = False
                # if (str(dayBegin) == "2019-05-31 09:30:00"):
                    # verbose = True
                stockRa = self.computeRa(self.bjzz1, self.bjzz2, stockRb)
            except Exception as err:
                print(err)
                print(dayBegin)
                print(dayEnd)
                print("stockRa")

            try:
                perStockFlipRate = self.computeFlipRate(stockRa, stockRb)
            except Exception as err:
                print(err)
                print(dayBegin)
                print(dayEnd)
                print("perStockFlipRate")
                print(stockRa, stockRb)

        #     nb_change = round(rate * nb_non-retail)
            nb_change = round(perStockFlipRate * (dailyTotal_off - nb_off_retail))
            
            # print( '  '.join([ str(it) for it in [dayBegin, dayEnd, stockRb, stockRa, perStockFlipRate, dailyTotal_off, nb_off_retail, nb_change]]))

            # flip "PRICE" == "MID_POINT" first --- remove this due to rule change
            # price_mid_cond_off = (DFdaily_off.PRICE == DFdaily_off.MID_POINT)
            # DFpriceMid_off = DFdaily_off[price_mid_cond_off]
            # changingIndex = []
            # len_mid_off = len(DFpriceMid_off)
            # if nb_change <= len_mid_off:
            #     changingIndex += [DFpriceMid_off.sample(n=nb_change, random_state=__randomState).index]
            # else:
            #     changingIndex += [DFpriceMid_off.index]
            #     DFpriceNotMid_off = DFdaily_off[DFdaily_off.PRICE != DFdaily_off.MID_POINT]
            #     changingIndex += [DFpriceNotMid_off.sample(n=nb_change - len_mid_off, 
            #                                                random_state=__randomState).index]
            
            # if nb_change == -6:
                # print(dayBegin, dayEnd, stockRb, stockRa, perStockFlipRate, dailyTotal_off, nb_off_retail, nb_change)
                
            changingIndex = DFdaily_off_nonRetail.sample(n=nb_change, random_state=self.__randomState).index.to_list()
            
            # try:
                # changingIndex = DFdaily_off.sample(n=nb_change, random_state=self.__randomState).index.to_list()
                # print(type(changingIndex))
            # except:
                # print("nb_change", nb_change)
            
            return (changingIndex)
        
        # on daily basis compute change rate and reaisgn.
        # map datetime to daily
        # date start
        # date end
        # map everyday and every stock
        datetimeMap_daily_stock = []
        # each day
        curDate = self.minDate
        while curDate <= self.maxDate:
            dayBegin = pd.Timestamp.combine(curDate, self.timeInt[0]) 
            dayEnd = pd.Timestamp.combine(curDate, self.timeInt[-1])
            datetimeMap_daily_stock.append([dayBegin, dayEnd])
            curDate += pd.Timedelta(days=1)
        
        mte = MultiThreadExecutor(random_flip_per_stock_daily)
        change_indexes = mte.calculateParallel(datetimeMap_daily_stock, self.nb_workers)
        del mte
        
        # remove empty index returns
        change_inds = []
        for ind_list in change_indexes:
            for l in ind_list:
                change_inds.append(l)
                    
        # Do change
#         for ind in change_inds:
        self.offExcDF_stockDict.loc[change_inds, 'reasigned_retail'] = 1
            
        # combine
        #     retail     combined with  retail
        #     non-retail combined with  non-retail
        
        #test
#         print(3)
#         print("elapsed: ", time.time()-t0, "sec")
        
        df_aggregated = self.agg_30min(self.offExcDF_stockDict)
        df_aggregated = pd.DataFrame.from_dict(df_aggregated).sort_values(["Time", "Is_Retail"], 
                                                                          ascending=[True, True])
        
        if len(df_aggregated) > 0:
            df_aggregated.to_csv(storePath + df_aggregated.Symbol.iloc[0].replace("::", "__") + "_agg30min.csv", index = False)
            return 0
        else:
            return 1

