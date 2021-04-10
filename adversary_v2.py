import pandas as pd
import os
import datetime
import re
import numpy as np
import math

'''
TODO:
1. make transaction fee a function of transactions per day (if >= 10 transactions/day, fee is $1/transaction)
2. simulate saturation using slabs of %1 DCV 
'''

PRED_COL = ['day','time','act','quant','shares','of','symbol']
STREAM_COL = ['symbol','time','price','change','%%ch','volume','open','high','low','bid','ask']


# https://stackoverflow.com/a/16870699
def vali_date(date_text):
    try:
        datetime.datetime.strptime(date_text, '%Y-%m-%d')
    except ValueError:
        return False
        #raise ValueError("Incorrect data format, should be YYYY-MM-DD")
    return True
    
def vali_time(time_text):
    try:
        datetime.datetime.strptime(time_text, '%H:%M')
    except ValueError:
        #raise ValueError("Incorrect time format, should be HH:MM")
        return False
    return True

def make_dt(date, time):
    d_arr = date.split("-")
    t_arr = time.split(":")
    if(not vali_date(date) or not vali_time(time) or len(d_arr) != 3 or len(t_arr) != 2):
        return None
    return datetime.datetime(*(d_arr + t_arr))

# converts transaction type (buy/sell) to corresponding streaming column name
def ttype_to_scol(ttype):
    if(ttype == 'buy'): return 'ask'
    elif (ttype == 'sell'): return 'bid'
    else: return 'ERROR'

# increases or decreases prices proportional to price-dcv ratio
# dir=1 implies buying because price increases; dir=-1 implies selling
def simulate_saturation(price, dcv, dir, row, day_tbl):
    if dir != -1 and dir != 1: raise ValueError("dir is neither -1 or 1")
    per_inc = float(price) / dcv
    if per_inc >= .01: print(round(per_inc * 100, 3),"% of daily cash volume")
    
    temp = day_tbl.groupby('symbol').get_group(row['symbol'])
    last_t = temp.iloc[(np.searchsorted(temp.time, row['time']) - 1).clip(0)]
    
    if is_number(last_t['ask']): ask = last_t['ask']
    else: ask = last_t['price']
        
    if is_number(last_t['bid']): bid = last_t['bid']
    else: bid = last_t['price']
        
    penalty = abs(ask-bid)/2
    perc = round(per_inc * 100, 3)
    slab = 0
    totalprice = 0
    while slab < math.floor(perc): 
        totalprice += (float(price)/perc + dir*slab*penalty)
        slab += 1
    
    totalprice += (float(price)/perc) * (perc - slab) + dir*slab*penalty
    
    return round(totalprice, 2)
    #return round(price * (1.0 + dir * float(price) / dcv),2)
    
def is_number(x):
    return (type(x) == int or float) and not math.isnan(x)

def format_day(day):
    temp = str(day)
    if len(temp) == 1: temp = "0" + temp
    return temp
    
class Adversary:
    #PRED_COL = ['date','time','type','quant','symbol']
    PRED_COL = ['day','time','act','quant','shares','of','symbol']
    STREAM_COL = ['symbol','time','price','change','%%ch','volume','open','high','low','bid','ask']
    
    def __init__(self, tradefn, dataroot, startcash):
        self.tradefn = tradefn
        self.droot = dataroot # root directory points to a month
        self.df = None
        self.cash = startcash
        self.value = startcash
        self.tfee = 10
        self.dailytrns = 0
        self.stocks = {}
        self.pull_trades()
        
    # Populates dataframe with transactions from predictor
    def pull_trades(self):
        del self.df
        self.df = pd.read_csv(self.tradefn,
                              #sep=" ", 
                              header=None, 
                              names=PRED_COL,
                              delim_whitespace=True)
        # if(not(vali_date(self.df['date']) 
               # and vali_time(self.df['time'])
               # and self.df['type'].isin(['buy','sell'])
               # and self.df['quant'].isnumeric())
               # and ):
            # del self.df
            # raise ValueError("Invalid entry/entries in " + self.tradefn)
        #TODO: check that the datetimes of the pandas dateframe are monotonically non-decreasing
        #pattern = re.compile("^\d+:\d+[AP]M$")
        
        # convert time column into datetime objects (for easier searching)
        # https://stackoverflow.com/a/51235728
        self.df['time'] = pd.to_datetime(self.df['time'], format='%H:%M')#'%I:%M%p')
        #print(self.df)
    
    def get_dir_from_day(self, day):
        #return os.path.join(self.droot, date.split("-")[2], "streaming")
        return os.path.join(self.droot, format_day(day), "streaming.tsv")
    
    # TODO
    def most_recent_trans(self, row, day_tbl, ttype):
        temp = day_tbl.groupby('symbol').get_group(row['symbol'])
        last_t = temp.iloc[(np.searchsorted(temp.time, row['time']) - 1).clip(0)]
        if last_t['time'] > row['time']:
            print("TRADING ERROR: Trade of "+ row['symbol'] + " "
                + "ordered at " + str(row['time']) + " "
                + "but first quote is " + str(last_t['time']) + "; "
                + "trade will occur at " + str(last_t['time']))
            #temp = last_t
        if ttype == 1 and is_number(last_t['ask']):
            return last_t['ask']
        elif ttype == -1 and is_number(last_t['bid']):
            return last_t['bid']
        return last_t['price']
    
    # for each transaction
    #     get daily cash volume for stock on target day
    #    if buying, get most recent asking price prior to target time
    #    if selling, get most recent selling price prior to target time
    #    compute saturated transaction price & multiply by desired quantity of stocks
    #    if buying, check whether user has enough money to purchase
    #    if selling, check whether user has enough stocks to sell
    #    if either case fails, throw an exception
    #    otherwise, complete transaction and move onto next transaction
    def do_transactions(self):
        day_tbl = None
        curr_day = None
        dcv = 0.0
        for index, row in self.df.iterrows():
            # update day_tbl if undeclared or if transaction date didnt change
            if day_tbl is None or curr_day != row['day']: 
                # update current date of day_tbl
                curr_day = row['day']
                # update day_tbl
                day_tbl = pd.read_csv(self.get_dir_from_day(row['day']), 
                                      #sep=" ", 
                                      header=None,
                                      skiprows=[0],
                                      names=STREAM_COL,
                                      delim_whitespace=True)
                #print(day_tbl.head(10))
                
                # replace start-of-day entries with 00:00
                pattern = re.compile("^[A-Za-z]+\d+$")
                #print(pattern.search(str(day_tbl['time']))
                #day_tbl.loc[,'time'] = '00:00AM'
                
                day_tbl['time'] = pd.to_datetime(day_tbl['time'], format='%H:%M')
                # Update total number of transactions executed in a day.
                self.dailytrns = 1
                self.tfee = 10
            else:
                self.dailytrns += 1
                                

            # change transaction fee if daily transactions exceed 10.
            if self.tfee == 10 and self.dailytrns >= 10:
                self.tfee = 1
                self.cash += 90
            
            print("trans fee: " + str(self.tfee) + "nums: " + str(self.dailytrns))
            # compute daily cash volume for current transaction's ticker symbol
            final_row = day_tbl.groupby('symbol').get_group(row['symbol']).iloc[-1]
            dcv = final_row.price*final_row.volume
            
            # transaction type: 1 for buy, -1 for sell
            ttype = 1 if row['act']=='buy' else -1
            # get the bid or ask of the most the recent recorded transaction
            price = self.most_recent_trans(row, day_tbl, ttype)
            print("price of ",row['symbol']," is ",price," at time ",row['time'])
            # simulate saturation to punish larger transactions
            subtot = simulate_saturation(price*row['quant'], dcv, ttype, row, day_tbl)
            
            # ensure enough cash and/or stocks to complete transaction
            if ttype == 1 and self.cash < subtot + self.tfee:
                raise ValueError("not enough cash to purchase " + row)
            elif ttype == -1 and (row['symbol'] not in self.stocks or self.stocks[row['symbol']][1] < row['quant']):
                raise ValueError("not enough stocks to sell " + row)
            elif ttype == -1 and self.cash < self.tfee:
                raise ValueError("not enough cash to pay for transaction fee")
            
            # update cash, stocks, and portfolio value
            self.cash -= ttype*subtot + self.tfee# cash lowers when buying (ttype=1)
            if ttype == 1:
                # assumes that each stock is sold completely before being bought again
                self.stocks[row['symbol']] = (price, row['quant'])
                self.value -= self.tfee
                print(str(row['day']) + " " 
                    + str(row['time']) + " " 
                    + str(row['quant']) + " " 
                    + row['symbol'] + " "
                    + "bought at $" + str(price) + "/share; "
                    + "cash spent $" + str(subtot) + "; "
                    + "cash balance $" + "{:.2f}".format(self.cash))
                    #+ "; account value $" + self.value)
            else: # if selling, update value and stocks
                self.value += subtot - self.stocks[row['symbol']][0] * self.stocks[row['symbol']][1]
                self.stocks[row['symbol']] = ()
                print(str(row['day']) + " " 
                    + str(row['time']) + " " 
                    + str(row['quant']) + " " 
                    + row['symbol'] + " "
                    + "sold at $" + str(price) + "/share; "
                    + "cash acquired $" + str(subtot) + "; "
                    + "cash balance $" + "{:.2f}".format(self.cash))
                    #+ "; account value $" + self.value)

# inputs: transactions file path, directory to month of streaming data (.tsv files)
a = Adversary(r".\test_input.txt",
              r"",
              10000000)
a.do_transactions()
