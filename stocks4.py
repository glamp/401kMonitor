#http://stackoverflow.com/questions/1301493/python-timezones
import urllib2, urllib
import json
import csv
import pprint as pp
import random
import time, date
from datetime import datetime, timedelta
import os
import re

def convert_dataypes(x):
    try:
        return float(re.sub('[$-+]', '', x))
    except Exception, e:
        return x

def get_json(url):
    try:
        src = urllib2.urlopen(url).read()
        rsp = json.loads(src)
    except:
        rsp = {}
    return rsp

def get_weekday():
    daydict = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'}
    now = datetime.now() + timedelta(hours=3)
    today = now.isoweekday()
    return daydict.get(today)

def get_hour():
    hourdict = {0: '0:00-1:00', 1: '1:00-2:00', 2: '2:00-3:00', 3: '3:00-4:00', 4: '4:00-5:00', 5: '5:00-6:00',
                6: '6:00-7:00', 7: '7:00-8:00', 8: '8:00-9:00', 9: '9:00-10:00', 10: '10:00-11:00', 11: '11:00-12:00', 
                12: '12:00-13:00', 13: '13:00-14:00', 14: '14:00-15:00', 15: '15:00-16:00', 16: '16:00-17:00', 17: '17:00-18:00', 
                18: '18:00-19:00', 19: '19:00-20:00', 20: '20:00-21:00', 21: '21:00-22:00', 22: '22:00-23:00', 23: '23:00-24:00'}
    now = datetime.now() + timedelta(hours=3)
    cur_hour = now.hour
    return hourdict.get(cur_hour)

dirname, filename = os.path.split(os.path.abspath(__file__))

APIKEY = "6YRmn23V34GpcTPULtAmjh.hzXNAh1bcGEL0bl5p6EwUqm25o_FyQZyQrCxisg--"

base_uri = "http://query.yahooapis.com/v1/public/yql?"

# define some stocks
stocks = [line.strip() for line in open(dirname + '/tickers.txt').read().split('\n')]
#encapsulate for the query
stocks = ["'" + stock + "'" for stock in stocks]

with open(dirname + '/tickers_funds.csv', 'rU') as funds:
    FundReader = csv.reader(funds)
    FundDict = dict((rows[0],rows[1]) for rows in FundReader)

random.shuffle(stocks)

time_stamp = str(datetime.now())

f = open('/Users/admin/Desktop/Demo_Data/TickerTracker/Stock_Data/stockdata_'+time_stamp+'.csv', 'wb')
w = csv.writer(f)
columns = [u'AfterHoursChangeRealtime', u'AnnualizedGain', u'Ask', u'AskRealtime', u'AverageDailyVolume', u'Bid', u'BidRealtime', u'BookValue', u'Change', u'ChangeFromFiftydayMovingAverage', u'ChangeFromTwoHundreddayMovingAverage', u'ChangeFromYearHigh', u'ChangeFromYearLow', u'ChangePercentRealtime', u'ChangeRealtime', u'Change_PercentChange', u'ChangeinPercent', u'Commission', u'DaysHigh', u'DaysLow', u'DaysRange', u'DaysRangeRealtime', u'DaysValueChange', u'DaysValueChangeRealtime', u'DividendPayDate', u'DividendShare', u'DividendYield', u'EBITDA', u'EPSEstimateCurrentYear', u'EPSEstimateNextQuarter', u'EPSEstimateNextYear', u'EarningsShare', u'ErrorIndicationreturnedforsymbolchangedinvalid', u'ExDividendDate', u'FiftydayMovingAverage', u'HighLimit', u'HoldingsGain', u'HoldingsGainPercent', u'HoldingsGainPercentRealtime', u'HoldingsGainRealtime', u'HoldingsValue', u'HoldingsValueRealtime', u'LastTradeDate', u'LastTradePriceOnly', u'LastTradeRealtimeWithTime', u'LastTradeTime', u'LastTradeWithTime', u'LowLimit', u'MarketCapRealtime', u'MarketCapitalization', u'MoreInfo', u'Name', u'Notes', u'OneyrTargetPrice', u'Open', u'OrderBookRealtime', u'PEGRatio', u'PERatio', u'PERatioRealtime', u'PercebtChangeFromYearHigh', u'PercentChange', u'PercentChangeFromFiftydayMovingAverage', u'PercentChangeFromTwoHundreddayMovingAverage', u'PercentChangeFromYearLow', u'PreviousClose', u'PriceBook', u'PriceEPSEstimateCurrentYear', u'PriceEPSEstimateNextYear', u'PricePaid', u'PriceSales', u'SharesOwned', u'ShortRatio', u'StockExchange', u'Symbol', u'TickerTrend', u'TradeDate', u'TwoHundreddayMovingAverage', u'Volume', u'YearHigh', u'YearLow', u'YearRange', 'datestamp', 'timestamp', 'funds', 'dayofweek', 'hourofday']

w.writerow(columns)

for block in range(0, len(stocks), 150):
    stocks_subset = stocks[block:block+150]
    # define the parameters
    query = {
        "q":"select * from yahoo.finance.quotes where symbol in (%s)" % ', '.join(stocks_subset),
        "env":"http://datatables.org/alltables.env",
        "format":"json"
    }

    # create the rest request
    url = base_uri + urllib.urlencode(query)

    print url


    rsp = get_json(url)
    quotes = []
    if 'query' in rsp and \
        'results' in rsp['query']\
         and 'quote' in rsp['query']['results']:
        quotes = rsp['query']['results']['quote']

    for quote in quotes:
        for col in quote:
            quote[col] = convert_dataypes(quote[col])

        #Add day and time columns
        quote['hourofday'] = str(get_hour())
        quote['dayofweek'] = str(get_weekday())
        cur_time = time.time()
        cur_date = datetime.now() + timedelta(hours=3)
        quote['timestamp'] = int(cur_time)
        quote['datestamp'] = str(cur_date)
        
        #Add 401k plan fund names to each relevant row.
        quote['funds'] = FundDict.get(quote['Symbol'])
        pp.pprint(quote)
        w.writerow([quote.get(col) for col in columns])
        print "*"*80

f.close()