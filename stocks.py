import urllib2, urllib
import json, csv
import pprint as pp
import random
import time
from datetime import datetime, timedelta
import os, re, sys
from boto.s3.connection import S3Connection
from boto.s3.key import Key
import boto


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
    now = datetime.now() + timedelta(hours=-5)
    cur_hour = now.hour
    return hourdict.get(cur_hour)

dirname, filename = os.path.split(os.path.abspath(__file__))

base_uri = "http://query.yahooapis.com/v1/public/yql?"

# define some stocks
stocks = [line.strip() for line in open(dirname + '/tickers.txt').read().split('\n')]
#encapsulate for the query
stocks = ["'" + stock + "'" for stock in stocks]

with open(dirname + '/tickers_funds.csv', 'rU') as funds:
    FundReader = csv.reader(funds)
    FundDict = dict((rows[0],rows[1]) for rows in FundReader)

random.shuffle(stocks)

cur_date = datetime.now() #+ timedelta(hours=8)
time_stamp = str(cur_date)
year = str(cur_date.year)
month = str(cur_date.month)
day = str(cur_date.day)
hour = str(cur_date.hour)
date_plug = 'y='+year+'/m='+month+'/d='+day+'/h='+hour+'/'
#ubuntu_filename = '/Users/admin/Desktop/stockdata_'+time_stamp+'.csv'
ubuntu_filename = '/home/ubuntu/repo/flatfiles/stockdata_'+time_stamp+'.csv'
s3_filename = 'stockdata/'+date_plug+'stockdata_'+time_stamp+'.csv'

f = open(ubuntu_filename, 'wb')
#f = open('/Users/admin/Desktop/Demo_Data/TickerTracker/Stock_Data/stockdata_'+time_stamp+'.csv', 'wb')
w = csv.writer(f)
columns = [u'AfterHoursChangeRealtime', u'Ask', u'AskRealtime', u'AverageDailyVolume', u'Bid', u'BidRealtime', u'BookValue', u'Change', u'ChangeFromYearHigh', u'ChangeFromYearLow', u'ChangePercentRealtime', u'ChangeRealtime', u'ChangeinPercent', u'DaysHigh', u'DaysLow', u'DaysRange', u'DaysValueChange', u'DividendShare', u'DividendYield', u'EBITDA', u'EarningsShare', u'ErrorIndicationreturnedforsymbolchangedinvalid', u'FiftydayMovingAverage', u'LastTradePriceOnly', u'MarketCapRealtime', u'MarketCapitalization', u'Name', u'Open', u'PEGRatio', u'PERatio', u'PercebtChangeFromYearHigh', u'PercentChange', u'PercentChangeFromTwoHundreddayMovingAverage', u'PercentChangeFromYearLow', u'PreviousClose', u'PriceBook', u'PricePaid', u'ShortRatio', u'StockExchange', u'Symbol', u'TradeDate', u'TwoHundreddayMovingAverage', u'Volume', u'YearHigh', u'YearLow', 'datestamp', 'timestamp', 'funds', 'dayofweek', 'hourofday']

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
        est_date = datetime.now() + timedelta(hours=-5) #offset assumes AWS uses UTC
        quote['timestamp'] = int(cur_time)
        quote['datestamp'] = str(est_date)
        
        #Add 401k plan fund names to each relevant row.
        quote['funds'] = FundDict.get(quote['Symbol'])
        pp.pprint(quote)
        w.writerow([quote.get(col) for col in columns])
        print "*"*80

f.close()

#Import s3 credentials from ubuntu directory
cred_file = open('/home/ubuntu/keys/s3_creds_mmx.json')
creds = json.load(cred_file)
AWS_ACCESS_KEY_ID = creds['aws_access_key_id']
AWS_SECRET_ACCESS_KEY = creds['aws_secret_access_key']
cred_file.close()

#write files to s3 bucket
s3 = boto.connect_s3(AWS_ACCESS_KEY_ID,AWS_SECRET_ACCESS_KEY)
bucket = s3.get_bucket('metamx-shecht')
key = bucket.new_key(s3_filename)
key.set_contents_from_filename(ubuntu_filename)

#delete file from ubuntu after saving it to s3
os.unlink(ubuntu_filename)
