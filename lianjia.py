#-*- coding:utf-8 -*-
"""
Get lianjia historical trades data from its website and put into Postgresql
"""
import requests
import json
import psycopg2
import sys
import datetime
import argparse
from dateutil.relativedelta import *
from lxml import etree

class ObseleteItemError(Exception):
    def __init__(self, value):
        self.value = value
    def __str__(self):
        return repr(self.value)

url_pre_login = 'https://passport.lianjia.com/cas/prelogin/loginTicket?'
url_login = 'https://passport.lianjia.com/cas/login'
url_getusrinfo = 'http://login.lianjia.com/login/getUserInfo/'
url_dict = 'http://www.lianjia.com/api/getCityDict?city_id=110000&aggregation=1.html'
url_analysis_day = 'http://bj.lianjia.com/fangjia/priceTrend//?analysis=1&duration=day'
url_analysis_month = 'http://bj.lianjia.com/fangjia/priceTrend//?analysis=1'
url_price_trend = 'http://bj.lianjia.com/fangjia/priceTrend/'
usr = ''
pwd = ''
startdate= ''
conn = psycopg2.connect(database="RealAsset", user="postgres", password="Wcp181114", host="localhost", port="5432")
cur = conn.cursor()

def date_format_check(date):
    list1 = ''.join(date).split('.')
    idx = 0
    year = month = day = 1
    for i in list1:
        if (idx == 0):
            year = i
        elif (idx == 1):
            month = i
        elif (idx == 2):
            day = i
        idx += 1
    date = "%s.%s.%s" % (year, month, day)
    return date

def write_chengjiao_item_to_db(houseinfo):
    order = "INSERT INTO lianjia_trades(\"Date\", \"Community\", \"Layout\", \"Square\", \"Direction\", \"Floor\",\
        \"Building\", \"Introduce\", \"Pricepersquare\", \"Totalprice\", \"Link\")\
        VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')" % (
        houseinfo['date'], houseinfo['community'].encode('utf-8'), houseinfo['layout'].encode('utf-8'), \
        houseinfo['square'].encode('utf-8'), houseinfo['direction'].encode('utf-8'), houseinfo['floor'].encode('utf-8'), \
        houseinfo['building'].encode('utf-8'), houseinfo['introduces'].encode('utf-8'), houseinfo['pricesquare'],
        houseinfo['totalprice'], houseinfo['link'].encode('utf-8'))

    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if(err.find('duplicate key value') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
            sys.exit(-1)
    conn.commit()

def write_ad_to_db(adinfo):
    order = "INSERT INTO lianjia_analysis_day(\"Date\", \"HouseAmount\", \"CustomerAmount\", \"ShowAmount\")\
        VALUES (\'%s\', \'%s\', \'%s\', \'%s\')" % (
        adinfo['date'], adinfo['houseAmount'],adinfo['customerAmount'], adinfo['showAmount'])
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if(err.find('duplicate key value') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
            sys.exit(-1)
    conn.commit()

def write_am_to_db(aminfo):
    order = "INSERT INTO lianjia_analysis_month(\"Date\", \"HouseAmount\", \"CustomerAmount\", \"ShowAmount\")\
        VALUES (\'%s\', \'%s\', \'%s\', \'%s\')" % (
        aminfo['date'], aminfo['houseAmount'],aminfo['customerAmount'], aminfo['showAmount'])
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if(err.find('duplicate key value') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
            sys.exit(-1)
    conn.commit()

def extract_single_chengjiao_page(selector):
    items = selector.xpath('/html/body/div[6]/div[2]/div[2]/div[3]/ul/li')
    for item in items:
        houseinfo = {}

        title = item.xpath('div[2]/h2/a/text()')
        if not title:
            title = item.xpath('div[2]/h2/div/text()')
        if not title:
            print "Error when extract single chengjiao page."
            continue
        houseinfo['date'] = ''.join(item.xpath('div[2]/div/div[2]/div/div[1]/div/text()'))
        houseinfo['date'] = date_format_check(houseinfo['date'])
        d1 = datetime.datetime.strptime(houseinfo['date'],'%Y.%m.%d').date()
        if(d1 < datetime.datetime.strptime(startdate,'%Y-%m-%d').date()):
            raise ObseleteItemError("obselete item met")

        houseinfo['community'] = houseinfo['layout'] = houseinfo['square'] = ' '
        list1 = ''.join(title).split(' ')
        idx = 0
        for i in list1:
            if(idx == 0):
                houseinfo['community'] = i
            elif(idx == 1):
                houseinfo['layout'] = i
            elif(idx == 2):
                houseinfo['square'] = i
            idx += 1

        #houseinfo['community'],houseinfo['layout'],houseinfo['square'] = ''.join(title).split(' ',2)
        houseinfo['square'] = filter(str.isdigit, houseinfo['square'].encode('utf-8'))
        houseinfo['link'] = ''.join(item.xpath('div[2]/h2/a/@href'))

        other = item.xpath('div[2]/div/div[1]/div[1]/div/text()')
        houseinfo['direction'] = houseinfo['floor'] = houseinfo['building'] = ' '
        list1 = ''.join(other).split('/')
        idx = 0
        for i in list1:
            if (idx == 0):
                houseinfo['direction'] = i
            elif (idx == 1):
                houseinfo['floor'] = i
            elif (idx == 2):
                houseinfo['building'] = i
            idx += 1
        #houseinfo['direction'], houseinfo['floor'], houseinfo['building'] = ''.join(other).split('/', 2)
        intros = item.xpath('div[2]/div/div[1]/div[2]/span')
        introduces = ''
        for itro in intros:
            introduces += itro.text
        houseinfo['introduces'] = introduces
        houseinfo['pricesquare'] = ''.join(item.xpath('div[2]/div/div[2]/div/div[2]/div/text()'))
        if(houseinfo['pricesquare'] == ''):
            houseinfo['pricesquare'] = '0'
        houseinfo['totalprice'] = ''.join(item.xpath('div[2]/div/div[2]/div/div[3]/div/text()'))

        if(houseinfo['link'] == '' and (houseinfo['pricesquare'] == '0' or houseinfo['pricesquare'] == '--')):
            continue
        write_chengjiao_item_to_db(houseinfo)
    return

def lianjia_login():
    lt = requests.get(url_pre_login)
    ltcont = lt.content
    pre_cookie = lt.cookies
    ltjson = json.loads(ltcont)
    header = {
        'Accept':'*/*',
        'Accept-Encoding':'gzip, deflate',
        'Accept-Language':'en-US,en;q=0.8,zh-CN;q=0.6',
        'Connection':'keep-alive',
        'Content-Type':'application/x-www-form-urlencoded',
        'Host':'passport.lianjia.com',
        'Origin':'https://passport.lianjia.com',
        'Referer':'https://passport.lianjia.com/cas/xd/api?name=passport-lianjia-com',
        'User-Agent':'Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/49.0.2623.112 Safari/537.36',
        'X-Requested-With':'XMLHttpRequest'
        }
    data = {
        'username':usr,
        'password':pwd,
        'verifycode':'',
        'service':'http://bj.lianjia.com/chengjiao/',
        'isajax':'true',
        'code':'',
        'lt':ltjson['data']
        }
    loginr = requests.post(url_login, data=data, headers=header, cookies=pre_cookie)
    lcookies = loginr.cookies
    st = loginr.content
    stjson = json.loads(st)
    payload = {'service': 'http://bj.lianjia.com/chengjiao/', 'st': stjson['ticket']}
    r = requests.get(url_getusrinfo, params=payload, cookies=lcookies)
    global lianjia_cookies
    lianjia_cookies = r.cookies

def get_lianjia_chengjiao():
    #get circle dict
    dict = requests.get(url_dict, cookies=lianjia_cookies).content
    pdata = json.loads(dict)
    circles = set()
    for district in pdata['data']['district']:
        for bizcircle in district['bizcircle']:
            for bc_data in bizcircle['data']:
                circles.add(bc_data['bizcircle_quanpin'])

    #fh = open('c:\\temp\\log.txt', 'at')
    #grab historical trades
    err_urls = set()
    for circle in circles:
        for sidx in range(1,9):
            for lidx in range(1,7):
                for pg in range(1,101):
                    url_page = 'http://bj.lianjia.com/chengjiao/'+circle+'/pg'+str(pg)+'l'+str(lidx)+'a'+str(sidx)
                    #url_page = 'http://bj.lianjia.com/chengjiao/dongsi1/pg1a7'
                    content = requests.get(url_page, cookies=lianjia_cookies).content
                    print "Started %s" % url_page
                    try:
                        nselector = etree.HTML(content)
                        totalpage = json.loads(nselector.xpath('/html/body/div[6]/div[2]/div[2]/div[3]/div')[0].attrib['page-data'])['totalPage']
                        extract_single_chengjiao_page(nselector)
                    except IndexError, e:
                        #check if really no content to show
                        other = ''.join(nselector.xpath('/html/body/div[6]/div[2]/div[2]/div[3]/ul/li/p/text()'))
                        if(other.find(u"没有找到相关内容") > 0):
                            print "No further content to show for %s" % url_page
                            break
                        else:
                            err = "Index error for %s" % url_page
                            print err
                            err_urls.add(url_page)
                            continue
                        #fh.write('###########################################################################################################################################################################')
                        #fh.write(content)
                    except ObseleteItemError, e:
                        print "Obselete item met in %s, ignore latter pages" % url_page
                        break
                    except Exception, e:
                        err = "Other error for %s" % url_page
                        print err
                        err_urls.add(url_page)
                        continue
                    print "Finished %s" % url_page
                    if(pg >= totalpage):
                        break
    #fh.close()
    #retry for those pages in error
    while(err_urls):
        for page in err_urls:
            content = requests.get(page, cookies=lianjia_cookies).content
            print "Retry for %s" % page
            try:
                nselector = etree.HTML(content)
                totalpage = json.loads(nselector.xpath('/html/body/div[6]/div[2]/div[2]/div[3]/div')[0].attrib['page-data'])['totalPage']
            except IndexError, e:
                # check if really no content to show
                other = ''.join(nselector.xpath('/html/body/div[6]/div[2]/div[2]/div[3]/ul/li/p/text()'))
                if (other.find(u"没有找到相关内容") > 0):
                    print "No further content to show for %s" % page
                    continue
                else:
                    err = "Index error for %s" % page
                    print err
                    continue
            except Exception, e:
                err = "Other error for %s" % page
                print err
                continue
            extract_single_chengjiao_page(nselector)
            print "Finished %s" % page
            err_urls.remove(page)

def get_analysis_day():
    content = requests.get(url_analysis_day, cookies=lianjia_cookies).content
    try:
        addata = json.loads(content)
    except Exception, e:
        err = "Exception for %s" % url_analysis_day
        print err

    print "Started %s" % url_analysis_day
    # get start date
    year = month = day = 0
    year = addata['time']['year']
    month = filter(str.isdigit,addata['time']['month'].encode('utf-8'))
    day = filter(str.isdigit,addata['time']['day'].encode('utf-8'))
    stdate = datetime.datetime(int(year), int(month), int(day))
    adinfo = {}
    idx = 0
    for h in addata['houseAmount']:
        adinfo['date'] = stdate + datetime.timedelta(days = idx)
        adinfo['houseAmount'] = h
        adinfo['customerAmount'] = addata['customerAmount'][idx]
        adinfo['showAmount'] = addata['showAmount'][idx]
        idx += 1
        write_ad_to_db(adinfo)
    print "Finished %s" % url_analysis_day

def get_analysis_month():
    content = requests.get(url_analysis_month, cookies=lianjia_cookies).content
    try:
        amdata = json.loads(content)
    except Exception, e:
        err = "Exception for %s" % url_analysis_month
        print err

    print "Started %s" % url_analysis_month
    # get start date
    year = month = day = 0
    year = int(amdata['time']['year'])
    month = int(filter(str.isdigit,amdata['time']['month'].encode('utf-8')))
    day = 1
    stdate = datetime.datetime(year, month, day)
    aminfo = {}
    idx = 0
    for h in amdata['houseAmount'][::-1]:
        aminfo['date'] = stdate + relativedelta(months=-idx)
        aminfo['houseAmount'] = h
        aminfo['customerAmount'] = amdata['customerAmount'][idx]
        aminfo['showAmount'] = amdata['showAmount'][idx]
        idx += 1
        write_am_to_db(aminfo)
    print "Finished %s" % url_analysis_month

def getArgs():
    parse=argparse.ArgumentParser()
    parse.add_argument('-lu',type=str)
    parse.add_argument('-lp',type=str)
    parse.add_argument('-s',type=str)
    args=parse.parse_args()
    return vars(args)

if __name__ == '__main__':
    args = getArgs()
    usr = args['lu']
    pwd = args['lp']
    startdate = args['s']
    lianjia_login()
    get_lianjia_chengjiao()
    conn.close()

