#-*- coding:utf-8 -*-
"""
Get historical trades data from http://www.bjjs.gov.cn/ and put into Postgresql
"""
import requests
import psycopg2
import sys
from lxml import etree

url_zjw = 'http://www.bjjs.gov.cn/tabid/2167/default.aspx'
conn = psycopg2.connect(database="RealAsset", user="postgres", password="Wcp181114", host="localhost", port="5432")
cur = conn.cursor()

def create_zjw_db_tables():
    order = 'CREATE TABLE public.zjw_sh_house\
            (\
              "Date" date NOT NULL,\
              "Total_signfromnet" integer,\
              "Total_square_signfromnet" real,\
              "House_signfromnet" integer,\
              "House_square_signfromnet" real,\
              "Total_forsell" integer,\
              "Total_square_forsell" real,\
              "House_forsell" integer,\
              "House_square_forsell" real,\
              "Total_new_forsell" integer,\
              "Total_square_new_forsell" real,\
              "House_new_forsell" integer,\
              "House_square_new_forsell" real,\
              CONSTRAINT pk_zjw_sh_house PRIMARY KEY ("Date")\
            )\
            WITH (\
              OIDS=FALSE\
            );\
            ALTER TABLE public.zjw_sh_house\
              OWNER TO postgres;'
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if (err.find('already exist') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
            sys.exit(-1)
    conn.commit()

    order = 'CREATE TABLE public.zjw_fd_house\
            (\
              "Date" date NOT NULL,\
              "Total_subfromnet" integer,\
              "Total_square_subfromnet" real,\
              "House_subfromnet" integer,\
              "House_square_subfromnet" real,\
              "Commercial_subfromnet" integer,\
              "Commercial_square_subfromnet" real,\
              "Office_subfromnet" integer,\
              "Office_square_subfromnet" real,\
              "Carpark_subfromnet" integer,\
              "Carpark_square_subfromnet" real,\
              "Total_signfromnet" integer,\
              "Total_square_signfromnet" real,\
              "House_signfromnet" integer,\
              "House_square_signfromnet" real,\
              "Commercial_signfromnet" integer,\
              "Commercial_square_signfromnet" real,\
              "Office_signfromnet" integer,\
              "Office_square_signfromnet" real,\
              "Carpark_signfromnet" integer,\
              "Carpark_square_signfromnet" real,\
              "Total_forsell" integer,\
              "Total_square_forsell" real,\
              "House_forsell" integer,\
              "House_square_forsell" real,\
              "Commercial_forsell" integer,\
              "Commercial_square_forsell" real,\
              "Office_forsell" integer,\
              "Office_square_forsell" real,\
              "Carpark_forsell" integer,\
              "Carpark_square_forsell" real,\
              CONSTRAINT pk_zjw_fd_house PRIMARY KEY ("Date")\
            )\
            WITH (\
              OIDS=FALSE\
            );\
            ALTER TABLE public.zjw_fd_house\
              OWNER TO postgres;'
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if (err.find('already exist') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
            sys.exit(-1)
    conn.commit()

    order = 'CREATE TABLE public.zjw_existing_house\
        (\
          "Date" date NOT NULL,\
          "Total_subfromnet" integer,\
          "Total_square_subfromnet" real,\
          "House_subfromnet" integer,\
          "House_square_subfromnet" real,\
          "Commercial_subfromnet" integer,\
          "Commercial_square_subfromnet" real,\
          "Office_subfromnet" integer,\
          "Office_square_subfromnet" real,\
          "Carpark_subfromnet" integer,\
          "Carpark_square_subfromnet" real,\
          "Total_signfromnet" integer,\
          "Total_square_signfromnet" real,\
          "House_signfromnet" integer,\
          "House_square_signfromnet" real,\
          "Commercial_signfromnet" integer,\
          "Commercial_square_signfromnet" real,\
          "Office_signfromnet" integer,\
          "Office_square_signfromnet" real,\
          "Carpark_signfromnet" integer,\
          "Carpark_square_signfromnet" real,\
          "Total_forsell" integer,\
          "Total_square_forsell" real,\
          "House_forsell" integer,\
          "House_square_forsell" real,\
          "Commercial_forsell" integer,\
          "Commercial_square_forsell" real,\
          "Office_forsell" integer,\
          "Office_square_forsell" real,\
          "Carpark_forsell" integer,\
          "Carpark_square_forsell" real,\
          CONSTRAINT pk_zjw_fd_house PRIMARY KEY ("Date")\
        )\
        WITH (\
          OIDS=FALSE\
        );\
        ALTER TABLE public.zjw_fd_house\
          OWNER TO postgres;'
    try:
        cur.execute(order)
    except psycopg2.DatabaseError, e:
        err = 'Error %s' % e
        if (err.find('already exist') > 0):
            conn.rollback()
            return
        else:
            print err
            conn.rollback()
            conn.close()
            sys.exit(-1)
    conn.commit()

def write_zjw_sh_to_db(sh):
    order = "INSERT INTO zjw_sh_house(\"Date\", \"Total_signfromnet\", \"Total_square_signfromnet\",\"House_signfromnet\",\
        \"House_square_signfromnet\", \"Total_forsell\", \"Total_square_forsell\",\
        \"House_forsell\", \"House_square_forsell\", \"Total_new_forsell\",\
        \"Total_square_new_forsell\", \"House_new_forsell\",\"House_square_new_forsell\")\
        VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')" % (
        sh['date'], sh['totalsign'], sh['totalsignarea'], sh['housesign'], sh['housesignarea'], sh['totalforsell'],sh['totalforsellarea'], \
        sh['houseforsell'], sh['houseforsellarea'],sh['totalnew'], sh['totalnewarea'], sh['housenew'], sh['housenewarea'])

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

def write_zjw_eh_to_db(eh):
    order = "INSERT INTO zjw_existing_house(\"Date\", \"Total_subfromnet\", \"Total_square_subfromnet\", \"House_subfromnet\",\
            \"House_square_subfromnet\", \"Commercial_subfromnet\", \"Commercial_square_subfromnet\",\
            \"Office_subfromnet\", \"Office_square_subfromnet\", \"Carpark_subfromnet\",\
            \"Carpark_square_subfromnet\", \"Total_signfromnet\", \"Total_square_signfromnet\",\
            \"House_signfromnet\", \"House_square_signfromnet\", \"Commercial_signfromnet\",\
            \"Commercial_square_signfromnet\", \"Office_signfromnet\", \"Office_square_signfromnet\",\
            \"Carpark_signfromnet\", \"Carpark_square_signfromnet\", \"Total_forsell\",\
            \"Total_square_forsell\", \"House_forsell\", \"House_square_forsell\",\
            \"Commercial_forsell\", \"Commercial_square_forsell\", \"Office_forsell\",\
            \"Office_square_forsell\", \"Carpark_forsell\", \"Carpark_square_forsell\")\
        VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\',\
            \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')" % (\
        eh['date'], eh['totalsub'], eh['totalsubarea'], eh['housesub'], eh['housesubarea'], eh['businesssub'],eh['businesssubarea'], \
        eh['officesub'], eh['officesubarea'], eh['parksub'], eh['parksubarea'], eh['totalsign'], eh['totalsignarea'], eh['housesign'], eh['housesignarea'], \
        eh['businesssign'], eh['businesssignarea'], eh['officesign'], eh['officesignarea'],eh['parksign'], eh['parksignarea'], eh['totalforsell'], \
        eh['totalforsellarea'], eh['houseforsell'], eh['houseforsellarea'], eh['businessforsell'], eh['businessforsellarea'], eh['officeforsell'],\
        eh['officeforsellarea'], eh['parkforsell'], eh['parkforsellarea'])

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

def write_zjw_fh_to_db(fh):
    order = "INSERT INTO zjw_fd_house(\"Date\", \"Total_subfromnet\", \"Total_square_subfromnet\", \"House_subfromnet\",\
            \"House_square_subfromnet\", \"Commercial_subfromnet\", \"Commercial_square_subfromnet\",\
            \"Office_subfromnet\", \"Office_square_subfromnet\", \"Carpark_subfromnet\",\
            \"Carpark_square_subfromnet\", \"Total_signfromnet\", \"Total_square_signfromnet\",\
            \"House_signfromnet\", \"House_square_signfromnet\", \"Commercial_signfromnet\",\
            \"Commercial_square_signfromnet\", \"Office_signfromnet\", \"Office_square_signfromnet\",\
            \"Carpark_signfromnet\", \"Carpark_square_signfromnet\", \"Total_forsell\",\
            \"Total_square_forsell\", \"House_forsell\", \"House_square_forsell\",\
            \"Commercial_forsell\", \"Commercial_square_forsell\", \"Office_forsell\",\
            \"Office_square_forsell\", \"Carpark_forsell\", \"Carpark_square_forsell\")\
        VALUES (\'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\',\
            \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\', \'%s\')" % ( \
        fh['date'], fh['totalsub'], fh['totalsubarea'], fh['housesub'], fh['housesubarea'], fh['businesssub'],fh['businesssubarea'], \
        fh['officesub'], fh['officesubarea'], fh['parksub'], fh['parksubarea'], fh['totalsign'], fh['totalsignarea'],fh['housesign'], fh['housesignarea'], \
        fh['businesssign'], fh['businesssignarea'], fh['officesign'], fh['officesignarea'], fh['parksign'],fh['parksignarea'], fh['totalforsell'], \
        fh['totalforsellarea'], fh['houseforsell'], fh['houseforsellarea'], fh['businessforsell'],fh['businessforsellarea'], fh['officeforsell'], \
        fh['officeforsellarea'], fh['parkforsell'], fh['parkforsellarea'])

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

def get_sh_from_zjw(nselector):
    # get date
    sh = {}
    sh['date'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_timeMark2"]/text()'))
    sh['totalsign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_totalCount4"]/text()'))
    sh['totalsignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_totalArea4"]/text()'))
    sh['housesign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_residenceCount4"]/text()'))
    sh['housesignarea'] =  ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_residenceArea4"]/text()'))

    sh['totalnew'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_totalCount2"]/text()'))
    sh['totalnewarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_totalArea2"]/text()'))
    sh['housenew'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_residenceCount2"]/text()'))
    sh['housenewarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_residenceArea2"]/text()'))

    sh['totalforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_totalCount"]/text()'))
    sh['totalforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_totalArea"]/text()'))
    sh['houseforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_residenceCount"]/text()'))
    sh['houseforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5112_FDCJY_SignOnlineStatistics_residenceArea"]/text()'))

    write_zjw_sh_to_db(sh)

def get_eh_from_zjw(nselector):
    # get date
    eh= {}
    eh['date'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_timeMark4"]/text()'))
    eh['totalsign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalCount8"]/text()'))
    eh['totalsignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalArea8"]/text()'))
    eh['housesign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceCount8"]/text()'))
    eh['housesignarea'] =  ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceArea8"]/text()'))
    eh['businesssign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessCount8"]/text()'))
    eh['businesssignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessArea8"]/text()'))
    eh['officesign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeCount8"]/text()'))
    eh['officesignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeArea8"]/text()'))
    eh['parksign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkCount8"]/text()'))
    eh['parksignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkArea8"]/text()'))

    eh['totalsub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalCount7"]/text()'))
    eh['totalsubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalArea7"]/text()'))
    eh['housesub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceCount7"]/text()'))
    eh['housesubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceArea7"]/text()'))
    eh['businesssub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessCount7"]/text()'))
    eh['businesssubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessArea7"]/text()'))
    eh['officesub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeCount7"]/text()'))
    eh['officesubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeArea7"]/text()'))
    eh['parksub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkCount7"]/text()'))
    eh['parksubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkArea7"]/text()'))

    eh['totalforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalCount5"]/text()'))
    eh['totalforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalArea5"]/text()'))
    eh['houseforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceCount5"]/text()'))
    eh['houseforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceArea5"]/text()'))
    eh['businessforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessCount5"]/text()'))
    eh['businessforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessArea5"]/text()'))
    eh['officeforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeCount5"]/text()'))
    eh['officeforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeArea5"]/text()'))
    eh['parkforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkCount5"]/text()'))
    eh['parkforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkArea5"]/text()'))

    write_zjw_eh_to_db(eh)

def get_fh_from_zjw(nselector):
    # get date
    fh= {}
    fh['date'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_timeMark2"]/text()'))
    fh['totalsign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalCount4"]/text()'))
    fh['totalsignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalArea4"]/text()'))
    fh['housesign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceCount4"]/text()'))
    fh['housesignarea'] =  ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceArea4"]/text()'))
    fh['businesssign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessCount4"]/text()'))
    fh['businesssignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessArea4"]/text()'))
    fh['officesign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeCount4"]/text()'))
    fh['officesignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeArea4"]/text()'))
    fh['parksign'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkCount4"]/text()'))
    fh['parksignarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkArea4"]/text()'))

    fh['totalsub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalCount3"]/text()'))
    fh['totalsubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalArea3"]/text()'))
    fh['housesub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceCount3"]/text()'))
    fh['housesubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceArea3"]/text()'))
    fh['businesssub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessCount3"]/text()'))
    fh['businesssubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessArea3"]/text()'))
    fh['officesub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeCount3"]/text()'))
    fh['officesubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeArea3"]/text()'))
    fh['parksub'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkCount3"]/text()'))
    fh['parksubarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkArea3"]/text()'))

    fh['totalforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalCount"]/text()'))
    fh['totalforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_totalArea"]/text()'))
    fh['houseforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceCount"]/text()'))
    fh['houseforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_residenceArea"]/text()'))
    fh['businessforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessCount"]/text()'))
    fh['businessforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_BusinessArea"]/text()'))
    fh['officeforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeCount"]/text()'))
    fh['officeforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_OfficeArea"]/text()'))
    fh['parkforsell'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkCount"]/text()'))
    fh['parkforsellarea'] = ''.join(nselector.xpath('//*[@id="ess_ctr5115_FDCJY_HouseTransactionStatist_ParkArea"]/text()'))

    write_zjw_fh_to_db(fh)

if __name__ == '__main__':
    create_zjw_db_tables()
    content = requests.get(url_zjw).content
    try:
        nselector = etree.HTML(content)
    except Exception, e:
        err = "Exception for %s" % url_zjw
        print err
    get_sh_from_zjw(nselector)
    get_fh_from_zjw(nselector)
    get_eh_from_zjw(nselector)
    conn.close()

