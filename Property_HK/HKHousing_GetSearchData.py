#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May  9 02:54:34 2018

@author: Alex Lau
"""
import requests
import bs4 as bs
import pandas as pd
import numpy as np

def get_rows(input_list,page_num):
    # Set parameters
    purpose = str(input_list[0])
    size = str(input_list[1])
    yr = str(input_list[2])
    mth = str(input_list[3])
    page_num = str(page_num)
    # Parse website
    url_form = 'http://www.property.hk/tran.php?bldg=&prop={}&size={}&year={}&month={}&select=&page={}&dt=&tab=TRAN'
    url = url_form.format(purpose,size,yr,mth,page_num)
    r = requests.get(url)
    soup = bs.BeautifulSoup(r.content, 'lxml') # if r.text is used, will not properly encode
    table = soup.find('table', {'class': 'table table-hover hidden-xs'})
    rows = table.findAll('tr', {'bgcolor': ['','#f5f5f5']}) # OR operation
    return rows

def get_data(input_list):
    # Preset target list
    date_list = []
    address_list = []
    floor_list = []
    unit_list = []
    area_list = []
    deal_list = []
    unitprice_list = []
    
    # Start storing data into target list
    pg_num = 1
    rows = get_rows(input_list,pg_num)
    while not (rows == []):
        # For any content after page 20, it is directed to content at page 20 (website bug)
        if pg_num > 20:
            print('Further parsing would lead to infinite loop!')
            break
        print('Page %s is being parsed...' % pg_num)
        for row in rows:
            date = row.findAll('td',{'align':'center'})[0].text
            address = row.findAll('td',{'class':'hidden-xs'})[1].text
            floor = row.findAll('td',{'align':'center'})[1].text
            unit = row.findAll('td',{'align':'center'})[2].text
            area = row.findAll('td',{'align':'center'})[3].text
            deal = row.findAll('td',{'align':'center'})[4].text
            unitprice = row.findAll('td',{'align':'center'})[5].text
            date_list.append(date)
            address_list.append(address)
            floor_list.append(floor)
            unit_list.append(unit)
            area_list.append(area)
            deal_list.append(deal)
            unitprice_list.append(unitprice)
        # Proceed to next page
        pg_num+=1
        rows = get_rows(input_list,pg_num)
    
    # Consolidate and output data
    header = ['DATE','ADDRESS','FLOOR','UNIT','AREA','DEAL(MIL)','UNITPRICE']
    df = pd.DataFrame({'DATE':date_list,
                       'ADDRESS':address_list,
                       'FLOOR':floor_list,
                       'UNIT':unit_list,
                       'AREA':area_list,
                       'DEAL(MIL)':deal_list,
                       'UNITPRICE':unitprice_list})
    df = df[header]
    df.replace('', np.nan, regex=True,inplace=True)
    filename = 'HKHousing_%s_%s_%s_%s.csv' % tuple(input_list)
    df.to_csv(filename,index=False)
    print('%s is being exported!' % filename)
    return df

def input_translate(usage,size,year,month):
    if usage == 'A':
        usage = ''
    if size == '0':
        size = ''
    if month == '0':
        month = ''
    user_input = [usage,size,year,month]
    return user_input

# Run main programme
usage = input('Enter the usage(Input represented letter!):\nA:All\nR:Residential\nC:Retail\nO:Office\nI:Commercial\nP:Car Park\n')
size = input('Enter the property size(Input represented integer):\n0:All\n1:0-600 feet\n2:601-1000 feet\n3:1001-1500 feet\n4:1501-2000 feet\n5:2001-3000 feet\n6:3001-5000 feet\n7:5001-10000 feet\n8:10000+ feet\n')
year = input('Enter the transaction year(Input 2018-2011):\n')
month = input('Enter the transaction month(Input 0-12, 0 stands for all months):\n')
# Translate user input into user form
input_list = input_translate(usage,size,year,month)
print(input_list)
# usage, size, year, month
df = get_data(input_list)
