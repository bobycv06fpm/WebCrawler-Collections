#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  8 02:13:39 2018

@author: Alex Lau
"""
import requests
import bs4 as bs
import os
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import Select
from time import sleep
import pandas as pd
import numpy as np

def pageno_to_html(page_num,driver):
    js_getpage = "page(" + str(page_num) + ")"
    driver.execute_script(js_getpage)
    element = driver.find_element_by_xpath("//*")
    html_raw = element.get_attribute("outerHTML")
    return html_raw

def html_to_id(html_raw):
    soup = bs.BeautifulSoup(html_raw, 'lxml')
    table = soup.find('tbody')
    rows = table.findAll('td', {'class': 'taR'})[1::2]
    id_list = [tag.text for tag in rows]
    return id_list

def id_to_rhtml(id_str,driver):
    js_getrpage = "detail(" + id_str + ")"
    driver.execute_script(js_getrpage)
    element = driver.find_element_by_xpath("//*")
    rhtml_raw = element.get_attribute("outerHTML")
    js_back = "back()"
    driver.execute_script(js_back)
    return rhtml_raw

def rhtml_to_data(rhtml_raw):
    soup = bs.BeautifulSoup(rhtml_raw, 'lxml')
    table = soup.find('div',{'class':'contentsBox'})

    # First Table
    geo = table.find('table',{'class':'m-item_tbl'})
    # record data
    overall_place = geo.findAll('td',{'class':'taR'})[0].text
    number_card = geo.findAll('td',{'class':'taR'})[1].text
    name = geo.findAll('td',{'class':'taL'})[0].text

    # Second Table
    detail = table.findAll('tbody')[1]
    # record data
    try:
        race_category = detail.findAll('td')[0].text
        category_place = detail.findAll('td')[1].text
        age = detail.findAll('td')[2].text
        age_place = detail.findAll('td')[3].text
        gender = detail.findAll('td')[4].text
        gender_place = detail.findAll('td')[5].text
        nation = detail.findAll('td')[6].text
        nation_place = detail.findAll('td')[7].text
        residence = detail.findAll('td')[8].text
        residence_place = detail.findAll('td')[9].text
        time_net = detail.findAll('td')[10].text
        time_gross = detail.findAll('td')[11].text
    except:
        # Residence field does not exist for non-Japanese runners
        race_category = detail.findAll('td')[0].text
        category_place = detail.findAll('td')[1].text
        age = detail.findAll('td')[2].text
        age_place = detail.findAll('td')[3].text
        gender = detail.findAll('td')[4].text
        gender_place = detail.findAll('td')[5].text
        nation = detail.findAll('td')[6].text
        nation_place = detail.findAll('td')[7].text
        residence = np.nan
        residence_place = np.nan
        time_net = detail.findAll('td')[8].text
        time_gross = detail.findAll('td')[9].text

    # Third Table
    try:
        time_record = soup.findAll('table',{'class':'m-item_tbl mb10'})[1]
        time_blocks = time_record.findAll('tr')[1:]
        # record data
        time_5km = time_blocks[0].findAll('td',{'class':'taC'})[0].text
        time_10km = time_blocks[2].findAll('td',{'class':'taC'})[0].text
        time_15km = time_blocks[4].findAll('td',{'class':'taC'})[0].text
        time_20km = time_blocks[6].findAll('td',{'class':'taC'})[0].text
        time_half = time_blocks[8].findAll('td',{'class':'taC'})[0].text
        time_25km = time_blocks[9].findAll('td',{'class':'taC'})[0].text
        time_30km = time_blocks[11].findAll('td',{'class':'taC'})[0].text
        time_35km = time_blocks[13].findAll('td',{'class':'taC'})[0].text
        time_40km = time_blocks[15].findAll('td',{'class':'taC'})[0].text
        time_finish = time_blocks[17].findAll('td',{'class':'taC'})[0].text
    except:
        # Some players don't have time record breakdown
        time_5km = np.nan
        time_10km = np.nan
        time_15km = np.nan
        time_20km = np.nan
        time_half = np.nan
        time_25km = np.nan
        time_30km = np.nan
        time_35km = np.nan
        time_40km = np.nan
        time_finish = np.nan

    # Make Dataframe
    header = ['overall_place','id','name',
              'race_category','category_place','age','age_place','gender','gender_place','nation','nation_place','residence','residence_place','time_net','time_gross',
             'time_5km','time_10km','time_15km','time_20km','time_half','time_25km','time_30km','time_35km','time_40km','time_finish']
    df_dict = {'overall_place':[overall_place],
              'id':[number_card],
              'name':[name],
              'race_category':[race_category],
              'category_place':[category_place],
              'age':[age],
              'age_place':[age_place],
              'gender':[gender],
              'gender_place':[gender_place],
              'nation':[nation],
              'nation_place':[nation_place],
              'residence':[residence],
              'residence_place':[residence_place],
              'time_net':[time_net],
              'time_gross':[time_gross],
              'time_5km':[time_5km],
              'time_10km':[time_10km],
              'time_15km':[time_15km],
              'time_20km':[time_20km],
              'time_half':[time_half],
              'time_25km':[time_25km],
              'time_30km':[time_30km],
              'time_35km':[time_35km],
              'time_40km':[time_40km],
              'time_finish':[time_finish]}

    df = pd.DataFrame(df_dict,columns=header)
    return df

def get_all_runners(start_page,end_page,step,driver,filename='DF_OUTPUT_From401Page.xlsx'):
    page_no = start_page
    print('Start scraping from Page ',page_no)
    df = pd.DataFrame()

    # Dummy Setting
    rows = ['XXX']
    next_page_html = pageno_to_html(page_no,driver)

    # Iterate until last page (if last page, rows == [])
    while not rows == []:
        page_html = next_page_html
        id_list = html_to_id(page_html)
        for runner_id in id_list:
            # If Not found (error to origin), run the code again
            error_num = 0
            error = True
            while error:
                if error_num==3:
                    break
                try:
                    runner_html = id_to_rhtml(runner_id,driver)
                    error = False
                except:
                    error+=1
                    driver.refresh()
                    pass
            tmp_df = rhtml_to_data(runner_html)
            df = df.append(tmp_df)

        # Checkpoint
        if page_no % step == 0:
            print('First ',page_no,' pages has been scraped. Save point with a XLSX file.')
            df.to_excel(filename,index=False)
        if page_no == end_page:
            break
        page_no+=1

        # Check if it is the ending page
        next_page_html = pageno_to_html(page_no,driver)
        soup = bs.BeautifulSoup(next_page_html, 'lxml')
        table = soup.find('tbody')
        rows = table.findAll('td', {'class': 'taR'})[1::2]

    # Output xlxs
    df.to_excel(filename,index=False)
    return df

def main_run(start_pg,end_pg,stp):
    # Set up web-driver
    chromedriver = "../chromedriver"
    os.environ["webdriver.chrome.driver"] = chromedriver
    driver = webdriver.Chrome(chromedriver)
    # Open the window
    url = "http://www.marathon.tokyo/2017/result/index.php"
    driver.get(url)
    # Start scraping
    df = get_all_runners(start_page=start_pg,end_page=end_pg,step=stp,driver=driver)
    driver.quit()
    print('Web Scraping Completed!!')
    return df

# Get all runner data starting from page 1 till the last page
# end_pg = 9999 is dummy
df = main_run(start_pg=401,end_pg=9999,stp=50)
