#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue May  8 23:34:15 2018

@author: Alex Lau
"""
import requests
from bs4 import BeautifulSoup
import os
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from time import sleep
import pandas as pd
import itertools
import numpy as np

class wait_for_non_empty_text(object):
    def __init__(self, locator):
        self.locator = locator

    def __call__(self, driver):
        try:
            element_text = EC._find_element(driver, self.locator).text.strip()
            return element_text != ""
        except StaleElementReferenceException:
            return False

def getPrem(driver, info_list):
    # Setup Attributes
    gender = info_list[0]
    dob = info_list[1]
    smoke = info_list[2]
    sum_assured = info_list[3]
    if gender == 'M':
        gender = "'#et-gender-male'"
    else:
        gender = "'#et-gender-female'"
    if smoke == 'Y':
        smoke = "'#et-smoker-yes'"
    else:
        smoke = "'#et-smoker-no'"
    
    # Show next button
    js_next = "isDisplayNext(true,true,true,true);"
    driver.execute_script(js_next)
    sleep(0.5)
    # Show about yourself session
    js_abt_you = "var $aboutYourSelf = $('#et-about-yoursel-section');$aboutYourSelf.removeClass('hide-element')"
    driver.execute_script(js_abt_you)
    sleep(0.5)
    # Show calc session
    js_proceed = "$('et-btn-ay-self').removeClass('hide-element');"
    driver.execute_script(js_proceed)
    sleep(0.5)
    # Click 4 Buttons
    js_button = "$('#et-before-yes').click();$('#et-before-yes-04').click();$('#et-before-yes-02').click();$('#et-before-yes-03').click();"
    driver.execute_script(js_button)

    # Fill User Form
    js_gender = "$(" + gender +  ").click();"
    driver.execute_script(js_gender)
    js_dob = "$('#et-select-plan-date-input').datepicker('setDate','" + str(dob) + "')"
    driver.execute_script(js_dob)
    js_smoke = "$(" + smoke + ").click();"
    driver.execute_script(js_smoke)
    js_proceed = "var $planOption = $('#et-plan-option-section');$planOption.removeClass('hide-element');"
    driver.execute_script(js_proceed)
    js_sa = "$('#R2').val(" + str(sum_assured) + ");"
    driver.execute_script(js_sa)
    #sleep(1.5)

    # Keep making request until premium amount is found
    m_prem = None
    error_num = 0
    while m_prem is None:
        if error_num ==3:
            break
        try:
            js_getprem = "getEliteTermPremium()"
            driver.execute_script(js_getprem)
            wait = WebDriverWait(driver, 10)
            element = wait.until(
                wait_for_non_empty_text((By.ID, 'et-month-amount'))
                )
            m_prem = driver.find_element_by_id('et-month-amount').text
        except:
            error_num += 1
            sleep(30)
            pass
    info_list_wprem = info_list + [m_prem]
    return info_list_wprem

def getDF(user_list):
    # Set up web-driver
    chromedriver = './chromedriver'
    os.environ["webdriver.chrome.driver"] = chromedriver
    driver = webdriver.Chrome(chromedriver)
    # Open the window
    url = 'https://i.fwd.com.hk/en/term-life-insurance/select-plan'
    driver.get(url)
    sleep(4)
    # Iteratively get premium
    data = []
    num = 1
    for user in user_list:
        print('User input no. ',num)
        list1 = getPrem(driver=driver, info_list=user)
        data.append(list1)
        num += 1
    # Make dataframe
    df = pd.DataFrame(data, columns=['GENDER','DOB','SMOKE','SUM_ASSURED','PREMIUM'])
    driver.quit()
    return df

def main_run():
    # Create a comprehensive list of user input (1892 inputs)
    # Gender x 2
    gender_list = ['M','F']
    # DoB Age 19-61 x 43
    dob_list = ['01/01/%s' % s for s in range(1959,2001)]
    # Smoke Class x 2
    smoke_list = ['Y','N']
    # Sum Assured 400000,410000,...500000 x 11
    sa_list = list(range(400000,510000,10000))
    listObject = [gender_list, dob_list, smoke_list, sa_list]
    user_lists = []
    for ind in itertools.product(*listObject):
        user_lists.append(list(ind))
    #user_lists = user_lists[:25]
    print('Start Scraping FWD Premium ...')
    df = getDF(user_lists)
    print('Scraping Completed!!')
    df.to_csv('AllPrem_Output.csv',index=False)

# Execute scraping programme
main_run()
