#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jul 27 14:30:15 2018

@author: ALEX LAU

OBJECTIVE:
INPUT A COLLECTION OF COMPANY ENTITY ID
OUTPUT THE RELATED TRANSACTION-LEVEL DATA

REMARK:
1. Each company name have a list of entity ids
2. For each entity id, parse a list of elite proxy
3. For each page, randomly select a proxy from the list

TO BE IMPROVED:
1. Shift/ change proxies set if some of them take too long time to access requests
"""
import pandas as pd
from bs4 import BeautifulSoup
import requests
import time
import random
import os
from lxml.html import fromstring
from collections import OrderedDict
from multiprocessing import Pool
from fake_useragent import UserAgent

###############################################################
####################### PARAMETERS ############################
###############################################################
TMP_INPUT_FILE = "tmp_eid_list.csv"
INPUT_FILE = "cstat_firm_name_list_weid.csv"
OUTPUT_FILE = "all_transaction_data.csv"
USER_AGENT_LIST = ["Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36",
                   "Mozilla/5.0 (Windows NT 6.1; WOW64; Trident/7.0; rv:11.0) like Gecko",
                   "Chrome/67.0.3396.99 Safari/537.36"]
SEARCH_URL_ROOT = "https://www.followthemoney.org/aaengine/aafetch.php?d-eid={}&gro=c-t-id&so=c-r-osid&p={}"
OUTPUT_FOLDER = "OUTPUTS"
DENY_MESSAGE = "have permission to access"
UA = UserAgent()
PROXY_LIST_LEN = 80
PROXY_NUM_MIN = 10
PROXY_NUM_MAX = 30
REQUEST_FAILURE_LIMIT = 5
CPU_NUM = 38
IS_GET_RESIDUAL = True

# Test case
test_case1 = [3539570, 8519439, 6300723, 3511715, 6502952, 6637623] # MAYTAG CORP
test_case1_name = 'MAYTAG CORP'
test_case2 = [4409389] # NORTHWEST PIPELINE CORP
test_case3 = [7513943] # ASPEN GROUP INC
test_case4 = [21520] # INCLUDE 966 pages (not in the input file)
test_case5_name = 'FIRST BANK'
test_case5 = [26283836, 26283820, 26283823, 26283824, 26283829, 26283830, 26283833,
              26283825, 26283819, 26283818, 26283821, 26283827, 26283835, 26283838,
              26283826, 26283832, 26283828, 26283834, 43551114, 26283831]


###############################################################
############## UTILITY FUNCTION FOR PROXIES ###################
###############################################################
def get_proxies():
    url = 'https://free-proxy-list.net/'
    response = requests.get(url)
    parser = fromstring(response.text)
    proxies = set()
    # consider the first 60 proxy in the list
    for i in parser.xpath('//tbody/tr')[:PROXY_LIST_LEN]:
        # If http is supported
        if i.xpath('.//td[7][contains(text(),"yes")]'):
            # And if the proxy = "elite proxy"
            if i.xpath('.//td[5]/text()')[0] == 'elite proxy':
                #Grabbing IP and corresponding PORT
                proxy = ":".join([i.xpath('.//td[1]/text()')[0], i.xpath('.//td[2]/text()')[0]])
                proxies.add(proxy)
    return proxies

# Set upper and lower limit for proxies collected
def get_enough_proxies():
    proxies = get_proxies()
    # if lower limit is not meet, re-collect another set of proxies
    while len(proxies) <= PROXY_NUM_MIN:
        print("Collected proxies are not enough, sleep 5s and collect another set of proxies...")
        time.sleep(5)
        proxies = get_proxies()
    # truncated to upper limit
    proxies = set(list(proxies)[:PROXY_NUM_MAX])
    return proxies


###############################################################
############## UTILITY FUNCTION FOR PARSING ###################
###############################################################
# Synthesize a complete url for search
def get_search_url(eid, page_num):
    search_url = SEARCH_URL_ROOT.format(eid, page_num)
    return search_url

# output a random user_agent as header
def get_rand_user_agent():
    rand_user_agent = random.sample(USER_AGENT_LIST,1)[0]
    return rand_user_agent

# Check if request access is denies
def is_block_page(html_text):
    if DENY_MESSAGE in html_text:
        return True
    else:
        return False

# Input a company name
# Output a html
def get_html(eid, proxies, page_num):
    # Randomly select a proxies from proxies
    proxy = random.sample(proxies, 1)[0]
    # Get a random user agent
    user_agent = UA.random
    header = {'user-agent': user_agent}
    # Set up url for request
    search_url = get_search_url(eid, page_num = page_num)

    # Start making a GET request, keep trying until proxy works
    error_num = 0
    is_proxy_broken = True
    while is_proxy_broken:
        try:
            time.sleep(3)
            raw_html = requests.get(search_url,
                                    headers = header,
                                    proxies = {'http': proxy, 'https': proxy},
                                    timeout = 15)
            if is_block_page(raw_html.text):
                raise ValueError
            # Escape the while loop if request is successful
            is_proxy_broken = False
        except:
            # If too many errors are found, collect another set of proxies
            error_num += 1
            if error_num >= REQUEST_FAILURE_LIMIT:
                print('Too many proxy are invalid. Collect another set of proxies...')
                proxies = get_enough_proxies()
                # initialize back error_num = 0
                error_num = 0
            # Sample another proxy for request
            print("%s can't be used, rotate to another proxy" % proxy)
            proxy = random.sample(proxies, 1)[0]

    # Clean the raw html
    bs_html = BeautifulSoup(raw_html.text , 'lxml') # 'html.parser'
    return bs_html

# Input a bs_html
# Output a transaction data of the specified html
# **** Can include candidate entity id as well
def html_to_trans_data(bs_html):
    # Initialized a Ordered Dict
    header_order = ['candiate_name', 'election_status', 'status_of_candidates',
                   'specific_party', 'general_party', 'election_jurisdiction',
                   'election_year', 'election_type', 'office_sought',
                   'incumbency_status', 'no_of_record', 'total_amt']
    df_dict = OrderedDict([(header, []) for header in header_order])
    df_dict['candidate_entity_id'] = []

    # Check if it is no result found
    if is_endpage(bs_html):
        # If no result, return an empty df
        empty_df = pd.DataFrame(df_dict, columns = header_order)
        return empty_df
    else:
        # If there is result, start parsing data into dict
        rows_html = bs_html.findAll('tbody')[0].findAll('tr')
        for row in rows_html:
            for ind, value in enumerate(header_order):
                df_dict[value].append(row.findAll('td')[1 + ind].text)
            df_dict['candidate_entity_id'].append(row.findAll('td')[1].findAll('a')[0]['tokenvalue'])
        # Store result in dataframe and return
        header_order += ['candidate_entity_id']
        trans_df = pd.DataFrame(df_dict, columns = header_order).reset_index(drop = True)
        return trans_df

# Input an entity id
# Parse all pages of the entity id and output all transaction data of the id
def eid_to_trans_data(eid, terminate_page = 99999):
    # Initialize
    page_num = 0 # Start from page 1 (index 0)
    proxies = get_enough_proxies()
    trans_df_list = []

    # Start parsing page until no result is found
    bs_html = get_html(eid, proxies, page_num)
    while not is_endpage(bs_html):
        trans_df = html_to_trans_data(bs_html)
        trans_df_list.append(trans_df)
        page_num += 1
        # If page_num exceed specified limit, escape the loop
        if page_num == terminate_page:
            break
        bs_html = get_html(eid, proxies, page_num)

    # Concat the list of transaction df
    # If entity id has no transaction data, ignore it
    try:
        eid_trans_data = pd.concat(trans_df_list).reset_index(drop = True)
        return eid_trans_data
    except:
        pass

# Inupt a list of eid(s)
# Process all its entities id and return all transaction data of the company
def eid_list_to_trans_data(donation_name, eid_list, output_filename):
    print('Processing %s ...' % donation_name)
    eid_trans_data_list = []
    # Store transaction data for each eid into a list
    for eid in eid_list:
        eid_trans_data = eid_to_trans_data(eid)
        # Include only if any transaction data is available
        if isinstance(eid_trans_data, pd.DataFrame):
            # Create 2 fields: eid & donation_name
            eid_trans_data['donation_name'] = donation_name
            eid_trans_data['firm_entity_id'] = eid
            # Append transaction data of specified eid into a list
        eid_trans_data_list.append(eid_trans_data)

    # No file output if no data is captured
    if eid_trans_data_list != []:
        # Aggregate a list of df -> transaction data of a firm
        eid_list_trans_data = pd.concat(eid_trans_data_list).reset_index(drop = True)
        # Output a tmp csv file
        if not os.path.isdir(OUTPUT_FOLDER):
            os.mkdir(OUTPUT_FOLDER)
        target_path = os.path.join(OUTPUT_FOLDER, output_filename)
        eid_list_trans_data.to_csv(target_path, index = False)
        print('%s has been exported to %s' % (output_filename, target_path))
        return eid_list_trans_data
    else:
        pass

# Input a row , output all transaction data of the related firm
def ind_to_trans_data(ind):
    row = df.iloc[ind, :]
    donation_name = row['donation_name']
    eid_list = row['eid']
    output_filename = row['output_filename']
    eid_list_trans_data = eid_list_to_trans_data(donation_name, eid_list, output_filename)
    return eid_list_trans_data

# Check if the page is terminated
# Input bs_html; Output boolean value
def is_endpage(bs_html):
    endpage_word = "No results found"
    foo = endpage_word in bs_html.text
    return foo

###############################################################
####################### DATA PROCESS ##########################
###############################################################
# Convert string list to true list type
# Need a debug for element with null list
def str_to_list(str_series):
    # Remove bracket and split by comma
    tmp_series = str_series.apply(lambda x: x[1:-1].split(','))
    # Convert each element to int
    list_series = tmp_series.apply(lambda list_: [int(str_) for str_ in list_])
    return list_series

# Input the mapping df
# Output the remaining firms that are yet to be parsed
def get_residual_df(df):
    exist_file_list = []
    for file in os.listdir(OUTPUT_FOLDER):
        exist_file_list.append(file)
    residual_df = df[~df.output_filename.isin(exist_file_list)].reset_index(drop = True)
    return residual_df

###############################################################
######################## MAIN RUN #############################
###############################################################
if __name__ == '__main__':
    start_time = time.time()
    print('Start parsing FollowTheMoney.org...')
    # Read in files and select list for input
    df = pd.read_csv(INPUT_FILE)
    print('Original file has %s firms' % df.shape[0])
    # Format list and partition input list
    df.eid = str_to_list(df.eid)

    if IS_GET_RESIDUAL:
        df = get_residual_df(df)
        print('Residual mode activated. Parse the remaining %s firms...' % df.shape[0])

    # Start multiprocessing and collection dfs
    p = Pool(processes=CPU_NUM)
    df_len = df.shape[0]
    df_list = p.map(ind_to_trans_data, range(df_len))

    # Aggregate dfs into one
    output_df = pd.concat(df_list).reset_index(drop = True)
    # Output to agg df to local dir
    output_df.to_csv(OUTPUT_FILE, index = False)
    print('Programme completed! Output file to local dir!')
    print('Time used %s' % (time.time()-start_time))
