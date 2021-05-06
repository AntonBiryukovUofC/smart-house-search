import os
import re
from time import sleep

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import requests
from dotenv import load_dotenv, find_dotenv
from loguru import logger as log
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import ssl

from selenium.webdriver.common.keys import Keys

ssl._create_default_https_context = ssl._create_unverified_context
load_dotenv(find_dotenv())


def save_html(html, path):
    with open(path, 'wb') as f:
        f.write(html)

def login():
    options = Options()
    options.headless = False
    driver = webdriver.Chrome(chrome_options=options)
    driver.get('http://honestdoor.com')
    base_url = driver.current_url
    sleep(1.5)
    property_url = f'{base_url}property'
    url_test = f'{property_url}/2627-lionel-crescent-sw-calgary-ab'
    log.info(f'Visiting {url_test}')
    driver.get(url_test)
    buttons_xpath = '//*[contains(concat( " ", @class, " " ), concat( " ", "ant-btn-primary", " " ))]//span'
    field_pwd_xpath = '//*[(@id = "password")]'
    field_username_xpath = '//*[(@id = "username")]'

    for i in driver.find_elements_by_xpath(buttons_xpath):
        log.info(i.text)
        if 'Sign in' in i.text:
            i.click()
            sleep(1)
            # Ready to login
            pwd = driver.find_element_by_xpath(field_pwd_xpath)
            uname = driver.find_element_by_xpath(field_username_xpath)
            pwd.send_keys(os.environ['HONESTDOOR_PWD'])
            uname.send_keys(os.environ['HONESTDOOR_USERNAME'])
            pwd.send_keys(Keys.ENTER)
            break
    sleep(1)
    return driver

def get_page_source(url,driver):
    driver.get(url)
    sleep(1.5)
    page_str = driver.page_source
    return page_str


def read_transaction_table(page_str,property_id):
    log.info(f'Transactions for {property_id}...')
    soup = BeautifulSoup(page_str,'lxml')

    table = soup.find('div', attrs={'class': re.compile('^TransactionsSection__Root.*')})
    log.info(f'Print {table}')
    df = pd.read_html(str(table))[0]
    df['property_id'] = property_id
    return df

def read_assessment_price(page_str):
    soup = BeautifulSoup(page_str, 'lxml')
    # Get city assessment price:
    value_section = soup.find('div', attrs={'class': re.compile('^AssessmentsSection__Root.*')}).find('span', attrs={
        'class': re.compile('.*statistic-.*-value.*')})
    city_price = value_section.text
    return city_price

