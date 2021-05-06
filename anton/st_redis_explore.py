import json
import os
import re
from time import sleep

from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import streamlit as st
import requests
from dotenv import load_dotenv, find_dotenv
from loguru import logger as log
from redis_dict import RedisDict
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
import ssl


load_dotenv(find_dotenv())
namespace = 'house-search'
r_dic = RedisDict(namespace=namespace)
listings = list(r_dic.redis.smembers('house-search:listings'))
st.write('Example')
key = f'{namespace}:listings/{listings[1]}'
str_val = r_dic.redis.get(key)
st.write(str_val)
json_listing = json.loads(str_val)
st.write(json_listing)
st.write('Listing keys:')
st.write(listings)



