import json

import streamlit as st
from dotenv import load_dotenv, find_dotenv
from redis_dict import RedisDict
import pandas as pd
load_dotenv(find_dotenv())
namespace = 'house-search'
r_dic = RedisDict(namespace=namespace)
listings = list(r_dic.redis.smembers('house-search:listings_honestdoor'))
st.write('Example')
key = f'{namespace}:listings_honestdoor/1117-36-street-se-calgary-albert-parkradisson-heights'
str_val = r_dic.redis.get(key)
json_listing = pd.read_json(str_val)
st.write(json_listing)



