import json
import random

from loguru import logger as log
from redis_dict import RedisDict
import pandas as pd
from anton.honestdoor_utils import login, get_page_source, read_transaction_table, read_assessment_price
import os
# Get a subset from Redis
namespace = 'house-search'
NSUB=350
r_dic = RedisDict(namespace=namespace)
listings_all = r_dic.redis.smembers('house-search:listings')
listings_honestdoor = r_dic.redis.smembers('house-search:listings_honestdoor')
listings_honestdoor_blacklist =r_dic.redis.smembers('house-search:listings_honestdoor_blacklist')

log.info(f'Listings processed: {len(listings_honestdoor)} | total: {len(listings_all)} | blacklisted: {len(listings_honestdoor_blacklist)}')
listings_difference = list(listings_all - listings_honestdoor-listings_honestdoor_blacklist)

log.info(f'Difference size: {len(listings_difference)}')
listings_to_process = random.choices(listings_difference,k=NSUB)
dict_to_process = {'listing':listings_to_process}
with open('listings.json','w') as f:
    json.dump(dict_to_process, f,indent=4)


# Run transaction pulling
driver =login()
pages_to_parse = pd.read_json('listings.json')
tmp_data_folder = os.environ['TMP_DATA']
os.makedirs(tmp_data_folder,exist_ok=True)

for property in pages_to_parse['listing']:
    prop_url_part = property.split('-calgary')[0]+'-calgary-ab'
    full_url =f'https://www.honestdoor.com/property/{prop_url_part}'
    fname = os.path.join(tmp_data_folder, f'{property}.json')
    log.info(f'Processing {full_url}')
    try:
        page_str = get_page_source(full_url,driver)
        if not('page could not be found' in page_str):
            df = read_transaction_table(page_str,property)
            city_price=read_assessment_price(page_str)
            df['assessment_price'] = city_price
            df.to_json(fname)
            # populate set at Redis
            r_dic.redis.sadd(f'{namespace}:listings_honestdoor',property)
            r_dic.redis.set(f'{namespace}:listings_honestdoor/{property}', df.to_json())
        else:
            log.error(f'Page does not exist at {full_url}!')
            r_dic.redis.sadd(f'{namespace}:listings_honestdoor_blacklist',property)


    except Exception as e:
        log.exception(e)
        log.info(f'Failed at {full_url}!')
driver.close()
# get a subset of those (10)

