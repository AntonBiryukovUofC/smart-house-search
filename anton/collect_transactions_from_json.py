from dotenv import load_dotenv, find_dotenv
from loguru import logger as log
from redis_dict import RedisDict

from anton.honestdoor_utils import login, read_transaction_table, get_page_source, read_assessment_price
import pandas as pd
import os
load_dotenv(find_dotenv())

driver =login()
pages_to_parse = pd.read_json('listings.json')
tmp_data_folder = os.environ['TMP_DATA']
os.makedirs(tmp_data_folder,exist_ok=True)

namespace = 'house-search'
r_dic = RedisDict(namespace=namespace)
listings_all = r_dic.redis.smembers(f'{namespace}:listings')
listings_honestdoor = r_dic.redis.smembers(f'{namespace}:listings_honestdoor')


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

