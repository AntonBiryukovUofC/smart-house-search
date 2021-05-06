from dotenv import load_dotenv, find_dotenv
from loguru import logger as log
from anton.honestdoor_utils import login, read_transaction_table, get_page_source, read_assessment_price
import pandas as pd
import os
load_dotenv(find_dotenv())

driver =login()
pages_to_parse = pd.read_json('listings.json')
tmp_data_folder = os.environ['TMP_DATA']
os.makedirs(tmp_data_folder,exist_ok=True)

for url_prop in pages_to_parse['url_property']:
    prop_id = url_prop.split('/')[-1]
    fname = os.path.join(tmp_data_folder, f'{prop_id}.json')
    if not(os.path.exists(fname)):
        page_str = get_page_source(url_prop,driver)
        df = read_transaction_table(page_str,prop_id)
        city_price=read_assessment_price(page_str)
        df['assessment_price'] = city_price
        df.to_json(fname)
    else:
        log.info(f'SKipping {prop_id}')
#st.write(page_str)
