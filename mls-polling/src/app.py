import asyncio
import os
import asyncio_redis
import time
import requests
from typing import Optional
from pydantic import BaseModel
import re

API_URL = 'https://api37.realtor.ca'
ROOT_KEY = "house-search"
LISTING_COLLECTION_KEY = "listings"


class ListingModel(BaseModel):
    address: str
    detail_url: str
    price: Optional[float]
    id: str
    mls_number: str
    key: str



async def store_single_listing_data(data_chunk, redis):
    address = data_chunk['Property']['Address']['AddressText']
    print(f"Storing address={address}")

    regex_match = re.search('/real-estate/([0-9]*)/(.*)', data_chunk["RelativeDetailsURL"])
    url_key = regex_match.group(2)
    print(f"at url_key={url_key}")

    try:
        price=float(data_chunk["Property"]["Price"].lstrip("$").replace(",", ""))
    except:
        print(f"Failed to convert price string {data_chunk['Property']['Price'].lstrip('$').replace(',', '')} to float")
        price=None

    listing = ListingModel(address=address,
                           detail_url="https://www.realtor.ca/real-estate" + data_chunk["RelativeDetailsURL"],
                           id=data_chunk["Id"],
                           mls_number=data_chunk["MlsNumber"],
                           price=price,
                           key=url_key
                           )

    transaction = await redis.multi()
    await transaction.set(ROOT_KEY+"/"+url_key, listing.json())
    await transaction.sadd(ROOT_KEY+"/"+LISTING_COLLECTION_KEY, [url_key])

    await transaction.exec()


async def poll_search_url(redis):
    search_opts = {
        "CultureId": 1,
        "ApplicationId": 37,
        "PropertySearchTypeId": 1,
        "RecordsPerPage": 500,
        "CurrentPage": 1,
        "PriceMin": 100000,
        "PriceMax": 2000000,
        "LongitudeMin": -114.40999,
        "LongitudeMax": -113.76385,
        "LatitudeMin": 50.77617,
        "LatitudeMax": 51.27885,
    }
    search_result = requests.post(API_URL + "/Listing.svc/PropertySearch_Post", data=search_opts)

    # store data for the individual results
    # TODO: Follow through the pagination
    tasks = [store_single_listing_data(chunk, redis) for chunk in search_result.json()["Results"]]
    await asyncio.gather(*tasks)


async def main():
    print("starting main")
    redis_host = os.getenv("REDIS_HOST", "10.20.40.57")

    redis_connection = await asyncio_redis.Pool.create(host=redis_host, poolsize=10)

    while True:
        # poll mls search
        print("about to poll")
        await poll_search_url(redis_connection)

        # wait
        time.sleep(3600)


asyncio.run(main())


