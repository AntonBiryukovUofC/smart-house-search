import asyncio
import os
import asyncio_redis
import time
import requests
from typing import Optional
from pydantic import BaseModel
import re
import math
import functools
from loguru import logger as log

API_URL = 'https://api37.realtor.ca'
NAMESPACE = "house-search:"
LISTING_COLLECTION_KEY = "listings"

REDIS_CONN_COUNT = 100
PARALLEL_PAGE_PULL_COUNT = 5

class ListingModel(BaseModel):
    address: str
    lat: float
    long: float
    detail_url: str
    price: Optional[float]
    id: str
    mls_number: str
    key: str
    bathrooms: Optional[float]
    bedrooms: Optional[int]
    size: Optional[str]
    type: Optional[str]
    stories: Optional[float]
    lot_size: Optional[str]
    photo_url: Optional[str]


async def store_single_listing_data(data_chunk, redis, redis_semaphore):
    address = data_chunk['Property']['Address']['AddressText']
    log.info(f"Storing address={address}")

    regex_match = re.search('/real-estate/([0-9]*)/(.*)', data_chunk["RelativeDetailsURL"])
    url_key = regex_match.group(2)
    log.info(f"at url_key={url_key}")

    lat = data_chunk["Property"]['Address']["Latitude"]
    long = data_chunk["Property"]['Address']["Longitude"]

    try:
        price=float(data_chunk["Property"]["Price"].lstrip("$").replace(",", ""))
    except Exception:
        log.error(f"Failed to convert price string {data_chunk['Property']['Price'].lstrip('$').replace(',', '')} to float")
        price = None

    if "SizeTotal" in data_chunk["Land"]:
        lot_size = data_chunk["Land"]['SizeTotal']
    else:
        lot_size = None

    if "Building" in data_chunk:
        building_data = data_chunk["Building"]
        if "BathroomTotal" in building_data:
            bathrooms = float(building_data["BathroomTotal"])
        else:
            bathrooms = None
        if "Bedrooms" in building_data:
            bedrooms = sum([int(i) for i in re.findall(r'\d+', building_data["Bedrooms"])])
        else:
            bedrooms = None
        if "SizeInterior" in building_data:
            size = building_data["SizeInterior"]
        else:
            size = None
        if "StoriesTotal" in building_data:
            stories = building_data["StoriesTotal"]
        else:
            stories = None
        if "Type" in building_data:
            build_type = building_data["Type"]
        else:
            build_type = None

    if "Photo" in data_chunk["Property"]:
        photo_url = data_chunk["Property"]["Photo"][0]["HighResPath"]
    else:
        photo_url = None

    listing = ListingModel(address=address,
                           detail_url="https://www.realtor.ca/real-estate" + data_chunk["RelativeDetailsURL"],
                           id=data_chunk["Id"],
                           mls_number=data_chunk["MlsNumber"],
                           price=price,
                           key=url_key,
                           bathrooms=bathrooms,
                           bedrooms=bedrooms,
                           size=size,
                           stories=stories,
                           type=build_type,
                           lot_size=lot_size,
                           photo_url=photo_url,
                           lat=lat,
                           long=long,
                           )

    async with redis_semaphore:
        transaction = await redis.multi()
        await transaction.set(NAMESPACE+LISTING_COLLECTION_KEY+"/"+url_key, listing.json())
        await transaction.sadd(NAMESPACE+LISTING_COLLECTION_KEY, [url_key])

        await transaction.exec()


async def poll_page(page, options, page_pull_semaphore):
    async with page_pull_semaphore:
        log.info(f"pulling page {page}")
        loop = asyncio.get_event_loop()
        options["CurrentPage"] = page
        post = functools.partial(requests.post, API_URL + "/Listing.svc/PropertySearch_Post", data=options)
        result = await loop.run_in_executor(None, post)
    return result.json()["Results"]


async def poll_search_url(redis, redis_semaphore):

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

    # Identify number of requests to make
    # "Paging":{"RecordsPerPage":500,"CurrentPage":1,"TotalRecords":6206,"MaxRecords":500,"TotalPages":1,"RecordsShowing":500,"Pins":1961}
    paging_data = search_result.json()["Paging"]
    pages = math.ceil(int(paging_data["TotalRecords"])/int(paging_data["RecordsPerPage"]))

    listing_results = search_result.json()["Results"]

    page_pull_semaphore = asyncio.BoundedSemaphore(value=PARALLEL_PAGE_PULL_COUNT)
    page_tasks = [poll_page(pg, search_opts, page_pull_semaphore) for pg in range(2, pages)]
    log.info(f"About to pull {pages-1} of data")

    page_results = await asyncio.gather(*page_tasks)

    for result in page_results:
        listing_results.extend(result)

    # store data for the individual results
    tasks = [store_single_listing_data(chunk, redis, redis_semaphore) for chunk in listing_results]
    await asyncio.gather(*tasks)


async def main():
    log.info("starting main")
    redis_host = os.getenv("REDIS_HOST", "10.20.40.57")
    delay_time = int(os.getenv("CYCLE_TIME", 43200))

    log.info(f"starting with redis_host={redis_host} and delay_time={delay_time}")

    redis_connection = await asyncio_redis.Pool.create(host=redis_host, poolsize=REDIS_CONN_COUNT)
    redis_semaphore = asyncio.BoundedSemaphore(value=REDIS_CONN_COUNT)

    while True:
        # poll mls search
        log.info("about to poll")
        await poll_search_url(redis_connection, redis_semaphore)

        # wait
        time.sleep(delay_time)


asyncio.run(main())


