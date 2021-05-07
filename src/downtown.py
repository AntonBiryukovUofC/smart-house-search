import logging
import os
import traceback

import redis
from redis import Redis

from src.location import geocode_destination_here, Location
from src.redis_locations import location_from_listing, set_latitude_longitude_listing

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

REDIS_CONN_COUNT = 100

lat = 51.04782078532812
long = 114.05920283654567
key = 'house-search:listings/*'

dt = geocode_destination_here("Downtown, Calgary, AB")
dt_loc = Location(dt)
redis = Redis(host=os.getenv("REDIS_HOST", "10.20.40.57"))


def add_downtown_to_all():
    listings = redis.keys('house-search:listings/*')
    logging.info("Checking downtown data for "+str(len(listings))+" potential listings")
    for i in listings:
        k = i.decode()
        if "latitude" in k or "longitude" in k or "/downtown" in k or "/poi/" in k or "score" in k:
            pass
            # these keys do not represent a listing
        else:
            try:
                if redis.get(k + "/downtown") is None:
                    lat = redis.get(k + "latitude")
                    if lat is None:
                        l = location_from_listing(k, redis)
                    else:
                        l = Location(latitude=lat, longitude=redis.get(k + "longitude"))
                    data = l.get_point_of_interest_data(dt_loc)
                    redis.set(k + "/downtown", str(data['commute']))
            except Exception as e:
                log.exception(traceback.format_exc())


def add_downtown_to_one(location):
    try:
        data = location.get_point_of_interest_data(dt_loc)
        redis.set(location.listing_key + "/downtown", str(data['commute']))
    except Exception as e:
        print(e)


def main():
    # default behaviour is to add to all
    add_downtown_to_all()


if __name__ == "__main__":
    main()
