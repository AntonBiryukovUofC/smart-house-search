import json

from redis import Redis

from location import Location, geocode_destination_here


def set_latitude_longitude_listing(location: Location, redis):
    if location.listing_key is None:
        return False
    else:
        redis.set(location.listing_key + "/latitude", location.latitude)
        redis.set(location.listing_key + "/longitude", location.longitude)
        return True


def get_latitude_longitude_listing(location: Location, redis):
    if location.listing_key is None:
        return False
    else:
        lat=float(redis.get(location.listing_key + "/latitude"))
        long=float(redis.get(location.listing_key + "/longitude"))
        return lat, long


def location_from_listing(listing: str, redis: Redis):
    value = redis.get(listing)
    value = json.loads((value))
    l = Location(geocode_destination_here(value['address']))
    l.listing_key = listing
    return l


def listing_from_location(location: Location, redis: Redis):
    # not all locations are a listing
    return json.loads(redis.get(location.listing_key))

