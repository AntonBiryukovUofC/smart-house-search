import json

from redis import Redis

from location import geocode_destination_here, Location


def location_from_listing(listing: str, redis: Redis):
    value = redis.get(listing)
    value = json.loads((value))
    l = Location(geocode_destination_here(value['address']))
    l.listing_key = listing
    return l


def listing_from_location(location: Location, redis: Redis):
    # not all locations are a listing
    return json.loads(redis.get(location.listing_key))

