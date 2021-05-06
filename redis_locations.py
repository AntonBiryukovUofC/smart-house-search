import json

from redis import Redis

from location import geocode_destination_here, Location


def location_from_listing(listing: str, redis: Redis):
    value=redis.get(listing)
    value=json.loads((value))
    return Location(geocode_destination_here(value['address']))
