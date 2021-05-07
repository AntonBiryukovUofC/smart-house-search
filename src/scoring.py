import ast
import json
import logging
import os
import traceback

import redis
from redis import Redis

from src import location
from src.location import geocode_destination_here, Location
from src.redis_locations import location_from_listing, set_latitude_longitude_listing

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

key = 'house-search:listings/*'

redis = Redis(host=os.getenv("REDIS_HOST", "10.20.40.57"))

walk_max = 30
walk_perfect = 10
walk_slope = 0.5

bike_max = 60
bike_perfect = 15
bike_slope = 2 / 9

drive_max = 45
drive_perfect = 10
drive_slope = 2 / 7

transit_max = 60
transit_perfect = 25
transit_slope = 2 / 7


def calculate_walk_score(time):
    if time <= walk_perfect:
        return 10
    elif time >= walk_max:
        return 0
    else:
        return 10 - ((time - walk_perfect) * walk_slope)


def calculate_bike_score(time):
    if time <= bike_perfect:
        return 10
    elif time >= bike_max:
        return 0
    else:
        return 10 - ((time - bike_perfect) * bike_slope)


def calculate_drive_score(time):
    if time <= drive_perfect:
        return 10
    elif time >= drive_max:
        return 0
    else:
        return 10 - ((time - drive_perfect) * drive_slope)


def calculate_transit_score(time, route):
    if time <= transit_perfect:
        return 10
    elif time >= transit_max:
        return 0
    else:
        # Most transit routes contain a bit of walking
        # give a bonus to routes less 3 or less steps
        if len(route['sections']) < 3:
            # haven't thought about the number 1 too much
            return min(10, 10 - ((time - transit_perfect) * transit_slope) + 1)
        return 10 - ((time - transit_perfect) * transit_slope)


def add_custom_commute_score_to_all(walk_weight=1, bike_weight=1, transit_weight=1, drive_weight=1):
    listings = redis.keys('house-search:listings/*')
    if walk_weight < 0:
        walk_weight = 1
        log.info("Reset negative walk weight to 1")

    if bike_weight < 0:
        bike_weight = 1
        log.info("Reset negative bike weight to 1")

    if transit_weight < 0:
        transit_weight = 1
        log.info("Reset negative transit weight to 1")

    if drive_weight < 0:
        drive_weight = 1
        log.info("Reset negative drive weight to 1")

    weighted_sum = walk_weight + bike_weight + transit_weight + drive_weight
    if weighted_sum <= 0:
        # Someone entered weird values
        log.info("Resetting weights due to weighted_sum being <=0")
        weighted_sum = 4
        walk_weight = bike_weight = transit_weight = drive_weight = 1

    for i in listings:
        k = i.decode()
        if "latitude" in k or "longitude" in k or "/downtown" in k or "/poi/" in k or "score" in k:
            pass
            # these keys do not represent a listing
        else:
            try:
                walk_score = bike_score = transit_score = drive_score = 0
                pois = redis.keys(k + '/poi/*')
                if len(pois) > 0:
                    for place in pois:
                        str_place = place.decode()
                        data = ast.literal_eval(redis.get(str_place).decode())
                        walk_score = walk_score + calculate_walk_score(data['walk_time'])
                        bike_score = bike_score + calculate_bike_score(data['bike_time'])
                        drive_score = drive_score + calculate_drive_score(data['drive_time'])
                        transit_score = transit_score + calculate_transit_score(data['transit_time'],

                                                                                data['transit_route']['routes'][0])
                    score = (
                                    walk_score * walk_weight + bike_score * bike_weight + transit_score * transit_weight + drive_score * drive_weight) / (
                                    weighted_sum * len(pois))
                    redis.set(k + "/custom_commute_score", score)
            except Exception as e:
                log.exception(traceback.format_exc())


def add_downtown_commute_score_to_all(walk_weight=1, bike_weight=1, transit_weight=1, drive_weight=1):
    listings = redis.keys('house-search:listings/*')
    if walk_weight < 0:
        walk_weight = 1
        log.info("Reset negative walk weight to 1")

    if bike_weight < 0:
        bike_weight = 1
        log.info("Reset negative bike weight to 1")

    if transit_weight < 0:
        transit_weight = 1
        log.info("Reset negative transit weight to 1")

    if drive_weight < 0:
        drive_weight = 1
        log.info("Reset negative drive weight to 1")

    weighted_sum = walk_weight + bike_weight + transit_weight + drive_weight
    if weighted_sum <= 0:
        # Someone entered weird values
        log.info("Resetting weights due to weighted_sum being <=0")
        weighted_sum = 4
        walk_weight = bike_weight = transit_weight = drive_weight = 1

    for i in listings:
        k = i.decode()
        if "latitude" in k or "longitude" in k or "/downtown" in k or "/poi/" in k or "score" in k:
            pass
            # these keys do not represent a listing
        else:
            try:
                walk_score = bike_score = transit_score = drive_score = 0
                data = ast.literal_eval(redis.get(k + '/downtown').decode())
                walk_score = walk_score + calculate_walk_score(data['walk_time'])
                bike_score = bike_score + calculate_bike_score(data['bike_time'])
                drive_score = drive_score + calculate_drive_score(data['drive_time'])
                transit_score = transit_score + calculate_transit_score(data['transit_time'],
                                                                        data['transit_route']['routes'][0])
                score = (
                                walk_score * walk_weight + bike_score * bike_weight + transit_score * transit_weight + drive_score * drive_weight) / (
                            weighted_sum)
                redis.set(k + "/downtown_commute_score", score)
            except Exception as e:
                log.exception(traceback.format_exc())


def add_custom_commute_score_to_one(location, walk_weight=1, bike_weight=1, transit_weight=1, drive_weight=1):
    listings = redis.keys('house-search:listings/*')
    if walk_weight < 0:
        walk_weight = 1
        log.info("Reset negative walk weight to 1")

    if bike_weight < 0:
        bike_weight = 1
        log.info("Reset negative bike weight to 1")

    if transit_weight < 0:
        transit_weight = 1
        log.info("Reset negative transit weight to 1")

    if drive_weight < 0:
        drive_weight = 1
        log.info("Reset negative drive weight to 1")

    weighted_sum = walk_weight + bike_weight + transit_weight + drive_weight
    if weighted_sum <= 0:
        # Someone entered weird values
        log.info("Resetting weights due to weighted_sum being <=0")
        weighted_sum = 4
        walk_weight = bike_weight = transit_weight = drive_weight = 1

    pois = redis.keys(location.listing_key + '/poi/*')
    if len(pois) > 0:
        try:
            walk_score = bike_score = transit_score = drive_score = 0
            for place in pois:
                str_place = place.decode()
                data = ast.literal_eval(redis.get(str_place).decode())
                walk_score = walk_score + calculate_walk_score(data['walk_time'])
                bike_score = bike_score + calculate_bike_score(data['bike_time'])
                drive_score = drive_score + calculate_drive_score(data['drive_time'])
                transit_score = transit_score + calculate_transit_score(data['transit_time'],
                                                                        data['transit_route']['routes'][0])
            score = (
                            walk_score * walk_weight + bike_score * bike_weight + transit_score * transit_weight + drive_score * drive_weight) / (
                            weighted_sum * len(pois))
            redis.set(location.listing_key + "/custom_commute_score", score)
        except Exception as e:
            log.exception(traceback.format_exc())


def add_downtown_commute_score_to_one(location, walk_weight=1, bike_weight=1, transit_weight=1, drive_weight=1):
    if walk_weight < 0:
        walk_weight = 1
        log.info("Reset negative walk weight to 1")

    if bike_weight < 0:
        bike_weight = 1
        log.info("Reset negative bike weight to 1")

    if transit_weight < 0:
        transit_weight = 1
        log.info("Reset negative transit weight to 1")

    if drive_weight < 0:
        drive_weight = 1
        log.info("Reset negative drive weight to 1")

    weighted_sum = walk_weight + bike_weight + transit_weight + drive_weight
    if weighted_sum <= 0:
        # Someone entered weird values
        log.info("Resetting weights due to weighted_sum being <=0")
        weighted_sum = 4
        walk_weight = bike_weight = transit_weight = drive_weight = 1

    try:
        walk_score = bike_score = transit_score = drive_score = 0
        data = ast.literal_eval(redis.get(location.listing_key + "/downtown").decode())
        walk_score = walk_score + calculate_walk_score(data['walk_time'])
        bike_score = bike_score + calculate_bike_score(data['bike_time'])
        drive_score = drive_score + calculate_drive_score(data['drive_time'])
        transit_score = transit_score + calculate_transit_score(data['transit_time'],
                                                                data['transit_route']['routes'][0])
        score = (
                        walk_score * walk_weight + bike_score * bike_weight + transit_score * transit_weight + drive_score * drive_weight) / (
                    weighted_sum)
        redis.set(location.listing_key + "/downtown_commute_score", score)
    except Exception as e:
        log.exception(traceback.format_exc())


def add_total_score_to_all(max_price=10000000.0, min_price=0, min_lot_size=0, price_weight=1, transit_weight=1,
                           size_weight=1):
    listings = redis.keys('house-search:listings/*')
    if transit_weight < 0:
        walk_weight = 1
        log.info("Reset negative walk weight to 1")

    if price_weight < 0:
        price_weight = 1
        log.info("Reset negative bike weight to 1")

    if size_weight < 0:
        size_weight = 1
        log.info("Reset negative transit weight to 1")

    weighted_sum = price_weight + transit_weight + size_weight
    if weighted_sum <= 0:
        # Someone entered weird values
        log.info("Resetting weights due to weighted_sum being <=0")
        weighted_sum = 3
        price_weight = transit_weight = size_weight = 1

    for i in listings:
        k = i.decode()
        if "latitude" in k or "longitude" in k or "/downtown" in k or "/poi/" in k or "score" in k:
            pass
            # these keys do not represent a listing
        else:
            try:
                transit_score = redis.get(k + "/downtown_commute_score")
                if transit_score is None:
                    transit_score = 0

                listing = json.loads(redis.get(k))
                price = listing['price']
                if price > max_price or price < min_price:
                    price_score = 0
                else:
                    price_score = 10

                if 'lot_size' in listing:
                    size = listing['lot_size']
                    try:
                        size = [int(s) for s in size.split() if s.isdigit()][-1]
                        if size > min_lot_size:
                            size_score = 10
                    except:
                        log.warning('lot size not parsed for ' + k)
                        size_score = 0
                else:
                    size_score = 0
                score = (
                                price_score * price_weight + size_score * size_weight + float(
                            transit_score) * transit_weight) / (
                            weighted_sum)
                redis.set(k + "/total_score", score)
            except Exception as e:
                log.exception(traceback.format_exc())

def add_total_score_to_one(location, max_price=10000000.0, min_price=0, min_lot_size=0, price_weight=1, transit_weight=1,
                           size_weight=1):
    if transit_weight < 0:
        walk_weight = 1
        log.info("Reset negative walk weight to 1")

    if price_weight < 0:
        price_weight = 1
        log.info("Reset negative bike weight to 1")

    if size_weight < 0:
        size_weight = 1
        log.info("Reset negative transit weight to 1")

    weighted_sum = price_weight + transit_weight + size_weight
    if weighted_sum <= 0:
        # Someone entered weird values
        log.info("Resetting weights due to weighted_sum being <=0")
        weighted_sum = 3
        price_weight = transit_weight = size_weight = 1

    try:
        transit_score = redis.get(location.listing_key + "/downtown_commute_score")
        if transit_score is None:
            transit_score = 0

        listing = json.loads(redis.get(location.listing_key))
        price = listing['price']
        if price > max_price or price < min_price:
            price_score = 0
        else:
            price_score = 10

        if 'lot_size' in listing:
            size = listing['lot_size']
            try:
                size = [int(s) for s in size.split() if s.isdigit()][-1]
                if size > min_lot_size:
                    size_score = 10
            except:
                log.warning('lot size not parsed for ' + location.listing_key)
                size_score = 0
        else:
            size_score = 0
        score = (
                        price_score * price_weight + size_score * size_weight + float(
                    transit_score) * transit_weight) / (
                    weighted_sum)
        print(score)
        redis.set(location.listing_key + "/total_score", score)
    except Exception as e:
        log.exception(traceback.format_exc())


def main():
    # default behaviour is to add to all
    add_custom_commute_score_to_all()
    add_downtown_commute_score_to_all()
    # total score to all needs params to go any further
    # add_total_score_to_all()


if __name__ == "__main__":
    main()
