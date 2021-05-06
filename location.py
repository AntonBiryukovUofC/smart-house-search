import logging
import json
import math
from datetime import datetime

import requests

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)

URL = "https://geocode.search.hereapi.com/v1/geocode"
API_KEY = ''


class Location:

    def __init__(self, response: dict):

        self.hereid = response['items'][0]['id']
        self.address = response['items'][0]['title']
        self.longitude = response['items'][0]['position']['lng']
        self.latitude = response['items'][0]['position']['lat']
        self.mapview = response['items'][0]['mapView']
        self.points_of_interest = []

    def add_point_of_intrest(self, location):
        transit_route = transit_routes(self, location)
        time = transit_time(transit_route['routes'][0])
        walk_time=transit_time(walk(self, location)['routes'][0])
        commute={'transit_route': transit_route, 'transit_time': time, 'walk_time':walk_time}
        data = {'location': location, 'commute': commute}
        self.points_of_interest.append(data)

    def __eq__(self, other):
        if self.hereid==other.hereid:
            return True
        else:
            return False

def geocode_destination_here(x: str):
    log.info(f"Geocoding query : {x}")
    payload = {"q": x, "apiKey": API_KEY}
    r = requests.get(URL, params=payload)
    return r.json()

def transit_routes(spot1: Location, spot2: Location):
    data = {'apikey': API_KEY, 'origin': str(spot1.latitude) + ',' + str(spot1.longitude),
            'destination': str(spot2.latitude) + ',' + str(spot2.longitude)}
    r = requests.get(url='https://transit.router.hereapi.com/v8/routes', params=data)
    return r.json()

def transit_time(route):
    time = 0
    for i in route['sections']:
        departure = datetime.strptime(i['departure']['time'], '%Y-%m-%dT%H:%M:%S%z')
        arrival = datetime.strptime(i['arrival']['time'], '%Y-%m-%dT%H:%M:%S%z')
        time = time + (arrival - departure).total_seconds()
    # convert too mins
    time = math.ceil((time / 60))
    return time

def walk(spot1: Location, spot2: Location):
    data = {'apikey': API_KEY, 'origin': str(spot1.latitude) + ',' + str(spot1.longitude),
            'destination': str(spot2.latitude) + ',' + str(spot2.longitude),
            'transportMode':'pedestrian'}
    r = requests.get(url='https://router.hereapi.com/v8/routes', params=data)
    return r.json()
