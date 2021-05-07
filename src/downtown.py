import os

from redis import Redis

from src.location import geocode_destination_here, Location
from src.redis_locations import location_from_listing, set_latitude_longitude_listing


class Downtown:
    lat = 51.04782078532812
    long = 114.05920283654567
    key = 'house-search:listings/*'

    dt = geocode_destination_here("Downtown, Calgary, AB")
    dt_loc = Location(dt)
    redis = Redis(host=os.getenv("REDIS_HOST", "10.20.40.57"))

    def add_downtown_to_all(self):
        listings = self.redis.keys('house-search:listings/*')
        for i in listings:
            k = i.decode()
            if "latitude" in k or "longitude" in k:
                pass
            else:
                try:
                    if self.redis.get(k + "/downtown") is None:
                        l = location_from_listing(k, self.redis)
                        data = l.get_point_of_interest_data(self.dt_loc)
                        self.redis.set(k + "/downtown", str(data['commute']))
                except Exception as e:
                    print(e)

    def add_downtown_to_one(self, location):
        try:
            data = location.get_point_of_interest_data(self.dt_loc)
            self.redis.set(location.listing_key + "/downtown", str(data['commute']))
        except Exception as e:
            print(e)

