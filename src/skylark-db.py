import os
import re
import redis


class SkylarkDB(object):
    def __init__(self, redis_ip='localhost', redis_port=6379):
        skylarkdb = redis.Redis(host=redis_ip, port=redis_port, db=0)
        _object_dict = {'object_path':object_path, 
                'cloud':cloud,
                'region':region,
                'profile':profile,
                'additional':''}
        skylarkdb.insert("schema", _object_dict)
        

    def insertObject(self, object_path, cloud, region, profile, additional):
        if object_path[:5] == "s3://":
            skylark_object = "sk://" + object_path[5:]
        else:
            print("We only support S3 for now")
        if skylarkdb.exists(skylark_object):
            pass # Need to check if there is match for regions, etc.
        skylarkdb.set(skylark_object, {object_path, cloud, region, profile, additional})
        return skylark_object

    def retrieveObject(self, skylark_object):
        if not skylarkdb.exists(skylark_object):
            return "{Null}"
        else:
            return skylarkdb.get(skylark_object)


