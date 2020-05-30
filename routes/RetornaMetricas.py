import tornado.web
import json

from pymongo import MongoClient
import math

# from ..config.Constants import RADIUS_OF_EARTH


def differenceInMeters(lat1, lon1, lat2, lon2):
    # raio_da_terra = 6371.009
    # dLat = lat2*math.pi/180 - lat1*math.pi/180
    # dLon = lon2*math.pi/180 - lon1*math.pi/180
    # a = math.sin(dLat/2) * math.sin(dLat/2) + math.cos(lat1 * math.pi / 180) * math.cos(lat2 * math.pi / 180) * math.sin(dLon / 2) * math.sin(dLon / 2)
    # c = 2 * math.atan2(math.sqrt(a), math.sqrt(1-a))
    # d = raio_da_terra * c

    metros_por_grau_latitude = 110574.61087757687
    metros_por_grau_longitude = 111302.61697430261

    diff_lat = lat1-lat2
    diff_lon = lon1-lon2

    diff_metros_lat = diff_lat * metros_por_grau_latitude
    diff_metros_lon = diff_lon * metros_por_grau_longitude

    print('diferença latitude: ', diff_metros_lat)
    print('diferença longitude: ', diff_metros_lon)

    d = math.sqrt(diff_metros_lat*diff_metros_lat + diff_metros_lon * diff_metros_lon)

    print('diferença: ', d)


class RetornaMetricas(tornado.web.RequestHandler):
    def get(self):
        mongo = MongoClient('localhost', 27017)
        database = mongo.denox
        collection = database.dados_rastreamento
        data = collection.find()
        for dt in data:
            print(dt)
        
        differenceInMeters(-15.7750828, -48.0779644, -22.5305064, 50.8850096)

        print('done')

        self.write("olá")
