import tornado.web
import tornado.escape

from pymongo import MongoClient


class CalculaMetricas(tornado.web.RequestHandler):
    def post(self):
        mongo = MongoClient('localhost', 27017)
        database = mongo.denox
        collection = database.dados_rastreamento

        response = tornado.escape.json_decode(self.request.body)

        collection.insert_one(response)
        self.write("Calcula Metricas")
