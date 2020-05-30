import tornado.ioloop
import tornado.web

from routes.Main import MainHandler
from routes.CalculaMetricas import CalculaMetricas
from routes.RetornaMetricas import RetornaMetricas


def make_app():


    return tornado.web.Application([
        (r"/", MainHandler),
        (r"/api/calcula_metricas", CalculaMetricas),
        (r"/api/retorna_metricas", RetornaMetricas),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
