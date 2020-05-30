import tornado.web

import json
import pandas as pd
from pymongo import MongoClient
import numpy as np
import datetime


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        print('gerando dataframe')
        dataframe = read_mongo('denox', 'dados_rastreamento')
        print(dataframe)
        print('oi')
        distances_km = []
        distances_get = getDistances(dataframe)
        print('distances_get ::: ', distances_get)

        print('soma ::: ', sum(distances_get))

        print(distances_km)
        self.write("Hello, world")

    def post(self):
        print(':::Capturando dados')
        response = tornado.escape.json_decode(self.request.body)
        print(response)
        print(':::Gerando dataframe')
        dataframe = read_mongo('denox', 'dados_rastreamento', {
            "serial": response['serial'],
            "datahora": {"$gt": response['dataInicio'], "$lt": response['dataFim']}
        })
        print(dataframe)
        print(':::Gerando lista de distancias')
        distancias = getDistances(dataframe)
        print(distancias)
        print(':::Somando distâncias')
        distancia_total = sum(distancias)
        print(distancia_total)
        print(':::Calculando quantidade de respostas')
        quantidade = len(dataframe.index)
        print(quantidade)
        print(':::Gerando array de movimento')
        print('tamanho do dataframe: ', len(dataframe.index))
        array_movimento = []
        tempos = []
        # array_movimento.append(dataframe.iloc[0])
        for x in range(quantidade):
            # print(dataframe.iloc[x])
            if (x == 0):
                array_movimento.append(dataframe.iloc[x])
                pass
            print(':::::::::::::::', dataframe.iloc[x]['situacao_movimento'],
                  ' != ', array_movimento[-1]['situacao_movimento'])

            if (dataframe.iloc[x]['situacao_movimento'] != array_movimento[-1]['situacao_movimento']):
                array_movimento.append(dataframe.iloc[x])
                movimentando = array_movimento[-1]['situacao_movimento']
                print("dataframe.iloc[x]['datahora']::::", dataframe.iloc[x]['datahora'])
                print("array_movimento[-1]['datahora']::", array_movimento[-1]['datahora'])
                deltatempo = dataframe.iloc[x]['datahora'] - \
                    array_movimento[-1]['datahora']
                print("deltatempo::", deltatempo)
                tempos.append((movimentando, deltatempo))
        print('::::::::::::::::::::::::::::::::::::::::')
        print(array_movimento)
        print(':::::::::::::::tempos')
        print(tempos)
        print(':::Montando resposta')
        resposta = {
            "distancia_percorrida": distancia_total,
            # "tempo_em_movimento": 43211234, #=> ESTE TEMPO DEVE ESTAR EM SEGUNDOS!
            # "tempo_parado": 987654321, #=> ESTE TEMPO DEVE ESTAR EM SEGUNDOS!
            # "centroides_paradas":[[-19.985399, -43.948095],[-19.974550, -43.948438]],
            "serial": response['serial']
        }
        print(resposta)
        print(':::Enviando resposta')
        self.write(resposta)


def getDistances(dataframe):
    distances_km = []
    anterior = None
    for row in dataframe.itertuples(index=False):
        if anterior:
            distances_km.append(
                haversine_distance(anterior.latitude, anterior.longitude,
                                   row.latitude, row.longitude)
            )
        else:
            distances_km.append(0)
        anterior = row
    # print(distances_km)
    return distances_km


def read_mongo(dbName, collectionName, query={}, host='localhost', port=27017, username=None, password=None):
    """ Read from Mongo and Store into DataFrame """

    # Connect to MongoDB
    db = _connect_mongo(host=host, port=port,
                        username=username, password=password, db=dbName)

    # Make a query to the specific DB and Collection
    cursor = db[collectionName].find(query)

    # Expand the cursor and construct the DataFrame
    df = pd.DataFrame(list(cursor))

    # print(df)
    # Delete the _id
    # print('tamanho :::: ', tamanho)
    # if no_id:
    # if no_id and len(df.index) > 0:
    #     del df['_id']

    return df


def haversine_distance(lat1, lon1, lat2, lon2):
    r = 6371009  # in meters
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2)**2 + np.cos(phi1) * \
        np.cos(phi2) * np.sin(delta_lambda / 2)**2
    res = r * (2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a)))
    return np.round(res, 2)


def _connect_mongo(host, port, username, password, db):
    """ A util for making a connection to mongo """

    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (
            username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)

    return conn[db]
