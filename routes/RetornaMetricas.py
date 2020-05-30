import tornado.web

import json
import pandas as pd
from pymongo import MongoClient
import numpy as np
import datetime


# def differenceInMeters(lat1, lon1, lat2, lon2):
#     metros_por_grau_latitude = 110574.61087757687
#     metros_por_grau_longitude = 111302.61697430261
#     diff_lat = lat1-lat2
#     diff_lon = lon1-lon2
#     diff_metros_lat = diff_lat * metros_por_grau_latitude
#     diff_metros_lon = diff_lon * metros_por_grau_longitude
#     print('diferença latitude: ', diff_metros_lat)
#     print('diferença longitude: ', diff_metros_lon)\
#     d = math.sqrt(diff_metros_lat*diff_metros_lat + diff_metros_lon * diff_metros_lon)
#     print('diferença: ', d)
#     return d


class RetornaMetricas(tornado.web.RequestHandler):
    def get(self):
        print(':::Capturando dados')
        serial = self.get_argument('serial')
        dataInicio = int(self.get_argument('dataInicio'))
        dataFim = int(self.get_argument('dataFim'))

        print(':::Gerando dataframe')
        dataframe = read_mongo('denox', 'dados_rastreamento', {
            "serial": serial,
            "datahora": {"$gt": dataInicio, "$lt": dataFim}
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
        temposParadoLista = []
        temposMovimentoLista = []
        # array_movimento.append(dataframe.iloc[0])
        for x in range(quantidade):
            # print(dataframe.iloc[x])
            if (x == 0):
                array_movimento.append(dataframe.iloc[x])
                pass
            print(':::::::::::::::', dataframe.iloc[x]['situacao_movimento'],
                  ' != ', array_movimento[-1]['situacao_movimento'])

            movimentando = array_movimento[-1]['situacao_movimento']
            if (dataframe.iloc[x]['situacao_movimento'] != movimentando):
                # print("dataframe.iloc[x]['datahora']::::",
                #       dataframe.iloc[x]['datahora'])
                print('======================================================')
                print(array_movimento)
                print('======================================================')
                print("dataframe.iloc[x]['datahora']::::",
                      dataframe.iloc[x]['_id'])
                print("array_movimento[-1]['datahora']::",
                      array_movimento[-1]['_id'])
                deltatempo = int(
                    dataframe.iloc[x]['datahora']) - int(array_movimento[-1]['datahora'])
                print("deltatempo::", deltatempo)
                if(movimentando):
                    temposMovimentoLista.append(deltatempo)
                else:
                    temposParadoLista.append(deltatempo)
                array_movimento.append(dataframe.iloc[x])
        print('::::::::::::::::::::::::::::::::::::::::')
        print(array_movimento)
        print(':::::::::::::::tempos')
        tempoParado = sum(temposParadoLista)
        tempoMovimento = sum(temposMovimentoLista)
        print(':::Montando resposta')
        resposta = {
            "distancia_percorrida": distancia_total,
            "tempo_em_movimento": tempoMovimento, #=> ESTE TEMPO DEVE ESTAR EM SEGUNDOS!
            "tempo_parado": tempoParado, #=> ESTE TEMPO DEVE ESTAR EM SEGUNDOS!
            # "centroides_paradas":[[-19.985399, -43.948095],[-19.974550, -43.948438]],
            "serial": serial
        }
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
