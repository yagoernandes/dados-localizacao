import tornado.web

import json
import pandas as pd
from pymongo import MongoClient
import numpy as np
import datetime
from sklearn.cluster import KMeans


class RetornaMetricas(tornado.web.RequestHandler):
    def get(self):
        # Capturando parametros
        serial = self.get_argument('serial')
        dataInicio = int(self.get_argument('dataInicio'))
        dataFim = int(self.get_argument('dataFim'))

        # Gerando dataframe
        dataframe = read_mongo('denox', 'dados_rastreamento', {
            "serial": serial,
            "datahora": {"$gt": dataInicio, "$lt": dataFim}
        })

        # Gerando lista de distancias
        distancias = getDistances(dataframe)

        # Somando distâncias
        distancia_total = sum(distancias)

        # Calculando quantidade de respostas
        quantidade = len(dataframe.index)

        # initial state
        array_movimento = []
        temposParadoLista = []
        temposMovimentoLista = []
        posicoesParado = []
        quantidadeDeParadas = 0
        # Gerando array de movimento
        for x in range(quantidade):
            if (x == 0):
                array_movimento.append(dataframe.iloc[0])
                if(not dataframe.iloc[0]['situacao_movimento']):
                    quantidadeDeParadas = quantidadeDeParadas + 1
                pass

            movimentando = array_movimento[-1]['situacao_movimento']
            if (not movimentando):
                posicoesParado.append(
                    [dataframe.iloc[x]['latitude'], dataframe.iloc[x]['longitude']])

            if (dataframe.iloc[x]['situacao_movimento'] != movimentando):

                deltatempo = int(
                    dataframe.iloc[x]['datahora']) - int(array_movimento[-1]['datahora'])
                if (movimentando):  # Quando para
                    temposParadoLista.append(deltatempo)
                    quantidadeDeParadas = quantidadeDeParadas + 1
                else:  # Quando começa a andar
                    temposMovimentoLista.append(deltatempo)
                array_movimento.append(dataframe.iloc[x])
        kmeans = KMeans(n_clusters=quantidadeDeParadas,
                        random_state=0).fit(posicoesParado)

        tempoParado = sum(temposParadoLista)
        tempoMovimento = sum(temposMovimentoLista)
        # Enviando resposta
        resposta = {
            "distancia_percorrida": distancia_total,
            "tempo_em_movimento": tempoMovimento,
            "tempo_parado": tempoParado,
            "centroides_paradas": kmeans.cluster_centers_.tolist(),
            "serial": serial
        }
        # gravando
        print(write_mongo('denox', 'resultados_yago', resposta))
        resposta['_id'] = str(resposta['_id'])
        self.write(resposta)


def getDistances(dataframe):
    distances = []
    anterior = None
    for row in dataframe.itertuples(index=False):
        if anterior:
            distances.append(
                haversine_distance(anterior.latitude, anterior.longitude,
                                   row.latitude, row.longitude)
            )
        else:
            distances.append(0)
        anterior = row
    return distances


def read_mongo(dbName, collectionName, query={}, host='localhost', port=27017, username=None, password=None):
    db = _connect_mongo(host=host, port=port,
                        username=username, password=password, db=dbName)
    cursor = db[collectionName].find(query)
    df = pd.DataFrame(list(cursor))
    return df


def write_mongo(dbName, collectionName, data={}, host='localhost', port=27017, username=None, password=None):
    db = _connect_mongo(host=host, port=port,
                        username=username, password=password, db=dbName)
    collection = db[collectionName]
    collection.insert_one(data)
    # return str(inserted.inserted_id)


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
    if username and password:
        mongo_uri = 'mongodb://%s:%s@%s:%s/%s' % (
            username, password, host, port, db)
        conn = MongoClient(mongo_uri)
    else:
        conn = MongoClient(host, port)
    return conn[db]
