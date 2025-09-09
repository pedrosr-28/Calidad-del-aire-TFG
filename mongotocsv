from pymongo import MongoClient
import pandas as pd
import re, os
from datetime import datetime
from parseArgs import parse_args_descargarDatosMongoDB as parse_args, parse_date
import pytz
from collections import defaultdict

# Inputs
args = parse_args()
start_date = parse_date(args.start, datetime(2024, 1, 1))
end_date = parse_date(args.stop, datetime.today())

# Connection parameters
uri = X
client = MongoClient(uri)
print('Connected to client')

CaliDict = {
    "ciudad": "Cali",
    "db": "AirQuality-Colombia",
    "collection": "scrapping-cali",
}
BogotaDict = {
    "ciudad": "Bogota",
    "db": "AirQuality-Colombia",
    "collection": "scrapping-bogota",
}
MedellinDict = {
    "ciudad": "Medellin",
    "db": "AirQuality-Colombia",
    "collection": "scrapping-medellin",
}
CartagenaDict = {
    "ciudad": "Cartagena",
    "db": "AirQuality-Spain",
    "collection": "scrapping-cartagena",
}
MurciaDict = {
    "ciudad": "Murcia",
    "db": "AirQuality-Spain",
    "collection": "scrapping-murcia",
}
SevillaDict = { 
    "ciudad": "Sevilla",
    "db": "AirQuality-Spain",
    "collection": "scrapping-sevilla",
}
MadridDict = {
    "ciudad": "Madrid",
    "db": "AirQuality-Spain",
    "collection": "scrapping-madrid",
}
LondonDict = {
    "ciudad": "London",
    "db": "AirQuality-UnitedKingdom",
    "collection": "scrapping-london",
}

def data_max(data):
    # Paso 1: Preprocesar cada documento
    for doc in data:
        topic = doc.get('tags', {}).get('topic', '')
        topic_sin_num = topic.rsplit('/', 1)[0]  # quitar /1, /2, etc.
        
        if 'tags' in doc:
            doc['tags']['topic'] = topic_sin_num
        
        ts = doc.get('object_timestamp')
        if isinstance(ts, (int, float)):
            dt = datetime.utcfromtimestamp(ts)
        else:
            dt = pd.to_datetime(ts)
        doc['hora'] = dt.replace(minute=0, second=0, microsecond=0)  # truncar a la hora

    # Paso 2: Agrupar documentos por (topic, hora)
    grupos = defaultdict(list)
    for doc in data:
        clave = (doc['tags'].get('topic', ''), doc['hora'])
        grupos[clave].append(doc)


    campos_contaminantes = ['object_pm25', 'object_pm10', 'object_no2', 'object_co', 'object_so2', 'object_o3', 'object_coAQI_8h', 'object_no2AQI_1h', 'object_so2AQI', 'object_pm10AQI_24h', 'object_pm25AQI_24h', 'object_o3AQI', 'object_AQI' ]

    data_filtrada = [maximos_por_contaminante(grupo, campos_contaminantes) for grupo in grupos.values()]
    return data_filtrada

def maximos_por_contaminante(grupo, campos):
    resultado = {}

    # Para campos no contaminantes, puedes tomar del primer documento
    otros_campos = {k: v for k, v in grupo[0].items() if k not in campos + ['hora']}

    for doc in grupo:
        for c in campos:
            val = doc.get(c)
            if isinstance(val, (int, float)):
                if c not in resultado or val > resultado[c]:
                    resultado[c] = val

    # Combinar resultados con otros campos
    resultado.update(otros_campos)
    timestamp_utc = grupo[0]['hora'].replace(tzinfo=pytz.UTC)
    timestamp_formateado = timestamp_utc.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    resultado['hora'] = timestamp_formateado  # o la hora común del grupo

    return resultado


for usedDict in [CaliDict, BogotaDict, MedellinDict, CartagenaDict, MurciaDict, SevillaDict, MadridDict, LondonDict]:
    # Seleccionar la base de datos
    db = client[usedDict['db']]

    # Seleccionar la coleccion
    collection = db[usedDict['collection']]

    # Leer los documentos de la coleccion
    print('Loading...')
    cursor = collection.find({"timestamp": {"$gte": start_date, "$lt": end_date}})

    # Convertir el cursor a una lista de diccionarios
    data2 = list(cursor)
    data = data_max(data2)
    print('Load complete')

    # Convertir la lista de diccionarios a un DataFrame de Pandas
    df = pd.DataFrame(data)
    print('dataset size',df.shape)


    # Editar columnas
    def extract_topic(tags):
        topic = tags.get('topic')
        return topic.split('/')[-1] if topic else None

    df['topic'] = df['tags'].apply(extract_topic).astype(str)
    df['timestamp'] = pd.to_datetime(df['hora'], format='%Y-%m-%dT%H:%M:%S.%fZ')

    columns_to_keep = ['timestamp','topic','object_pm25','object_pm10','object_no2','object_co','object_so2','object_o3','object_temp','object_lat','object_lon']
    df = df[[col for col in columns_to_keep if col in df.columns]]
    print(df.head(10))
    print(df.shape)

    # Save files
    csv_filename = f"datosOriginal{usedDict['ciudad']}{args.filenameSuffix}.csv"
    parquet_filename = f"datosOriginal{usedDict['ciudad']}{args.filenameSuffix}.parquet"
    if args.overwrite or not os.path.exists(csv_filename):
        df.to_csv(csv_filename)
    else:
        raise FileExistsError(f"File {csv_filename} already exists and overwrite is set to false.")
    if args.overwrite or not os.path.exists(parquet_filename):
        df.to_parquet(parquet_filename)
    else:
        raise FileExistsError(f"File {parquet_filename} already exists and overwrite is set to false.")
