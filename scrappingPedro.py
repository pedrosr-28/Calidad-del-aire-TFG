# -*- coding: latin-1 -*-

import requests
import json
import sys
from datetime import datetime, timedelta
import re
import ast
import time
from datetime import datetime
from math import radians, cos, sin, sqrt, atan2
import paho.mqtt.client as mqtt
import pytz


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}")


# Configuracion del cliente MQTT
mqtt_broker = "ipDelBroker"
mqtt_port = numeroDePuerto
mqtt_client = mqtt.Client()


def on_connect(client, userdata, flags, rc):
    if rc != 0:
        print("Failed to connect, return code %d\n", rc)

mqtt_client.on_connect = on_connect
mqtt_client.connect(mqtt_broker, mqtt_port, 60)
mqtt_client.loop_start()  # Iniciar bucle en segundo plano para el cliente MQTT


# --- Funcion principal que ejecuta todo el flujo ---
def ejecutar():
    archivo_txt = "ciudades.txt"

    # Obtener datos de las tres fuentes
    datos_openaq_raw = obtener_datos_openaq(archivo_txt)
    datos_aqiin_raw = obtener_datos_aqi_in(archivo_txt)
    datos_aqicn_raw = obtener_datos_aqicn(archivo_txt)

    # Normalizar claves (nombres de las ciudades) a minusculas
    datos_openaq = {k.lower(): v for k, v in datos_openaq_raw.items()}
    datos_aqiin = {k.lower(): v for k, v in datos_aqiin_raw.items()}
    datos_aqicn = {k.lower(): v for k, v in datos_aqicn_raw.items()}

    # combinar datos
    todas_las_ciudades = set(datos_openaq) | set(datos_aqiin) | set(datos_aqicn)

    total = 0

    for ciudad in todas_las_ciudades:
        raw_openaq = datos_openaq.get(ciudad, {})
        raw_aqi_in = datos_aqiin.get(ciudad, {})
        raw_aqicn  = datos_aqicn.get(ciudad, {})

        # Combinar datos en un solo diccionario
        datos_combinados = {}
        for d in (raw_openaq, raw_aqi_in, raw_aqicn):
            if isinstance(d, dict):
                datos_combinados.update(d)

        # Normalizar nombres y topics basados en coordenadas
        datos_combinados = normalizar_nombres(datos_combinados)

        new_data = {}
        for key, value in datos_combinados.items():
            match = re.match(r"TOPIC: (.+/)([^/]+)/(\d+)", key)
            if match:
                prefix, old_name, number = match.groups()
                new_name = value["object_name"].lower().replace(" ", "_")
                new_key = f"{prefix}{new_name}/{number}"
                new_data[new_key] = value
            else:
                new_data[key] = value
        

        # Publicar datos en MQTT
        for key, value in new_data.items():
            topic = key
            payload = json.dumps(value)
            print(f"Publicando en {topic}: {payload}")
            mqtt_client.publish(topic, payload)


# --- Funciones individuales por fuente ---
# Extraccion de datos de OpenAQ
def obtener_datos_openaq(archivo_txt):

    # API Key y headers
    headers = {
        'accept': 'application/json',
        'X-API-Key': 'APIdeOpenAQ'
    }

    # Funcion para cargar ciudades y paises desde un archivo txt
    def cargar_diccionario_entre_openaq(archivo_txt):
        contenido_bloque = []
        dentro_del_bloque = False

        with open(archivo_txt, "r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if linea == "OPENAQ":
                    if dentro_del_bloque:
                        break
                    else:
                        dentro_del_bloque = True
                        continue
                if dentro_del_bloque:
                    contenido_bloque.append(linea)

        bloque_texto = "\n".join(contenido_bloque)
        try:
            country_list = ast.literal_eval(bloque_texto)
            return country_list
        except Exception as e:
            return []

    # Carga de ciudades y paises desde el archivo
    archivo_txt = "ciudades.txt"
    lista_paises_ciudades = cargar_diccionario_entre_openaq(archivo_txt)

    # Obtener todos los paises disponibles en la API
    all_countries = []
    page = 1
    limit = 100

    while True:
        countries_url = 'https://api.openaq.org/v3/countries'
        countries_params = {
            'order_by': 'id',
            'sort_order': 'asc',
            'limit': limit,
            'page': page
        }

        response = requests.get(countries_url, headers=headers, params=countries_params)
        if response.status_code == 200:
            data = response.json()
            results = data.get('results', [])
            all_countries.extend(results)
            if len(results) < limit:
                break  # ultima pagina
            page += 1  # Pasa a la siguiente pagina
        elif response.status_code == 429:
            print("Demasiadas solicitudes. Esperando 20 segundos...")
            time.sleep(20)
        else:
            print(f"Error al consultar paises: {response.status_code}")
            break  

    datos_en_memoria1 = {}

    # Procesar cada (pais, ciudades) como entrada independiente
    for nombre_pais, lista_ciudades in lista_paises_ciudades:
        # Buscar el pais en la lista descargada de la API
        match = next((c for c in all_countries if c['name'] == nombre_pais), None)
        if not match:
            print(f"No se encontro el pais con nombre {nombre_pais}")
            continue

        # Extraemos el id del pais para consultar las ciudades
        country_id = match['id']
        final_data = {}
        params = {
            'page': 1,
            'order_by': 'id',
            'sort_order': 'asc',
            'country': nombre_pais,
            'countries_id': country_id
        }
        # De cada pais, obtenemos las ciudades
        country_data = []
        while True:
            response = requests.get('https://api.openaq.org/v3/locations', headers=headers, params=params)
            if response.status_code == 429:  # Si obtenemos un error 429
                print("Demasiadas solicitudes. Esperando 60 segundos...")
                time.sleep(20)  # Esperamos 20 segundos antes de hacer otra solicitud
            elif response.status_code != 200:
                print(f"Error {response.status_code}: {response.text}")
                break

            data = response.json()
            results = data.get('results', [])

            filtered_results = []

            # Filtra estaciones en regiones concretas del pais
            if lista_ciudades and lista_ciudades[0]:
                for location in results:
                    locality = location.get('locality') or ""
                    loc_name = location.get('name') or ""
                    datetime_last_obj = location.get('datetimeLast')
                    datetime_last = datetime_last_obj.get('utc') if datetime_last_obj else None

                    # Comprobar si la ciudad coincide con la localidad o el nombre de la estacion y la fecha de actualizacion es del mes actual
                    if datetime_last:
                        try:
                            last_update = datetime.strptime(datetime_last, "%Y-%m-%dT%H:%M:%SZ")
                            now = datetime.utcnow()
                            if last_update.year == now.year and last_update.month == now.month:
                                for city in lista_ciudades:
                                    if city.lower() == locality.lower() or city.lower() in loc_name.lower():
                                        filtered_results.append(location)
                                        break
                        except ValueError:
                            print(f"Formato de fecha invalido: {datetime_last}")
            else:
                print(f"No se encontro la ciudad {lista_ciudades[0]} en el pais {nombre_pais}. Se obtienen todas las estaciones del pais.")

            country_data.extend(filtered_results)
            # Si hay mas resultados, pasamos pagina y seguimos comparando
            if len(results) < 100:
                break
            params['page'] += 1

        # Procesar los datos de cada estacion
        for item in country_data:
            location_data = {}
            results_pollutants = []
            object_name = item.get('name')
            locality = item.get('locality')
            id = item.get('id')
            object_lat = item.get('coordinates', {}).get('latitude')
            object_long = item.get('coordinates', {}).get('longitude')
            datetime_last_obj = item.get('datetimeLast')
            datetime_last = datetime_last_obj.get('utc') if datetime_last_obj else None

            dt = datetime.strptime(datetime_last.replace("Z", ""), "%Y-%m-%dT%H:%M:%S")
            timestamp = dt.timestamp()

            locality_part = f"{(locality or '').lower()}/" if locality else ''
            location_key = f"TOPIC: {normalizar_texto(nombre_pais)}/{normalizar_texto(lista_ciudades[0])}/{locality_part}{normalizar_texto(object_name)}/1"

            location_data["object_name"] = normalizar_texto(object_name)
            location_data["object_source"] = "openaq.org"
            location_data["object_timestamp"] = int(timestamp)
            location_data["object_lat"] = object_lat
            location_data["object_lon"] = object_long

            sensores = item["sensors"]
            sensores_list = [
                {"id": sensor['id'], "name": sensor['parameter']['name'], "type": sensor['parameter']['units']}
                for sensor in sensores
            ]

            umbral_antiguedad = timedelta(days=2)

            # Obtener datos de contaminantes (ultimos valores)
            url = f"https://api.openaq.org/v3/locations/{id}/latest?limit=100&page=1"
            response = requests.get(url, headers=headers)
            if response.status_code == 429:  # Si obtenemos un error 429
                print("Demasiadas solicitudes. Esperando 60 segundos...")
                time.sleep(20)  # Esperamos 20 segundos antes de hacer otra solicitud
            elif response.status_code == 200:
                data = response.json()
                results = data.get('results', [])
                if results:
                    for result in results:
                        datetime_str = result.get('datetime', {}).get('utc')
                        if datetime_str:
                            # Convertir a objeto datetime
                            dt_utc = datetime.strptime(datetime_str, "%Y-%m-%dT%H:%M:%SZ")
                            # Comparar con el umbral
                            if datetime.utcnow() - dt_utc <= umbral_antiguedad:
                                results_pollutants.append({
                                    "value": result.get('value'),
                                    "id": result.get('sensorsId')
                                })

            for pollutant in results_pollutants:
                pollutant_id = pollutant['id']
                for sensor in sensores_list:
                    if pollutant_id == sensor['id']:
                        valor = pollutant['value']
                        tipo = sensor['type']
                        if sensor['name'] == "pm25" or sensor['name'] == "pm10" or sensor['name'] == "co" or sensor['name'] == "o3" or sensor['name'] == "no2" or sensor['name'] == "so2" or sensor['name'] == "relativehumidity" or sensor['name'] == "temperature":
                            
                            if tipo == "µg/m³":
                                tipo = "ug/m3"

                            # Pasamos a ppb si el sensor es de tipo co, no2, so2 u o3
                            if sensor['name']  == "co" or sensor['name'] == "no2" or sensor['name'] == "so2" or sensor['name'] == "o3":
                                valor = ugm3_to_ppb(pollutant['value'], sensor['name'])
                                tipo = "ppb"

                            contaminante = sensor['name'].lower()

                            if contaminante == "relativehumidity":
                                contaminante = "hum"
                            if contaminante == "temperature":
                                contaminante = "temp"

                            location_data[f"object_{contaminante}"] = valor
                            location_data[f"object_{contaminante}_unit"] = tipo

            final_data[location_key] = location_data

        first_city = lista_ciudades[0] if lista_ciudades and lista_ciudades[0] else nombre_pais
        filename = f"{first_city.replace(' ', '_').lower()}"

        # Guardar en el diccionario en lugar de un archivo
        datos_en_memoria1[filename] = final_data

    return datos_en_memoria1


def obtener_datos_aqi_in(archivo_txt):

    url = "https://api.aqi.in/api/v1/getMonitorsByCity"

    # Leer ciudades y paises desde un archivo
    def cargar_cities_stations(archivo_txt):
        procesar = False
        bloque_lista = []
        offsets_utc = {}
        ciudades_paises = {}

        with open(archivo_txt, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()

                # Saltar comentarios
                if line.startswith("#"):
                    continue

                # Delimitadores AQIIN
                if "AQIIN" in line:
                    if procesar:
                        break  # Segunda aparicion => fin
                    else:
                        procesar = True  # Primera aparicion => inicio
                        continue

                if procesar:
                    # Procesar linea UTC
                    if line.upper().startswith("UTC:"):
                        try:
                            datos_utc = re.findall(r'\(([^)]+)\)', line)
                            for item in datos_utc:
                                nombre, offset = map(str.strip, item.split(","))
                                clave = nombre.lower()
                                offsets_utc[clave] = int(offset)
                        except Exception as e:
                            print(f"Error procesando linea UTC: {line}\n{e}")
                        continue

                    # Acumular lineas de la lista
                    if line:
                        bloque_lista.append(line)

        # Unir bloque y tratar de interpretarlo
        try:
            texto_lista = "\n".join(bloque_lista)
        

            ciudades_paises = ast.literal_eval(texto_lista)
        except Exception as e:
            print(f"Error interpretando lista de ciudades:\n{e}")

        return ciudades_paises, offsets_utc


    archivo_txt = "ciudades.txt"  
    ciudades_paises, offsets_utc = cargar_cities_stations(archivo_txt)

    data = {}  
    content = {}

    for pais, ciudades in ciudades_paises:

        ciudad_referencia = ciudades[0].lower()  # Usar el primer pais como referencia
        offset = offsets_utc.get(pais.lower(), 0)


        for ciudad in ciudades:
            headers = {
            'cityname': ciudad,
            'Content-Type': 'application/json'
            }

            response = requests.get(url, headers=headers)

            # Verificar si la solicitud fue exitosa
            if response.status_code == 200:

                # Parseamos la respuesta JSON
                content = response.json()
        

                # Extraemos los datos de interes de "Locations"
                locations = content.get('Locations', [])

               

                for location in locations:
                    location_name = location.get('locationName')
                    lat = float(location.get('lat'))
                    lon = float(location.get('lon'))

                    updated_at = location.get('updated_at')
                    dt_local = datetime.strptime(updated_at, "%d %b %Y, %I:%M%p")
                    dt_utc = dt_local - timedelta(hours=offset)  # Usar el offset UTC del pais
                    # print(f"Fecha y hora local: {dt_local}, Fecha y hora UTC: {dt_utc} (Offset: {offset} horas)")
                    timestamp_utc = int(dt_utc.timestamp())

                    # Extraemos los componentes de aire
                    air_components = {comp['sensorName']: {'value': comp['sensorData'], 'unit': comp['sensorUnit']} for comp in location.get('airComponents', [])}

                    # Asignamos valores por defecto si no estan presentes en los componentes de aire
                    def get_component(name, default_value, default_unit):
                        component = air_components.get(name, {'value': default_value, 'unit': default_unit})
                        # Cambiar unidades de temperatura a celsius
                        if component['unit'] == '°C':
                            component['unit'] = 'celsius'
                        # Cambiar unidades de concentracion a ug/m3
                        if component['unit'] == 'µg/m³':
                            component['unit'] = 'ug/m3'
                        # Cambiar unidades de presion a mbar
                        if component['unit'] == 'hPa':
                            component['unit'] = 'mbar'
                        # Convertir valores de 0.0 a None
                        if component['value'] == 0.0:
                            component['value'] = None
                        return component

                    aqiUS = get_component('aqi', 0, '')
                    pm25 = get_component('pm25', 0.0, 'ug/m3')
                    pm10 = get_component('pm10', 0.0, 'ug/m3')
                    so2 = get_component('so2', 0.0, 'ppb')
                    no2 = get_component('no2', 0.0, 'ppb')
                    o3 = get_component('o3', 0.0, 'ppb')
                    co = get_component('co', 0.0, 'ppb')
                    temp = get_component('t', 0.0, 'celsius')
                    hum = get_component('h', 0, '%')
                    dew = get_component('dew', 0.0, 'celsius')
                    wind = get_component('wind', 0.0, 'm/s')
                    pres = get_component('pressure', 0.0, 'mbar')

                    # Asegurarnos de que las unidades esten correctamente asignadas
                    if dew['unit'] == '':
                        dew['unit'] = 'celsius'
                    if wind['unit'] == '':
                        wind['unit'] = 'm/s'
                    if pres['unit'] == '':
                        pres['unit'] = 'mbar'


                    location_key = f"TOPIC: {normalizar_texto(pais)}/{ normalizar_texto(ciudad_referencia)}/{ normalizar_texto(ciudad)}/{ normalizar_texto(location_name)}/2"

                    # Crear el objeto JSON
                    output = {
                        "object_name": normalizar_texto(location_name),
                        "object_source": "aqi.in",
                        "object_lat": lat,
                        "object_lon": lon,
                        "object_timestamp": timestamp_utc,
                        "object_aqiUS": aqiUS['value'],
                        "object_aqiUS_unit": aqiUS['unit'],
                        "object_pm25": pm25['value'],
                        "object_pm25_unit": pm25['unit'],
                        "object_pm10": pm10['value'],
                        "object_pm10_unit": pm10['unit'],
                        "object_so2": so2['value'],
                        "object_so2_unit": so2['unit'],
                        "object_no2": no2['value'],
                        "object_no2_unit": no2['unit'],
                        "object_o3": o3['value'],
                        "object_o3_unit": o3['unit'],
                        "object_co": co['value'],
                        "object_co_unit": co['unit'],
                        "object_temp": temp['value'],
                        "object_temp_unit": temp['unit'],
                        "object_hum": hum['value'],
                        "object_hum_unit": hum['unit'],
                        "object_dew": dew['value'],
                        "object_dew_unit": dew['unit'],
                        "object_wind": wind['value'],
                        "object_wind_unit": wind['unit'],
                        "object_pres": pres['value'],
                        "object_pres_unit": pres['unit']
                    }

                    if ciudad_referencia not in data:
                        data[ciudad_referencia] = {}

                    data[ciudad_referencia][location_key] = output

        datos_en_memoria2 = data

    return datos_en_memoria2


def obtener_datos_aqicn(archivo_txt):
    API_KEY = "APIaqicn"

    def cargar_diccionario_entre_openaq(archivo_txt):
        contenido_bloque = []
        dentro_del_bloque = False

        with open(archivo_txt, "r", encoding="utf-8") as f:
            for linea in f:
                linea = linea.strip()
                if linea == "AQICN":
                    if dentro_del_bloque:
                        break
                    else:
                        dentro_del_bloque = True
                        continue
                if dentro_del_bloque:
                    contenido_bloque.append(linea)

        bloque_texto = "\n".join(contenido_bloque)
        try:
            country_list = ast.literal_eval(bloque_texto)
            return country_list
        except Exception as e:
            print(f"Error al interpretar el bloque como lista de tuplas: {e}")
            return []

    # Devuelve cadena con URL tipo: ciudad/estacion
    def obtener_estaciones(abreviacion, pais, ciudad):
        url = f"https://api.waqi.info/search/?token={API_KEY}&keyword={ciudad}"

        try:
            respuesta = requests.get(url, timeout=10)
            data = respuesta.json()

            if "data" in data:
                # Filtrar las estaciones por pais
                return [
                    station["station"]["url"]
                    for station in data["data"]
                    if "station" in station and "url" in station["station"] and (
                        ("country" in station["station"] and station["station"]["country"].lower() == abreviacion.lower()) or
                        (station["station"]["url"].split("/")[0].lower() == pais.lower())
                    )
                ]
            else:
                return []
        except requests.RequestException as e:
            print(f"Error obteniendo estaciones de {ciudad}: {e}")
            return []

    def obtener_datos_api(ciudad):
        url = f"http://api.waqi.info/feed/{ciudad}/?token={API_KEY}"
        try:
            respuesta = requests.get(url, timeout=10)
            data = respuesta.json()
            if data["status"] == "ok":
                info = data["data"]
                try:
                    fecha_iso = info["time"]["iso"]
                    dt_local = datetime.fromisoformat(fecha_iso)
                    dt_utc = dt_local.astimezone(pytz.UTC)
                    timestamp = dt_utc.timestamp()
                except:
                    timestamp = info["time"]["v"]  # Si no hay fecha ISO, usar el valor de tiempo
                aqi = info["aqi"]
                lat, lon = info["city"]["geo"]
                object_name = info["city"]["name"]
                object_name = (object_name.split(',')[0].strip()).lower()
                contaminantes = ["pm25", "pm10", "o3", "no2", "so2", "co", "t", "h", "p", "w"]
                unidades = {"pm25": "AQI", "pm10": "AQI", "o3": "AQI", "no2": "AQI", "so2": "AQI", "co": "AQI", "t": "celsius", "h": "%", "p": "mbar", "w": "m/s"}
                resultados = {"object_name": normalizar_texto(object_name), "object_source": "aqicn.org", "object_timestamp": timestamp, "object_lat": lat, "object_lon": lon, "object_AQI": aqi, "object_AQI_unit": "AQI"}
                for c in contaminantes:
                    valor = info.get("iaqi", {}).get(c, {}).get("v", None)
                    if valor is not None:
                        if c == "t":
                            resultados["object_temp"] = valor
                            resultados["object_temp_unit"] = unidades[c]
                        elif c == "h":
                            resultados["object_hum"] = valor
                            resultados["object_hum_unit"] = unidades[c]
                        elif c == "p":
                            resultados["object_pres"] = valor
                            resultados["object_pres_unit"] = unidades[c]
                        elif c == "w":
                            resultados["object_wind"] = valor
                            resultados["object_wind_unit"] = unidades[c]
                        elif c == "pm25" or c == "pm10":
                            resultados[f"object_{c}AQI_24h"] = valor
                            resultados[f"object_{c}AQI_unit"] = unidades[c]
                        elif c == "no2":
                            resultados["object_no2AQI_1h"] = valor
                            resultados["object_no2AQI_unit"] = unidades[c]
                        elif c == "co":
                            resultados["object_coAQI_8h"] = valor
                            resultados["object_coAQI_unit"] = unidades[c]
                        elif c == "o3":
                            resultados["object_o3AQI"] = valor
                            resultados["object_o3AQI_unit"] = unidades[c]
                        elif c == "so2":
                            if valor >= 200:
                                resultados["object_so2AQI_24h"] = valor
                                resultados["object_so2AQI_unit"] = unidades[c]
                            else:
                                resultados["object_so2AQI_1h"] = valor
                                resultados["object_so2AQI_unit"] = unidades[c]
                return resultados
            else:
                return {"error": "No se pudieron obtener los datos"}
        except requests.RequestException as e:
            return {"error": str(e)}

    def guardar_datos_en_memoria(ciudad, datos, diccionario_destino):
        diccionario_destino[ciudad] = datos

    archivo_txt = "ciudades.txt"
    ciudades_base = cargar_diccionario_entre_openaq(archivo_txt)

    ciudades = {}

    # Obtener las estaciones dinamicamente para cada ciudad
    for abreviacion, pais, ciudad in ciudades_base:
        estaciones = obtener_estaciones(abreviacion, pais, ciudad)
        if estaciones:  # Solo añadir si hay estaciones
            ciudades[ciudad] = estaciones

    datos_en_memoria3 = {}

    for ciudad, estaciones in ciudades.items():
        all_datos_api = {}
        for estacion in estaciones:
            datos_api = obtener_datos_api(estacion)

            # Organizar disposicion del topic
            partes = estacion.split("/")
            if len(partes) == 2:
                subpartes = partes[1].split('-', 1)
                partes = [partes[0], subpartes[0], subpartes[1]]
                datos_api["object_name"] = subpartes[1].lower().replace(" ","_").replace("-","_")
            if len(partes) == 4:
                partes.pop(1)  # Elimina el segundo valor (indice 1)
            partes.insert(1, ciudad) # Inserta la ciudad en la segunda posicion
            estacion_limpia = "/".join(partes)
            all_datos_api[f"TOPIC: {estacion_limpia.replace(' ', '_').replace('-', '_').lower()}/3"] = datos_api
        guardar_datos_en_memoria(ciudad, all_datos_api, datos_en_memoria3)

    return datos_en_memoria3

# --- Funciones auxiliares ---
# Formula Haversine para calcular distancia en metros entre dos puntos geograficos
def distancia_metros(lat1, lon1, lat2, lon2):
    R = 6371000  # Radio de la Tierra en metros
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c

# Funcion para poner mismo nombre a mismas estaciones de distintas fuentes
def normalizar_nombres(datos):
    aqicn, openaq, aqiin = [], [], []
    actualizado_por = {}  # ID o id(obj) como bandera para evitar sobrescritura

    # Clasificar por fuente
    for obj in datos.values():
        try:
            lat = obj['object_lat']
            lon = obj['object_lon']
            fuente = obj['object_source']
            nombre = obj['object_name']
            if fuente == "aqicn.org":
                aqicn.append((lat, lon, nombre, obj))
            elif fuente == "openaq.org":
                openaq.append((lat, lon, nombre, obj))
            elif fuente == "aqi.in":
                aqiin.append((lat, lon, nombre, obj))
        except Exception as e:
            print(f"Error clasificando objeto: {e}")
            continue

    # Fase 1: aqicn → openaq / aqiin
    for lat_c, lon_c, nombre_c, obj_c in aqicn:
        # openaq
        mejor_o, mejor_d_o = None, float('inf')
        for lat_o, lon_o, _, obj_o in openaq:
            d = distancia_metros(lat_c, lon_c, lat_o, lon_o)
            if d < 500 and d < mejor_d_o:
                mejor_d_o = d
                mejor_o = obj_o
        if mejor_o:
            nombre_original = mejor_o["object_name"]
            mejor_o["object_name"] = nombre_c
            mejor_o["object_lat"] = lat_c
            mejor_o["object_lon"] = lon_c
            actualizado_por[id(mejor_o)] = "aqicn"

        # aqiin
        mejor_a, mejor_d_a = None, float('inf')
        for lat_a, lon_a, _, obj_a in aqiin:
            d = distancia_metros(lat_c, lon_c, lat_a, lon_a)
            if d < 500 and d < mejor_d_a:
                mejor_d_a = d
                mejor_a = obj_a
        if mejor_a:
            nombre_original = mejor_a["object_name"]
            mejor_a["object_name"] = nombre_c
            mejor_a["object_lat"] = lat_c
            mejor_a["object_lon"] = lon_c
            actualizado_por[id(mejor_a)] = "aqicn"

    # Fase 2: openaq → aqiin (solo si no ha sido actualizado por aqicn)
    for lat_o, lon_o, nombre_o, obj_o in openaq:
        mejor_a, mejor_d_a = None, float('inf')
        for lat_a, lon_a, _, obj_a in aqiin:
            if id(obj_a) in actualizado_por:
                continue  # ya actualizado por aqicn, no sobrescribir
            d = distancia_metros(lat_o, lon_o, lat_a, lon_a)
            if d < 500 and d < mejor_d_a:
                mejor_d_a = d
                mejor_a = obj_a
        if mejor_a:
            nombre_original = mejor_a["object_name"]
            mejor_a["object_name"] = nombre_o
            mejor_a["object_lat"] = lat_o
            mejor_a["object_lon"] = lon_o
            actualizado_por[id(mejor_a)] = "openaq"

    return datos

# Normaliza el texto: quita espacios, guiones y lo convierte a minusculas
def normalizar_texto(texto):
    return re.sub(r'[\s\-]+', '_', texto or '').strip('_').lower()

def ugm3_to_ppb(ugm3, contaminante):
    pesos_moleculares = {
        'co': 28.01,
        'no2': 46.01,
        'so2': 64.07,
        'o3': 48.00
    }

    contaminante = contaminante.lower()
    if contaminante not in pesos_moleculares:
        return None  # contaminante no soportado

    pm = pesos_moleculares[contaminante]
    ppb = (ugm3 * 24.45) / pm
    return round(ppb, 1)


# Funcion Bucle
if __name__ == "__main__":
    log("Inicio del script de scraping de datos de calidad del aire")
    inicio = time.time()
    ejecutar()  # Primera ejecucion inicial
    log("Primera ejecucion de datos completada, esperando 5 minutos antes de la siguiente ejecucion.")
   
    time.sleep(300)  
    while True:
        tiempo_transcurrido = time.time() - inicio
        tiempo_para_ejecutar = 3500 - tiempo_transcurrido
        if tiempo_para_ejecutar <= 0:
            log("Hora de ejecutar() ha llegado, ejecutando de nuevo...")
            # Ya ha pasado la hora, ejecutar ejecutar() y resetear el tiempo
            inicio = time.time()
            ejecutar()
            log("Ejecucion de datos completada, esperando 1h antes de la proxima ejecucion.")
            time.sleep(tiempo_para_ejecutar)
        else:
            log(f"Tiempo restante para la proxima ejecucion: {tiempo_para_ejecutar:.2f} segundos.")
            time.sleep(tiempo_para_ejecutar + 5)  # Espera hasta que sea hora de ejecutar de nuevo


