#!/usr/bin/env python3

import sys
import re
import time
from datetime import datetime
from influxdb_client import InfluxDBClient

# Configuraciones de InfluxDB
INFLUXDB_URL = "http://127.0.0.1:8086"
INFLUXDB_TOKEN = ""
INFLUXDB_ORG = "qartia"
client = InfluxDBClient(url=INFLUXDB_URL, token=INFLUXDB_TOKEN, org=INFLUXDB_ORG)

LOG_FILE_PATH = "/etc/scraping/aqiCalculation/datos.log"

# DefiniciÃ³n de campos para gases y partÃ­culas
GAS_FIELDS = ["object_co", "object_so2", "object_o3", "object_no2"]
PARTICLE_FIELDS = ["object_pm25", "object_pm10"]
FIELDS_TO_QUERY = GAS_FIELDS + PARTICLE_FIELDS

AQI_BREAKPOINTS = {
    "pm25": [
        (0.0, 12.0, 0, 50),
        (12.1, 35.4, 51, 100),
        (35.5, 55.4, 101, 150),
        (55.5, 150.4, 151, 200),
        (150.5, 250.4, 201, 300),
        (250.5, 350.4, 301, 400),
        (350.5, 500.4, 401, 500),
    ],
    "pm10": [
        (0, 54, 0, 50),
        (55, 154, 51, 100),
        (155, 254, 101, 150),
        (255, 354, 151, 200),
        (355, 424, 201, 300),
        (425, 504, 301, 400),
        (505, 604, 401, 500),
    ],
    "no2": [
        (0, 53, 0, 50),
        (54, 100, 51, 100),
        (101, 360, 101, 150),
        (361, 649, 151, 200),
        (650, 1249, 201, 300),
        (1250, 1649, 301, 400),
        (1650, 2049, 401, 500),
    ],
    "so2_1h": [
        (0, 35, 0, 50),
        (36, 75, 51, 100),
        (76, 185, 101, 150),
        (186, 304, 151, 200),
    ],
    "so2_24h": [
        (0, 304, 0, 200),
        (305, 604, 201, 300),
        (605, 804, 301, 400),
        (805, 1004, 401, 500),
    ],
    "co": [
        (0.0, 4.4, 0, 50),
        (4.5, 9.4, 51, 100),
        (9.5, 12.4, 101, 150),
        (12.5, 15.4, 151, 200),
        (15.5, 30.4, 201, 300),
        (30.5, 40.4, 301, 400),
        (40.5, 50.4, 401, 500),
    ],
    "o3_8h": [
        (0.000, 0.054, 0, 50),
        (0.055, 0.070, 51, 100),
        (0.071, 0.085, 101, 150),
        (0.086, 0.105, 151, 200),
        (0.106, 0.200, 201, 300),
    ],
    "o3_1h": [
        (0.125, 0.164, 101, 150),
        (0.165, 0.204, 151, 200),
        (0.205, 0.404, 201, 300),
        (0.405, 0.504, 301, 400),
        (0.505, 0.604, 401, 500),
    ]
}


def parse_fields_from_line(line, fields_to_query):
    #Extrae los campos presentes en la lÃ­nea del STDIN.
    field_values = {}
    for field in fields_to_query:
        match = re.search(fr"{field}=([\d.]+)", line)  # Busca nÃºmeros (enteros o flotantes)
        if match:
            field_values[field] = float(match.group(1))  # Almacena como float
    return field_values

def query_influxdb_for_topic(bucket, measurement, topic, field_values_dict):
    results = {}

    for field, nuevo_valor in field_values_dict.items():
        results[field] = {}
        try:
            for period in ['1h', '8h', '24h']:
                # Consulta para la media
                query_mean = f'''
                from(bucket: "{bucket}")
                  |> range(start: -{period})
                  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                  |> filter(fn: (r) => r["topic"] == "{topic}")
                  |> filter(fn: (r) => r["_field"] == "{field}")
                  |> filter(fn: (r) => r["_value"] >= 0)
                  |> mean()
                '''
                # Consulta para el nÃºmero de puntos
                query_count = f'''
                from(bucket: "{bucket}")
                  |> range(start: -{period})
                  |> filter(fn: (r) => r["_measurement"] == "{measurement}")
                  |> filter(fn: (r) => r["topic"] == "{topic}")
                  |> filter(fn: (r) => r["_field"] == "{field}")
                  |> filter(fn: (r) => r["_value"] >= 0)
                  |> count()
                '''

                # Ejecutar ambas
                result_mean = client.query_api().query(query_mean, org=INFLUXDB_ORG)
                result_count = client.query_api().query(query_count, org=INFLUXDB_ORG)

                media_anterior = None
                n_anterior = 0

                for table in result_mean:
                    for record in table.records:
                        media_anterior = record.get_value()

                for table in result_count:
                    for record in table.records:
                        n_anterior = record.get_value()

                if media_anterior is not None and n_anterior > 0:
                    media_nueva = (media_anterior * n_anterior + nuevo_valor) / (n_anterior + 1)
                else:
                    media_nueva = nuevo_valor  # si no hay datos previos

                key = f"{field}_{period}"
                results[field][key] = media_nueva

        except Exception as e:
            error_msg = f"Error querying InfluxDB for field {field}: {e}"
            print(error_msg, file=sys.stderr)
            sys.stderr.flush()
    return results


def preparar_concentracion(contaminante, valor_ppb):
    #Convierte concentraciones de ppb a ppm para O3 y CO.
    if contaminante.startswith("o3") or contaminante == "co":
        converted = valor_ppb / 1000
        return converted
    return valor_ppb

def calculate_individual_aqi(pollutant, concentration, period=None):
    #Calcula el AQI para un contaminante segÃºn los breakpoints.
    key = pollutant if not period else f"{pollutant}_{period}"

    if key not in AQI_BREAKPOINTS:
        return None

    breakpoints = AQI_BREAKPOINTS[key]
    for (c_low, c_high, i_low, i_high) in breakpoints:
        if c_low <= concentration <= c_high:
            aqi = ((i_high - i_low) / (c_high - c_low)) * (concentration - c_low) + i_low
            return round(aqi)
    return None

def calculate_aqi_from_fields(fields_time):
    #Calcula los valores de AQI para todos los contaminantes.
    aqi_results = {}
    aqi_max = {}

    pm25_24h = fields_time.get("object_pm25_24h")
    if pm25_24h is not None:
        aqi_results["object_pm25AQI_24h"] = calculate_individual_aqi("pm25", float(pm25_24h), None)
        aqi_max["object_pm25AQI_24h"] = aqi_results["object_pm25AQI_24h"]

    pm10_24h = fields_time.get("object_pm10_24h")
    if pm10_24h is not None:
        aqi_results["object_pm10AQI_24h"] = calculate_individual_aqi("pm10", float(pm10_24h), None)
        aqi_max["object_pm10AQI_24h"] = aqi_results["object_pm10AQI_24h"]

    o3_8h = fields_time.get("object_o3_8h")
    if o3_8h is not None:
        o3_8h = preparar_concentracion("o3", float(o3_8h))
        aqi_results["object_o3AQI_8h"] = calculate_individual_aqi("o3", o3_8h, "8h")

    o3_1h = fields_time.get("object_o3_1h")
    if o3_1h is not None:
        o3_1h = preparar_concentracion("o3", float(o3_1h))
        aqi_results["object_o3AQI_1h"] = calculate_individual_aqi("o3", o3_1h, "1h")


    o3AQI_1h = aqi_results.get("object_o3AQI_1h")
    o3AQI_8h = aqi_results.get("object_o3AQI_8h")
    if o3AQI_1h is not None and o3AQI_8h is not None:
        o3 = max(float(o3AQI_1h), float(o3AQI_8h))
    elif o3AQI_8h is not None:
        o3 = float(o3AQI_8h)
    elif o3AQI_1h is not None:
        o3 = float(o3AQI_1h)
    else:
        o3 = None
    aqi_results["object_o3AQI"] = o3
    aqi_max["object_o3AQI"] = o3

    no2_1h = fields_time.get("object_no2_1h")
    if no2_1h is not None:
        aqi_results["object_no2AQI_1h"] = calculate_individual_aqi("no2", float(no2_1h), None)
        aqi_max["object_no2AQI_1h"] = aqi_results["object_no2AQI_1h"]

    co_8h = fields_time.get("object_co_8h")
    if co_8h is not None:
        co_8h = preparar_concentracion("co", float(co_8h))
        aqi_results["object_coAQI_8h"] = calculate_individual_aqi("co", co_8h, None)
        aqi_max["object_coAQI_8h"] = aqi_results["object_coAQI_8h"]

    so2_1h = fields_time.get("object_so2_1h")
    if so2_1h is not None:
        aqi_results["object_so2AQI_1h"] = calculate_individual_aqi("so2", float(so2_1h), "1h")

    so2_24h = fields_time.get("object_so2_24h")
    if so2_24h is not None:
        aqi_results["object_so2AQI_24h"] = calculate_individual_aqi("so2", float(so2_24h), "24h")

    so2 = None
    so2AQI_1h = aqi_results.get("object_so2AQI_1h")
    so2AQI_24h = aqi_results.get("object_so2AQI_24h")
    if so2AQI_1h is not None and so2_24h is not None:
        if float(so2_1h) < 305:
            so2 = so2AQI_1h
        elif float(so2_24h) <= 305:
            so2 = so2AQI_24h
        elif float(so2_1h) >= 305 and float(so2_24h) < 305:
            so2 = 200
    elif so2AQI_1h is not None:
        so2 = so2AQI_1h
    elif so2AQI_24h is not None:
        so2 = so2AQI_24h
    aqi_results["object_so2AQI"] = so2
    aqi_max["object_so2AQI"] = so2

    # Filtrar valores None para calcular el AQI total
    valid_aqi_values = [v for v in aqi_max.values() if v is not None]
    aqi_total = max(valid_aqi_values) if valid_aqi_values else None
    aqi_results["object_AQI"] = aqi_total
    return aqi_results

if __name__ == "__main__":
    sys.stderr.flush()
    while True:
        line = sys.stdin.readline().strip()
        if line == "":
            print("Empty line received, continuing...", file=sys.stderr)
            sys.stderr.flush()
            continue
        try:
            print(f"Processing line: {line}", file=sys.stderr)
            sys.stderr.flush()

            # Extraer el bucket, _measurement, y topic de la lÃ­nea de entrada
            bucket_match = re.search(r"bucket=([\w\-]+)", line)
            if not bucket_match:
                error_msg = "No bucket found in the line. Skipping."
                print(error_msg, file=sys.stderr)
                sys.stderr.flush()
                continue
            bucket = bucket_match.group(1)

            measurement_match = re.search(r"^([\w\-]+),", line)
            if not measurement_match:
                error_msg = "No measurement found in the line. Skipping."
                print(error_msg, file=sys.stderr)
                sys.stderr.flush()
                continue
            measurement = measurement_match.group(1)

            topic_match = re.search(r"topic=([\w/]+)", line)
            topic = topic_match.group(1) if topic_match else None
            if not topic:
                error_msg = "No topic found in the line. Skipping."
                print(error_msg, file=sys.stderr)
                sys.stderr.flush()
                continue
            elif topic.endswith("/3"):
                print(line)
                sys.stdout.flush()
                continue

            # Identificar campos presentes en la lÃ­nea
            field_values = parse_fields_from_line(line, FIELDS_TO_QUERY)
            if not field_values:
                error_msg = "No relevant fields found in the line. Skipping."
                print(error_msg, file=sys.stderr)
                sys.stderr.flush()
                continue

            # Consultar InfluxDB para los campos detectados
            mean_values = query_influxdb_for_topic(bucket, measurement, topic, field_values)

            # Calcular AQI para los valores obtenidos
            fields_time = {}
            for field, values in mean_values.items():
                for key, value in values.items():
                    fields_time[key] = value
            aqi_values = calculate_aqi_from_fields(fields_time)

            # Convertir los valores de AQI en formato cadena
            aqi_values_str = ""
            for key, value in aqi_values.items():
                if value is not None:
                    aqi_values_str += f",{key}={value}"

            # Dividir la lÃ­nea original en partes para que la marca de tiempo quede al final
            parts = line.split(" ")
            timestamp = parts[-1]
            data_fields = " ".join(parts[:-1])

            # Concatenar los nuevos campos antes de la marca de tiempo
            output_line = f"{data_fields}{aqi_values_str} {timestamp}"

            # Escribir en datos.log antes de imprimir en stdout
            try:
                with open(LOG_FILE_PATH, "a") as log_file:
                    log_file.write(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] OUTPUT: {output_line}\n")
                    log_file.flush()
            except Exception as e:
                error_msg = f"Failed to write output to {LOG_FILE_PATH}: {e}"
                print(error_msg, file=sys.stderr)
                sys.stderr.flush()

            # Imprimir la metrica procesada en el STDOUT para que Telegraf la procese
            print(output_line)
            sys.stdout.flush()

        except Exception as e:
            error_msg = f"Error processing line: {e}"
            print(error_msg, file=sys.stderr)
            sys.stderr.flush()
