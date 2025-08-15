# -*- coding: latin-1 -*-

import folium
import os
import cv2
import time
import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

def redimensionar_cv2(img):
    height, width = img.shape[:2]
    new_width = ((width + 15) // 16) * 16
    new_height = ((height + 15) // 16) * 16
    return cv2.resize(img, (new_width, new_height))

# --- Configuracion y datos ---
contaminantes = ['co', 'pm25', 'pm10', 'no2', 'o3', 'so2']
ciudades = ['Cali', 'Bogota', 'Medellin', 'Madrid', 'Cartagena', 'Murcia', 'Sevilla', 'London']
horas = ['1h', '2h', '1d']

imagenes_dir = "/home/usuario/prediccionesPython/imagenes"
salida_dir = "/home/usuario/prediccionesPython/pedro"
screenshots_dir = os.path.join(salida_dir, "screenshots")
videos_dir = "/var/www/qartia/maps/generatedVideos"

os.makedirs(screenshots_dir, exist_ok=True)
os.makedirs(videos_dir, exist_ok=True)

def obtener_coords(ciudad):
    coords = {
        'Cali':      ((3.523918, -76.575961), (3.279561, -76.4568)),
        'Bogota':    ((4.824446174896161, -74.20667507828892), (4.5397692048476825, -74.0005320015009)),
        'Medellin':  ((6.349131881712998, -75.65659770930033), (6.155727548907033, -75.48383560122318)),
        'Madrid':    ((40.99513263469628, -4.323253097664286), (40.10050165392667, -3.194924166916184)),
        'Cartagena': ((37.709116757663374, -1.0969966393871564), (37.56502284133423, -0.9054225377768926)),
        'Murcia':    ((38.51625748003036, -1.9646254875938247), (37.58196804950511, -0.8258322339837023)),
        'Sevilla':   ((37.46163310797518, -6.067321540264034), (37.29921783810181, -5.822141287335516)),
        'London':    ((51.63363484575785, -0.523631062229996), (51.422331505996674, 0.7496885004871515))
    }
    if ciudad not in coords:
        raise ValueError(f"Ciudad no reconocida: {ciudad}")
    return coords[ciudad]

def generar_mapa_con_imagen(ciudad, contaminante, imagen_path, output_html):
    esquina_sup_izq, esquina_inf_der = obtener_coords(ciudad)

    lat_margin = abs(esquina_sup_izq[0] - esquina_inf_der[0]) * 0.05
    lon_margin = abs(esquina_sup_izq[1] - esquina_inf_der[1]) * 0.2

    esquina_sup_izq_expandida = (
        esquina_sup_izq[0] + lat_margin,
        esquina_sup_izq[1] - lon_margin
    )
    esquina_inf_der_expandida = (
        esquina_inf_der[0] - lat_margin,
        esquina_inf_der[1] + lon_margin
    )
    mapa = folium.Map()
    mapa.fit_bounds([esquina_inf_der_expandida, esquina_sup_izq_expandida])

    if os.path.exists(imagen_path):
        folium.raster_layers.ImageOverlay(
            name=f"{contaminante.upper()} - {ciudad}",
            image=imagen_path,
            bounds=[esquina_inf_der, esquina_sup_izq],
            opacity=0.6,
            interactive=True,
            cross_origin=False
        ).add_to(mapa)
        folium.LayerControl().add_to(mapa)
        mapa.save(output_html)
        print(f"Mapa guardado: {output_html}")
    else:
        print(f"No se encontro la imagen para el mapa: {imagen_path}")

# --- Configuración Selenium ---
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--force-device-scale-factor=2")
chrome_options.add_argument("--window-size=1100,900")

driver = webdriver.Chrome(options=chrome_options)

# --- Diccionario para acumular frames por ciudad ---
frames_por_ciudad = {ciudad: [] for ciudad in ciudades}

# --- Proceso principal ---
for contaminante in contaminantes:
    for ciudad in ciudades:
        frames = []
        print(f"\nProcesando contaminante {contaminante.upper()} en ciudad {ciudad}")

        for hora in horas:
            imagen_hora = os.path.join(imagenes_dir, f"{contaminante.upper()}_{ciudad}_{hora}_isolines.png")
            html_salida = os.path.join(salida_dir, f"mapa_{contaminante}_{ciudad}_{hora}.html")
            generar_mapa_con_imagen(ciudad, contaminante, imagen_hora, html_salida)

        for hora in horas:
            html_path = os.path.join(salida_dir, f"mapa_{contaminante}_{ciudad}_{hora}.html")
            screenshot_path = os.path.join(screenshots_dir, f"{contaminante}_{ciudad}_{hora}.png")

            if os.path.exists(html_path):
                driver.get(f"file://{html_path}")
                time.sleep(2)
                driver.save_screenshot(screenshot_path)
                print(f"Captura guardada: {screenshot_path}")

                # Leer y modificar imagen con OpenCV
                img = cv2.imread(screenshot_path)
                if img is None:
                    print(f"No se pudo leer la captura: {screenshot_path}")
                    continue  

                # texto con contaminante, ciudad y hora
                text = f"{contaminante.upper()} - {ciudad} - {hora}"
                font = cv2.FONT_HERSHEY_SIMPLEX
                font_scale = 3
                thickness = 6
                color = (0, 0, 0)  
                line_type = cv2.LINE_AA
                
                img_height, img_width = img.shape[:2]
                (text_size, _) = cv2.getTextSize(text, font, font_scale, thickness)
                text_width, text_height = text_size
                text_x = (img_width - text_width) // 2
                text_y = 90  

                cv2.putText(img, text, (text_x, text_y), font, font_scale, color, thickness, line_type)
                print(f"Texto agregado: {text}")

                # texto con fecha de la imagen
                imagen_hora = os.path.join(imagenes_dir, f"{contaminante.upper()}_{ciudad}_{hora}_isolines.png")
                if os.path.exists(imagen_hora):
                    timestamp = os.path.getmtime(imagen_hora)
                    fecha = datetime.datetime.fromtimestamp(timestamp).replace(minute=0, second=0, microsecond=0)
                    fecha_str = fecha.strftime("%Y-%m-%d %H:%M")
                    fecha_text = f"Fecha: {fecha_str}"

                    fecha_x = 90
                    fecha_y = 70
                    cv2.putText(img, fecha_text, (fecha_x, fecha_y), font, 1, color, 5, line_type)
                    print(f"Fecha agregada: {fecha_text}")
                

                # Redimensionar 
                img = redimensionar_cv2(img)
                cv2.imwrite(screenshot_path, img)

                frames.append(img)
            else:
                print(f"Archivo HTML no encontrado: {html_path}")

        frames_por_ciudad[ciudad].extend(frames)

# Crear un unico video por ciudad 
for ciudad, frames in frames_por_ciudad.items():
    if frames:
        print(f"Frames generados para la ciudad: {ciudad}, total frames: {len(frames)}")
        height, width = frames[0].shape[:2]
        video_ciudad = os.path.join(videos_dir, f"{ciudad}_timelapse.mp4")

        fourcc = cv2.VideoWriter_fourcc(*'VP90')
        out = cv2.VideoWriter(video_ciudad, fourcc, 0.5, (width, height))

        for frame in frames:
            out.write(frame)

        out.release()
        print(f"Video de ciudad guardado: {video_ciudad}")
    else:
        print(f"No se generaron frames para la ciudad: {ciudad}")

# --- Cerrar Selenium ---
driver.quit()
 
