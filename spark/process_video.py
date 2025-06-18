import os
import sys
import cv2
import numpy as np
import json
import datetime
import random
import uuid
import subprocess
from collections import defaultdict
from pyspark.sql import SparkSession
from elasticsearch import Elasticsearch, helpers # <--- NUEVA IMPORTACIÓN

# ==============================================================================
# PROCESADOR DE VIDEO INCREMENTAL Y ROBUSTO CON SPARK E INTEGRACIÓN ELASTICSEARCH
# Formato de Salida: JSON anidado por video_id y carga directa a Elasticsearch
# Versión Integrada
# ==============================================================================

# --- Rutas de los archivos del modelo (distribuidos por --files) ---
PROTOTXT_PATH = "MobileNetSSD_deploy.prototxt"
MODEL_PATH = "MobileNetSSD_deploy.caffemodel"
CLASSES_FILE_PATH = "mobelenet_classes.txt"

# --- Parámetros de optimización de Spark/OpenCV ---
OPTIMIZATION_FRAMES_SKIP_FACTOR = 30
CONFIDENCE_THRESHOLD = 0.7
OPTIMIZATION_INFERENCE_RESOLUTION = (300, 300)

# --- Configuración de Elasticsearch (NUEVA SECCIÓN) ---
ES_HOST = "http://localhost:9200"
ES_API_KEY = "cVJreWhKY0JrYUxvRVZNNlhfYmE6dFFqR3E3bmpKeFRyeDVJRWd5dkZadw==" # Asegúrate de que esta clave sea correcta
ES_INDEX_NAME = "videos"
ES_VIDEO_BASE_URL = "http://localhost:8080/videos" # URL base para los archivos de video

# --- Carga de las etiquetas de las clases ---
CLASSES = ["background", "aeroplane", "bicycle", "bird", "boat", "bottle", "bus", "car", "cat", "chair", 'cow', 'diningtable', 'dog', 'horse', 'motorbike', 'person', 'pottedplant', 'sheep', 'sofa', 'train', 'tvmonitor']


# ==============================================================================
# LÓGICA DEL WORKER (sin cambios, se ejecuta en paralelo en los nodos del clúster)
# ==============================================================================

_mobilenet_model = None # Modelo global para cargar una vez por worker

def _initialize_mobilenet_model():
    """Inicializa el modelo MobileNet SSD una vez por proceso worker."""
    global _mobilenet_model
    if _mobilenet_model is None:
        print(f"Cargando modelo MobileNet en worker PID: {os.getpid()}...")
        try:
            _mobilenet_model = cv2.dnn.readNetFromCaffe(PROTOTXT_PATH, MODEL_PATH)
            print(f"Worker {os.getpid()}: Modelo cargado exitosamente.")
        except Exception as e:
            print(f"FATAL: No se pudo cargar el modelo en el worker: {e}")
            _mobilenet_model = "error_loading_model"
    return _mobilenet_model

def process_video_and_return_dict(video_path_hdfs):
    """
    Procesa un video. Primero lo copia de HDFS a un disco local temporal
    usando el comando `hdfs dfs`, y LUEGO lo procesa con OpenCV.
    """
    video_id = os.path.basename(video_path_hdfs)
    local_temp_path = f"/tmp/{uuid.uuid4()}_{video_id}"
    
    try:
        print(f"Worker {os.getpid()}: Intentando copiar desde HDFS '{video_path_hdfs}' a local '{local_temp_path}'")
        command = ["hdfs", "dfs", "-copyToLocal", video_path_hdfs, local_temp_path]
        subprocess.run(command, check=True, capture_output=True, text=True)
        print(f"Worker {os.getpid()}: Copia de '{video_id}' exitosa.")
    except subprocess.CalledProcessError as e:
        print(f"ERROR: El comando 'hdfs dfs -copyToLocal' falló en el worker para el video {video_id}. Stderr: {e.stderr}")
        return {video_id: {"error": f"Failed to copy from HDFS. HDFS command error: {e.stderr}"}}
    except FileNotFoundError:
        print("ERROR CRÍTICO: El comando 'hdfs' no se encontró en el PATH del worker de YARN.")
        return {video_id: {"error": "HDFS command not found in YARN container."}}

    result_data = {}
    try:
        model = _initialize_mobilenet_model()
        if model == "error_loading_model":
            return {video_id: {"error": "Model loading failed on worker"}}

        cap = cv2.VideoCapture(local_temp_path)
        if not cap.isOpened():
            return {video_id: {"error": f"Could not open local temp file after copy: {local_temp_path}"}}

        object_counts = defaultdict(int)
        frame_counter = 0
        while True:
            ret, frame = cap.read()
            if not ret: break
            if frame_counter % OPTIMIZATION_FRAMES_SKIP_FACTOR != 0:
                frame_counter += 1
                continue
            
            blob = cv2.dnn.blobFromImage(cv2.resize(frame, OPTIMIZATION_INFERENCE_RESOLUTION), 0.007843, OPTIMIZATION_INFERENCE_RESOLUTION, 127.5)
            model.setInput(blob)
            detections = model.forward()
            for i in np.arange(0, detections.shape[2]):
                confidence = detections[0, 0, i, 2]
                if confidence > CONFIDENCE_THRESHOLD:
                    idx = int(detections[0, 0, i, 1])
                    object_counts[CLASSES[idx]] += 1
            frame_counter += 1
        
        cap.release()
        print(f"Worker {os.getpid()}: Procesó el video: {video_id}")
        
        result_data = {
            "processing_time": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "priority": random.choice(["High", "Mid", "Low"]),
            "detected_objects": dict(object_counts) if object_counts else {"no_objects_detected": 0}
        }
    
    finally:
        if os.path.exists(local_temp_path):
            print(f"Worker {os.getpid()}: Limpiando archivo temporal '{local_temp_path}'")
            os.remove(local_temp_path)
            
    return {video_id: result_data}

# ==============================================================================
# LÓGICA DEL DRIVER (orquesta el trabajo y ahora también indexa en Elasticsearch)
# ==============================================================================

# --- Funciones de manejo de archivos (sin cambios) ---
def load_master_results(json_path):
    if os.path.exists(json_path) and os.path.getsize(json_path) > 0:
        with open(json_path, 'r') as f: return json.load(f)
    return {}

def save_master_results(data, json_path):
    with open(json_path, 'w') as f: json.dump(data, f, indent=4)
    print(f"Resultados maestros guardados exitosamente en '{json_path}'")

def get_videos_from_hdfs(hdfs_directory_path):
    print(f"Obteniendo lista de archivos del directorio HDFS: '{hdfs_directory_path}'")
    command = ["hdfs", "dfs", "-ls", "-C", hdfs_directory_path]
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        video_paths = [path for path in result.stdout.strip().split('\n') if path]
        if not video_paths:
            print(f"Advertencia: No se encontraron archivos en el directorio HDFS: '{hdfs_directory_path}'")
            return []
        print(f"Se encontraron {len(video_paths)} archivos en HDFS.")
        return video_paths
    except subprocess.CalledProcessError as e:
        print(f"ERROR: El comando para listar archivos en HDFS falló. Stderr: {e.stderr}")
        return []
    except FileNotFoundError:
        print("ERROR CRÍTICO: El comando 'hdfs' no se encontró en el PATH del driver.")
        return []

# --- Lógica de Elasticsearch (NUEVA SECCIÓN) ---

def setup_elasticsearch():
    """
    Se conecta a Elasticsearch y crea el índice si no existe.
    No borra el índice si ya existe para permitir la carga incremental.
    """
    print("Conectando a Elasticsearch...")
    es_client = Elasticsearch(ES_HOST, api_key=ES_API_KEY)
    
    if not es_client.indices.exists(index=ES_INDEX_NAME):
        print(f"El índice '{ES_INDEX_NAME}' no existe. Creándolo con el mapeo adecuado...")
        mapping = {
            "mappings": {
                "properties": {
                    "id": {"type": "keyword"},
                    "filename": {"type": "text"},
                    "url": {"type": "keyword"},
                    "duration": {"type": "float"},
                    "objects_detected": {
                        "type": "nested",
                        "properties": {
                            "type": {"type": "keyword"},
                            "count": {"type": "integer"}
                        }
                    },
                    "date": {"type": "date"},
                    "location": {"type": "keyword"},
                    "resolution": {"type": "keyword"},
                    "codec": {"type": "keyword"},
                    "processing_time": {"type": "date", "format": "date_optional_time"},
                    "priority": {"type": "keyword"}
                }
            }
        }
        es_client.indices.create(index=ES_INDEX_NAME, body=mapping)
        print(f"Índice '{ES_INDEX_NAME}' creado exitosamente.")
    else:
        print(f"El índice '{ES_INDEX_NAME}' ya existe. No se realizarán cambios en el mapeo.")
        
    return es_client

def index_new_results_to_es(es_client, new_results_list):
    """
    Transforma y carga una lista de nuevos resultados a Elasticsearch
    usando la API bulk para mayor eficiencia.
    """
    print(f"Preparando {len(new_results_list)} nuevos documentos para indexar en Elasticsearch...")

    def generate_es_actions():
        for result_dict in new_results_list:
            # La lista contiene diccionarios, cada uno con una sola clave (el nombre del archivo)
            for filename, data in result_dict.items():
                if "error" in data:
                    print(f"Omitiendo la indexación del video con error: {filename} ({data['error']})")
                    continue

                # 1. Transformar 'detected_objects' a formato nested
                objects_detected_nested = [
                    {"type": obj_type, "count": count}
                    for obj_type, count in data.get("detected_objects", {}).items()
                ]

                # 2. Convertir 'processing_time' a formato ISO 8601
                processing_time_iso = datetime.datetime.strptime(
                    data["processing_time"], "%Y-%m-%d %H:%M:%S"
                ).isoformat()
                
                # 3. Generar la URL del video
                video_filename_mp4 = filename.replace('.mpg', '.mp4')
                video_url = f"{ES_VIDEO_BASE_URL}/{video_filename_mp4}"
                
                # 4. Construir el documento final para Elasticsearch
                doc_id = str(uuid.uuid4())
                yield {
                    "_index": ES_INDEX_NAME,
                    "_id": doc_id,
                    "_source": {
                        "id": doc_id,
                        "filename": filename,
                        "url": video_url,
                        "duration": 310.0,  # Valor de ejemplo, se puede extraer con ffprobe si es necesario
                        "objects_detected": objects_detected_nested,
                        "date": processing_time_iso,
                        "location": "camera_1",  # Valor de ejemplo
                        "resolution": "720x480", # Valor de ejemplo
                        "codec": "h264",        # Valor de ejemplo
                        "processing_time": processing_time_iso,
                        "priority": data.get("priority", "Mid")
                    }
                }

    try:
        actions = list(generate_es_actions())
        if not actions:
            print("No hay acciones válidas para enviar a Elasticsearch.")
            return
            
        success, failed = helpers.bulk(es_client, actions)
        print(f"¡Carga a Elasticsearch completada! Documentos exitosos: {success}, Fallidos: {failed}")
        if failed:
            print(f"Detalles de los fallos: {failed}")
            
    except Exception as e:
        print(f"ERROR CRÍTICO: Falló la operación bulk de Elasticsearch: {e}")


# ==============================================================================
# PUNTO DE ENTRADA PRINCIPAL
# ==============================================================================

if __name__ == "__main__":
    
    HDFS_VIDEOS_DIRECTORY = "/user/hduser2/videosSpark/"
    master_results_file = "resultados_finales.json"

    spark = SparkSession.builder.appName("IncrementalVideoProcessorWithES").getOrCreate()

    resultados_maestros = load_master_results(master_results_file)
    print(f"Se encontraron {len(resultados_maestros)} videos ya procesados en '{master_results_file}'.")

    videos_disponibles = get_videos_from_hdfs(HDFS_VIDEOS_DIRECTORY)

    if not videos_disponibles:
        print("No se encontraron videos en el directorio HDFS especificado. Terminando la aplicación.")
    else:
        videos_ya_procesados = set(resultados_maestros.keys())
        videos_a_procesar = [path for path in videos_disponibles if os.path.basename(path) not in videos_ya_procesados]

        if not videos_a_procesar:
            print("¡No hay videos nuevos que procesar! Todos los videos en HDFS ya han sido procesados.")
        else:
            print(f"Se procesarán {len(videos_a_procesar)} videos nuevos en paralelo: {videos_a_procesar}")
            
            video_rdd = spark.sparkContext.parallelize(videos_a_procesar, numSlices=len(videos_a_procesar))
            resultados_nuevos_rdd = video_rdd.map(process_video_and_return_dict)
            lista_resultados_nuevos = resultados_nuevos_rdd.collect()
            
            # --- BLOQUE DE INTEGRACIÓN CON ELASTICSEARCH (NUEVO) ---
            if lista_resultados_nuevos:
                try:
                    es_driver_client = setup_elasticsearch()
                    index_new_results_to_es(es_driver_client, lista_resultados_nuevos)
                except Exception as e:
                    print(f"ADVERTENCIA: El proceso de Spark finalizó, pero la indexación en Elasticsearch falló: {e}")
            # --- FIN DEL BLOQUE DE INTEGRACIÓN ---

            print("Actualizando el archivo de resultados maestro en disco...")
            for res_dict in lista_resultados_nuevos:
                resultados_maestros.update(res_dict)
            
            save_master_results(resultados_maestros, master_results_file)
            
            print("\n--- ¡Procesamiento completado! ---")
            print("Contenido final del archivo maestro en disco:")
            print(json.dumps(resultados_maestros, indent=4))

    spark.stop()
