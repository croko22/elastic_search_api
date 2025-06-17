# Elasticsearch Video Search API

Servicio de búsqueda de videos basado en FastAPI e integrado con Elasticsearch. Incluye soporte para detección de objetos.

## Características

- Búsqueda de metadatos de videos
- Almacenamiento de videos en HDFS
- Script de **bulk data** para importar datos masivos a Elasticsearch
- Integración con Docker

---

## Step 1: Configurar Elasticsearch y Kibana

Para configurar localmente Elasticsearch y Kibana, ejecuta este script:

```bash
curl -fsSL https://elastic.co/start-local | sh
```

Después podrás acceder a:

- **Elasticsearch**: http://localhost:9200
- **Kibana**: http://localhost:5601

¡Eso es todo! No hay paso 2.

---

## Setup del Proyecto

### Requisitos

- Python 3.12+
- Docker
- Docker Compose
- (Opcional) HDFS

### Instalación

1. Clona el repositorio:
   ```bash
   git clone https://github.com/tu-repo/elastic_search_api.git
   cd elastic_search_api
   ```

2. Copia y ajusta las variables de entorno:
   ```bash
   cp .env.example .env
   ```

3. Crea y activa el entorno virtual:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```

4. Levanta los servicios con Docker:
   ```bash
   docker-compose up -d
   ```

5. Ejecuta la aplicación:
   ```bash
   ./run.sh
   ```

---

## Bulk Data

El archivo `bulk_data.py` permite insertar un conjunto grande de registros de video directamente en Elasticsearch. Es útil para inicializar la base de datos desde un archivo JSON preexistente.

Ejemplo de uso:
```bash
python bulk_data.py
```

Este script está preconfigurado para usar el archivo `data/sample_videos.json`.

---

## Uso básico

### Buscar videos
```bash
curl "http://localhost:8000/api/search?q=dog"
```

### Subir un video
```bash
curl -X POST -F "file=@tu_video.mp4" http://localhost:8000/api/upload
```

---

## Desarrollo

- Formatear código:
  ```bash
  black .
  ```

- Ejecutar pruebas:
  ```bash
  pytest
  ```