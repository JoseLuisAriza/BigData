from google.colab import drive
drive.mount('/content/drive')

!pip install PyPDF2

#Conectar elasticsearch desde Colab (usando la NUEVA API key)

!pip install elasticsearch tqdm

from elasticsearch import Elasticsearch

ES_CLOUD_URL = "https://55a79f5769ba4e0fa584f902639b9a95.us-central1.gcp.cloud.es.io"
ES_API_KEY   = "czl4ZTJKb0JTWHI2NmlYUzFIY286VUhfUmZidjdCQVcwSjdESFEybXl5Zw=="

es = Elasticsearch(
    ES_CLOUD_URL,
    api_key=ES_API_KEY,
    verify_certs=True
)

print(es.info())

import os
import re
import json

# Carpeta donde tienes los PDFs
CARPETA_PDFS = "/content/drive/MyDrive/Ucentral/2025S2/BigData/Final/Data/minibiblioteca"

# Carpeta donde quieres guardar el JSON
CARPETA_SALIDA = "/content/drive/MyDrive/Ucentral/2025S2/BigData/Final/Data"

# Nombre del JSON final
RUTA_JSON_SALIDA = os.path.join(CARPETA_SALIDA, "libros_minibiblioteca.json")


def extraer_anio_desde_nombre(nombre_archivo):
    """
    Busca un año de 4 dígitos (entre 1900 y 2099) en el nombre del archivo.
    Si no encuentra, devuelve None.
    """
    base = os.path.splitext(nombre_archivo)[0]
    m = re.search(r"(19|20)\d{2}", base)
    if m:
        return int(m.group(0))
    return None


documentos = []
errores = []

print("Carpeta PDFs existe:", os.path.exists(CARPETA_PDFS))
print("Archivos encontrados:", len(os.listdir(CARPETA_PDFS)))

for nombre in os.listdir(CARPETA_PDFS):
    if not nombre.lower().endswith(".pdf"):
        continue  # ignorar archivos que no sean PDF

    ruta_pdf = os.path.join(CARPETA_PDFS, nombre)
    print("Procesando:", nombre)

    try:
        # título provisional = nombre del archivo sin extensión
        titulo = os.path.splitext(nombre)[0]

        # intentar extraer año del nombre
        anio = extraer_anio_desde_nombre(nombre)

        doc = {
            "id_libro": len(documentos) + 1,
            "titulo": titulo,
            "autor": None,     # si luego tienes autor lo puedes actualizar
            "anio": anio,
            "ruta_pdf": ruta_pdf
        }

        documentos.append(doc)

    except Exception as e:
        print(f"  -> ERROR al procesar {nombre}: {e}")
        errores.append({"archivo": nombre, "motivo": "error", "detalle": str(e)})

print("\nResumen:")
print("Documentos creados correctamente:", len(documentos))
print("Archivos con problemas:", len(errores))

# Crear carpeta de salida si no existe
os.makedirs(CARPETA_SALIDA, exist_ok=True)

# Guardar el JSON con todos los libros procesados
with open(RUTA_JSON_SALIDA, "w", encoding="utf-8") as f:
    json.dump(documentos, f, ensure_ascii=False, indent=2)

print("\nJSON guardado en:", RUTA_JSON_SALIDA)

# Crear el índice

INDEX_NAME = "libros_bigdata"

mapping = {
    "mappings": {
        "properties": {
            "id_libro":  {"type": "integer"},
            "titulo":    {"type": "text", "analyzer": "spanish"},
            "autor":     {"type": "text", "analyzer": "spanish"},
            "anio":      {"type": "integer"},
            "ruta_pdf":  {"type": "keyword"}
        }
    }
}

# Si ya existía, se borra
if es.indices.exists(index=INDEX_NAME):
    es.indices.delete(index=INDEX_NAME)

es.indices.create(index=INDEX_NAME, body=mapping)
print("Índice creado:", INDEX_NAME)


#Cargar tu JSON libros_minibiblioteca.json

import json
from tqdm import tqdm

RUTA_JSON = "/content/drive/MyDrive/Ucentral/2025S2/BigData/Final/Data/libros_minibiblioteca.json"

with open(RUTA_JSON, "r", encoding="utf-8") as f:
    libros = json.load(f)

print("Libros en JSON:", len(libros))

for libro in tqdm(libros):
    # quitar campos None
    doc = {k: v for k, v in libro.items() if v is not None}
    # usar id_libro como ID de documento
    es.index(index=INDEX_NAME, id=doc["id_libro"], document=doc)

print("Carga terminada.")

import os
import json
from elasticsearch import Elasticsearch, helpers

# ======================================================
# VARIABLES DESDE RENDER
# ======================================================

ES_CLOUD_ID = os.getenv("ES_CLOUD_ID")      # << ESTA ES LA BUENA
ES_API_KEY  = os.getenv("ES_API_KEY")
INDEX_NAME  = "libros_bigdata"

RUTA_JSON = "/content/drive/MyDrive/Ucentral/2025S2/BigData/Final/Data/libros_minibiblioteca.json"


# ======================================================
def get_es_client():
    if not ES_CLOUD_ID:
        raise ValueError("ES_CLOUD_ID no está definido (Render no lo envió).")

    client = Elasticsearch(
        cloud_id=ES_CLOUD_ID,
        api_key=ES_API_KEY
    )
    return client


def ping_es(client):
    print("Ping Elasticsearch:", client.ping())


def cargar_json(ruta_json):
    with open(ruta_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    limpio = []
    for d in data:
        limpio.append({
            "id_libro": d.get("id_libro"),
            "titulo": d.get("titulo"),
            "autor": d.get("autor"),
            "anio": d.get("anio"),
            "ruta_pdf": d.get("ruta_pdf"),
        })

    print(f"Se cargaron {len(limpio)} libros (solo metadatos).")
    return limpio


def bulk_index(client, index_name, docs):
    acciones = (
        {
            "_index": index_name,
            "_id": d["id_libro"],
            "_source": d
        }
        for d in docs
    )

    helpers.bulk(client, acciones)
    print("✔ Bulk OK")


def verificar(client, index_name):
    if not client.indices.exists(index=index_name):
        print("El índice NO existe.")
        return

    count = client.count(index=index_name)["count"]
    print(f"Documentos en el índice: {count}")

    if count > 0:
        print("\nEjemplos:")
        hits = client.search(index=index_name, size=5)["hits"]["hits"]
        for h in hits:
            print(h["_source"])


# ======================================================
if __name__ == "__main__":

    es = get_es_client()
    ping_es(es)

    libros = cargar_json(RUTA_JSON)

    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print("Índice borrado.")

    es.indices.create(index=INDEX_NAME)
    print("Índice creado.")

    bulk_index(es, INDEX_NAME, libros)

    verificar(es, INDEX_NAME)




