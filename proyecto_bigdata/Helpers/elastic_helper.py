import os
from elasticsearch import Elasticsearch

# Leemos las variables de entorno (Render / .env)
ES_CLOUD_URL = os.environ.get("ES_CLOUD_URL")
ES_API_KEY = os.environ.get("ES_API_KEY")

if not ES_CLOUD_URL or not ES_API_KEY:
    raise RuntimeError("Faltan ES_CLOUD_URL o ES_API_KEY en las variables de entorno")

# Cliente global de Elasticsearch
_es = Elasticsearch(
    ES_CLOUD_URL,
    api_key=ES_API_KEY,
    verify_certs=True,
)

# Nombre del índice donde cargaste tus libros
INDEX_NAME = "libros_bigdata"


def buscar_libros(texto=None, autor=None, anio_desde=None, anio_hasta=None, size=30):
    """
    Busca libros en el índice usando texto libre, autor y rango de año.
    Devuelve: (total_resultados, lista_de_documentos)
    """
    must = []
    filtros = []

    if texto:
        must.append({
            "multi_match": {
                "query": texto,
                "fields": ["titulo^3", "texto", "autor"]
            }
        })

    if autor:
        must.append({
            "match": {
                "autor": {
                    "query": autor,
                    "operator": "and"
                }
            }
        })

    if anio_desde or anio_hasta:
        rango = {}
        if anio_desde:
            rango["gte"] = anio_desde
        if anio_hasta:
            rango["lte"] = anio_hasta
        filtros.append({"range": {"anio": rango}})

    if not must:
        must.append({"match_all": {}})

    query = {
        "query": {
            "bool": {
                "must": must,
                "filter": filtros
            }
        },
        "size": size
    }

    res = _es.search(index=INDEX_NAME, body=query)
    hits = res["hits"]["hits"]
    total = res["hits"]["total"]
    if isinstance(total, dict):
        total = total.get("value", total)

    documentos = []
    for h in hits:
        src = h["_source"]
        documentos.append({
            "id": h["_id"],
            "score": h["_score"],
            "titulo": src.get("titulo"),
            "autor": src.get("autor"),
            "anio": src.get("anio"),
            "ruta_pdf": src.get("ruta_pdf"),
        })

    return total, documentos


def contar_documentos():
    """
    Devuelve el número total de documentos en el índice.
    """
    res = _es.count(index=INDEX_NAME)
    return res["count"]


def agregar_libro(doc: dict):
    """
    Inserta un documento (libro) en el índice.
    Espera un dict con al menos 'titulo'; opcionalmente 'autor', 'anio', 'texto', etc.
    """
    _es.index(index=INDEX_NAME, document=doc)
