import os
from elasticsearch import Elasticsearch

ES_CLOUD_URL = os.environ.get("ES_CLOUD_URL")
ES_API_KEY = os.environ.get("ES_API_KEY")
ES_INDEX = os.environ.get("ES_INDEX", "libros_bigdata")

if not ES_CLOUD_URL or not ES_API_KEY:
    raise RuntimeError(
        "ES_CLOUD_URL y ES_API_KEY deben estar definidos como variables de entorno."
    )

# Cliente global
_es = Elasticsearch(ES_CLOUD_URL, api_key=ES_API_KEY)


def get_client() -> Elasticsearch:
    return _es


def get_index_name() -> str:
    return ES_INDEX


def crear_indice_si_no_existe():
    """
    Crea el índice si no existe.
    """
    es = get_client()
    if es.indices.exists(index=ES_INDEX):
        return

    es.indices.create(
        index=ES_INDEX,
        mappings={
            "properties": {
                "titulo": {"type": "text"},
                "autor": {"type": "text"},
                "anio": {"type": "integer"},
                "texto": {"type": "text"},
            }
        },
    )


def contar_documentos() -> int:
    """
    Número total de documentos en el índice.
    """
    es = get_client()
    if not es.indices.exists(index=ES_INDEX):
        return 0
    resp = es.count(index=ES_INDEX, query={"match_all": {}})
    return resp["count"]


def buscar_libros(texto=None, autor=None, anio_desde=None, anio_hasta=None, size=50):
    """
    Ejecuta una búsqueda en Elastic con filtros opcionales.
    Devuelve (lista_resultados, total).
    """
    es = get_client()
    must = []
    filtros = []

    if texto:
        must.append(
            {
                "multi_match": {
                    "query": texto,
                    "fields": ["titulo^3", "autor^2", "texto"],
                }
            }
        )

    if autor:
        must.append({"match": {"autor": autor}})

    if anio_desde or anio_hasta:
        rango = {}
        if anio_desde:
            rango["gte"] = int(anio_desde)
        if anio_hasta:
            rango["lte"] = int(anio_hasta)
        filtros.append({"range": {"anio": rango}})

    if not must and not filtros:
        query = {"match_all": {}}
    else:
        query = {"bool": {}}
        if must:
            query["bool"]["must"] = must
        if filtros:
            query["bool"]["filter"] = filtros

    resp = es.search(index=ES_INDEX, query=query, size=size)

    resultados = []
    for hit in resp["hits"]["hits"]:
        src = hit["_source"]
        resultados.append(
            {
                "titulo": src.get("titulo"),
                "autor": src.get("autor"),
                "anio": src.get("anio"),
                "texto": src.get("texto"),
                "score": hit["_score"],
            }
        )

    total = resp["hits"]["total"]["value"]
    return resultados, total


def cargar_libro(titulo, autor, anio, texto):
    """
    Inserta un nuevo documento en el índice.
    """
    es = get_client()
    crear_indice_si_no_existe()

    doc = {
        "titulo": titulo,
        "autor": autor,
        "anio": int(anio) if anio else None,
        "texto": texto,
    }
    es.index(index=ES_INDEX, document=doc)
