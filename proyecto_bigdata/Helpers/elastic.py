import os
from typing import List, Dict, Any, Tuple

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError  # noqa: N811
from elasticsearch.exceptions import NotFoundError

ELASTIC_CLOUD_ID = os.getenv("ELASTIC_CLOUD_ID")
ELASTIC_API_KEY = os.getenv("ELASTIC_API_KEY")
ELASTIC_HOST = os.getenv("ELASTIC_HOST", "http://localhost:9200")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "libros")


def _get_es_client() -> Elasticsearch:
    if ELASTIC_CLOUD_ID and ELASTIC_API_KEY:
        return Elasticsearch(cloud_id=ELASTIC_CLOUD_ID, api_key=ELASTIC_API_KEY)
    # fallback a host simple
    return Elasticsearch(ELASTIC_HOST)


es = _get_es_client()


def ping_elastic() -> bool:
    try:
        return bool(es.ping())
    except ESConnectionError:
        return False


def _asegurar_indice() -> None:
    """Crea el índice si no existe (con mapping sencillo)."""
    try:
        if not es.indices.exists(index=ELASTIC_INDEX):
            es.indices.create(
                index=ELASTIC_INDEX,
                mappings={
                    "properties": {
                        "titulo": {"type": "text"},
                        "autor": {"type": "text"},
                        "anio": {"type": "integer"},
                        "descripcion": {"type": "text"},
                        "resumen": {"type": "text"},
                    }
                },
            )
    except ESConnectionError:
        # Si no se puede conectar, no hacemos nada aquí.
        pass


def indexar_libros(libros: List[Dict[str, Any]]) -> int:
    """Indexa una lista de libros en Elasticsearch."""
    if not libros:
        return 0

    _asegurar_indice()

    total = 0
    for libro in libros:
        doc = dict(libro)
        # usamos el _id de Mongo si viene, pero como string
        _id = None
        if "_id" in doc:
            _id = str(doc["_id"])
            doc.pop("_id", None)

        es.index(index=ELASTIC_INDEX, id=_id, document=doc)
        total += 1

    es.indices.refresh(index=ELASTIC_INDEX)
    return total


def contar_documentos() -> int:
    """Devuelve el número de documentos del índice."""
    try:
        if not es.indices.exists(index=ELASTIC_INDEX):
            return 0
        resp = es.count(index=ELASTIC_INDEX)
        return int(resp.get("count", 0))
    except (ESConnectionError, NotFoundError):
        return 0


def buscar_libros(
    texto: str = "",
    autor: str = "",
    anio_desde: str | None = None,
    anio_hasta: str | None = None,
) -> Tuple[List[Dict[str, Any]], int]:
    """Busca libros con filtros básicos."""
    if not es.indices.exists(index=ELASTIC_INDEX):
        return [], 0

    must: List[Dict[str, Any]] = []

    if texto:
        must.append(
            {
                "multi_match": {
                    "query": texto,
                    "fields": ["titulo^3", "autor^2", "descripcion", "resumen", "*"],
                }
            }
        )

    if autor:
        must.append({"match": {"autor": autor}})

    if anio_desde or anio_hasta:
        rango: Dict[str, Any] = {}
        if anio_desde:
            rango["gte"] = int(anio_desde)
        if anio_hasta:
            rango["lte"] = int(anio_hasta)
        must.append({"range": {"anio": rango}})

    if must:
        query = {"bool": {"must": must}}
    else:
        query = {"match_all": {}}

    resp = es.search(index=ELASTIC_INDEX, query=query, size=100)

    hits = resp["hits"]["hits"]
    total_raw = resp["hits"]["total"]
    total = total_raw["value"] if isinstance(total_raw, dict) else int(total_raw)

    resultados: List[Dict[str, Any]] = []
    for h in hits:
        fuente = h.get("_source", {}) or {}
        fuente["id"] = h.get("_id")
        resultados.append(fuente)

    return resultados, total
