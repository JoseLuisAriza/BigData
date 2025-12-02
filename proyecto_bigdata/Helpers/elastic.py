import os
from typing import Any, Dict, List, Optional

from elasticsearch import Elasticsearch

# -------------------------------------------------------
# Configuración de cliente
# -------------------------------------------------------


def _get_env(*names: str, default: Optional[str] = None) -> Optional[str]:
    """
    Devuelve el primer valor de variable de entorno que exista
    entre la lista de nombres dada.
    """
    for name in names:
        value = os.getenv(name)
        if value:
            return value
    return default


ELASTIC_HOST = _get_env(
    "ELASTIC_HOST",
    "ELASTICSEARCH_HOST",
    "ELASTIC_URL",
    "ELASTICSEARCH_URL",
    default="http://localhost:9200",
)

ELASTIC_USER = _get_env(
    "ELASTIC_USER",
    "ELASTIC_USERNAME",
    "ELASTICSEARCH_USER",
)

ELASTIC_PASSWORD = _get_env(
    "ELASTIC_PASSWORD",
    "ELASTIC_PASS",
    "ELASTICSEARCH_PASSWORD",
)

INDEX_NAME = _get_env(
    "ELASTIC_INDEX",
    "ELASTICSEARCH_INDEX",
    "ELASTIC_INDICE",
    default="libros",
)


def get_client() -> Elasticsearch:
    """
    Crea un cliente de Elasticsearch usando las variables de entorno.
    """
    kwargs: Dict[str, Any] = {"hosts": [ELASTIC_HOST]}
    if ELASTIC_USER and ELASTIC_PASSWORD:
        kwargs["basic_auth"] = (ELASTIC_USER, ELASTIC_PASSWORD)

    # Para clusters con certificados raros (p. ej. pruebas)
    kwargs.setdefault("verify_certs", False)

    return Elasticsearch(**kwargs)


# -------------------------------------------------------
# Utilidades de índice y búsqueda
# -------------------------------------------------------


def crear_indice_si_no_existe() -> None:
    es = get_client()
    if es.indices.exists(index=INDEX_NAME):
        return

    mapping = {
        "mappings": {
            "properties": {
                "titulo": {"type": "text"},
                "autor": {"type": "text"},
                "anio": {"type": "integer"},
                "descripcion": {"type": "text"},
                "archivo": {"type": "keyword"},
                "ruta_archivo": {"type": "keyword"},
            }
        }
    }
    es.indices.create(index=INDEX_NAME, **mapping)


def indexar_libro(
    titulo: str,
    autor: str,
    anio: Optional[int],
    descripcion: str,
    archivo: str,
    ruta_archivo: str,
) -> str:
    """
    Indexa un documento sencillo en Elasticsearch.
    Devuelve el ID asignado.
    """
    crear_indice_si_no_existe()
    es = get_client()

    doc = {
        "titulo": titulo,
        "autor": autor,
        "anio": anio,
        "descripcion": descripcion,
        "archivo": archivo,
        "ruta_archivo": ruta_archivo,
    }

    resp = es.index(index=INDEX_NAME, document=doc)
    return resp.get("_id", "")


def buscar_libros(
    termino: str = "",
    autor: str = "",
    anio_desde: str = "",
    anio_hasta: str = "",
) -> List[Dict[str, Any]]:
    """
    Ejecuta una búsqueda básica sobre el índice de libros.
    """
    crear_indice_si_no_existe()
    es = get_client()

    must = []
    filtros = []

    if termino:
        must.append(
            {
                "multi_match": {
                    "query": termino,
                    "fields": ["titulo^3", "autor^2", "descripcion"],
                }
            }
        )

    if autor:
        must.append({"match": {"autor": autor}})

    if anio_desde or anio_hasta:
        rango: Dict[str, Any] = {}
        if anio_desde:
            try:
                rango["gte"] = int(anio_desde)
            except ValueError:
                pass
        if anio_hasta:
            try:
                rango["lte"] = int(anio_hasta)
            except ValueError:
                pass
        if rango:
            filtros.append({"range": {"anio": rango}})

    if not must and not filtros:
        query: Dict[str, Any] = {"match_all": {}}
    else:
        bool_query: Dict[str, Any] = {}
        if must:
            bool_query["must"] = must
        if filtros:
            bool_query["filter"] = filtros
        query = {"bool": bool_query}

    resp = es.search(index=INDEX_NAME, query=query, size=50)

    resultados: List[Dict[str, Any]] = []
    for hit in resp["hits"]["hits"]:
        src = hit.get("_source", {})
        resultados.append(
            {
                "id": hit.get("_id"),
                "titulo": src.get("titulo"),
                "autor": src.get("autor"),
                "anio": src.get("anio"),
                "descripcion": src.get("descripcion"),
                "archivo": src.get("archivo"),
            }
        )

    return resultados


def obtener_estado_indice() -> Dict[str, Any]:
    """
    Devuelve información básica del índice para mostrar en /admin/elastic.
    """
    try:
        crear_indice_si_no_existe()
        es = get_client()
        count_resp = es.count(index=INDEX_NAME)
        total = int(count_resp.get("count", 0))
        return {"indice": INDEX_NAME, "total_docs": total, "error": None}
    except Exception as exc:
        return {"indice": None, "total_docs": 0, "error": str(exc)}
