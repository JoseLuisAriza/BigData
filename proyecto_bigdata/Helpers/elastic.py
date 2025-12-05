# proyecto_bigdata/Helpers/elastic.py
import os
import json
from typing import Any, Dict, List, Tuple

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

# Variables de entorno (Render)
ES_CLOUD_ID = os.getenv("ES_CLOUD_ID", "")
ES_API_KEY = os.getenv("ES_API_KEY", "")

# Nombre fijo del índice de libros en Elasticsearch
INDICE_LIBROS = os.getenv("ES_INDEX_NAME", "libros_bigdata")


# ---------------------------------------------------------------------
# Cliente de Elasticsearch
# ---------------------------------------------------------------------
def get_es_client() -> Elasticsearch:
    """
    Crea el cliente de Elasticsearch usando cloud_id y api_key.
    """
    if not ES_CLOUD_ID or not ES_API_KEY:
        raise RuntimeError(
            "Faltan ES_CLOUD_ID o ES_API_KEY en las variables de entorno."
        )

    client = Elasticsearch(
        cloud_id=ES_CLOUD_ID,
        api_key=ES_API_KEY,
    )
    return client


def ping_elastic() -> bool:
    """
    Devuelve True si Elasticsearch responde al ping, False en caso contrario.
    """
    try:
        es = get_es_client()
        return es.ping()
    except Exception:
        return False


# ---------------------------------------------------------------------
# Utilidades de índice
# ---------------------------------------------------------------------
def contar_documentos(index: str = INDICE_LIBROS) -> int:
    """
    Devuelve el número de documentos en el índice (0 si no existe o hay error).
    """
    try:
        es = get_es_client()
        if not es.indices.exists(index=index):
            return 0
        resp = es.count(index=index)
        return int(resp.get("count", 0))
    except Exception:
        return 0


# ---------------------------------------------------------------------
# Búsqueda de libros
# ---------------------------------------------------------------------
def _build_search_query(
    texto: str = "",
) -> Dict[str, Any]:
    """
    Construye la query bool para buscar libros según texto libre.
    """
    must: List[Dict[str, Any]] = []
    filtros: List[Dict[str, Any]] = []

    texto = (texto or "").strip()

    if texto:
        must.append(
            {
                "multi_match": {
                    "query": texto,
                    "fields": ["titulo^3", "ruta_pdf"],
                    "type": "best_fields",
                }
            }
        )

    rango: Dict[str, Any] = {}

    query: Dict[str, Any] = {"bool": {"must": must}}
    if filtros:
        query["bool"]["filter"] = filtros

    return query


def buscar_libros(
    texto: str = "",
    tamano: int = 50,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Ejecuta la búsqueda en Elasticsearch y devuelve:
    - lista de resultados (dicts)
    - total de coincidencias

    Esta firma coincide con cómo lo llama app.py:
    buscar_libros(texto=...)
    """
    es = get_es_client()

    if not es.indices.exists(index=INDICE_LIBROS):
        return [], 0

    query = _build_search_query(texto)

    resp = es.search(
        index=INDICE_LIBROS,
        size=tamano,
        query=query,
    )

    total_obj = resp.get("hits", {}).get("total", 0)
    if isinstance(total_obj, dict):
        total = int(total_obj.get("value", 0))
    else:
        total = int(total_obj)

    resultados: List[Dict[str, Any]] = []
    for hit in resp.get("hits", {}).get("hits", []):
        src = hit.get("_source", {})
        resultados.append(
            {
                "id_libro": src.get("id_libro"),
                "titulo": src.get("titulo"),
                "ruta_pdf": src.get("ruta_pdf"),
                "score": hit.get("_score"),
            }
        )

    return resultados, total


# ---------------------------------------------------------------------
# Carga masiva desde JSON (usado en el panel de admin)
# ---------------------------------------------------------------------
def parsear_json_libros(json_str: str) -> List[Dict[str, Any]]:
    """
    Recibe un string JSON con una lista de libros y normaliza las claves esperadas.
    """
    try:
        data = json.loads(json_str)
    except json.JSONDecodeError as e:
        raise ValueError(f"JSON inválido: {e}") from e

    if not isinstance(data, list):
        raise ValueError("El JSON debe contener una lista de libros.")

    libros: List[Dict[str, Any]] = []
    for i, raw in enumerate(data, start=1):
        libros.append(
            {
                "id_libro": raw.get("id_libro", i),
                "titulo": raw.get("titulo"),
                "ruta_pdf": raw.get("ruta_pdf"),
            }
        )

    return libros


def indexar_libros_desde_json_str(json_str: str) -> Tuple[int, str]:
    """
    Recibe un string JSON (lista de libros) y los indexa en Elasticsearch.
    Devuelve (num_indexados, mensaje_error). Si todo va bien, mensaje_error = "".
    """
    try:
        libros = parsear_json_libros(json_str)
    except Exception as e:
        return 0, f"Error al parsear JSON: {e}"

    if not libros:
        return 0, "El JSON no contiene libros."

    es = get_es_client()

    # Borrar y recrear índice limpio
    try:
        if es.indices.exists(index=INDICE_LIBROS):
            es.indices.delete(index=INDICE_LIBROS)
        es.indices.create(index=INDICE_LIBROS)
    except Exception:
        # Si falla la creación porque el índice ya existe, seguimos igual
        pass

    acciones = [
        {
            "_index": INDICE_LIBROS,
            "_id": libro.get("id_libro"),
            "_source": libro,
        }
        for libro in libros
    ]

    try:
        helpers.bulk(es, acciones)
        es.indices.refresh(index=INDICE_LIBROS)
        return len(acciones), ""
    except Exception as e:
        return 0, f"Error en bulk: {e}"
