import os
from typing import Any, Dict, List, Tuple, Optional

from elasticsearch import Elasticsearch, helpers


# ============================================================
# Configuración de conexión
# ============================================================

# Nombres de variables de entorno que estás usando en Render
ES_CLOUD_URL = os.getenv("ES_CLOUD_URL") or os.getenv("ELASTIC_URL") or "http://localhost:9200"
ES_API_KEY = os.getenv("ES_API_KEY")

# Opcionalmente, si algún día quieres volver a user/password:
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")

INDICE_LIBROS = "libros_bigdata"


def _crear_cliente() -> Optional[Elasticsearch]:
    """
    Crea el cliente de Elasticsearch según lo que haya en variables de entorno.
    - En Render: usa ES_CLOUD_URL + ES_API_KEY (Elastic Cloud).
    - En local: si no hay nada, intenta http://localhost:9200 sin auth.
    """
    try:
        if ES_API_KEY:
            # Elastic Cloud con API key
            client = Elasticsearch(ES_CLOUD_URL, api_key=ES_API_KEY)
        elif ELASTIC_USER and ELASTIC_PASSWORD:
            # Opción user/password
            client = Elasticsearch(
                ES_CLOUD_URL,
                basic_auth=(ELASTIC_USER, ELASTIC_PASSWORD),
            )
        else:
            # Fallback: sin auth (por ejemplo localhost:9200 en tu PC)
            client = Elasticsearch(ES_CLOUD_URL)

        return client
    except Exception as e:
        print(f"[Elastic] Error creando cliente: {e}")
        return None


# ============================================================
# Funciones de administración / estado
# ============================================================

def ping_elasticsearch() -> Tuple[bool, str]:
    """
    Verifica si Elasticsearch responde al ping.
    Devuelve (ok, mensaje_error).
    """
    client = _crear_cliente()
    if client is None:
        return False, "No se pudo crear el cliente de Elasticsearch (revisa ES_CLOUD_URL / ES_API_KEY)."

    try:
        ok = client.ping()
        if not ok:
            return False, "El servidor de Elasticsearch no respondió al ping."
        return True, ""
    except Exception as e:
        return False, str(e)


def contar_documentos() -> Tuple[int, str]:
    """
    Cuenta documentos en el índice de libros.
    Devuelve (cantidad, mensaje_error).
    """
    client = _crear_cliente()
    if client is None:
        return 0, "No se pudo crear el cliente de Elasticsearch (revisa ES_CLOUD_URL / ES_API_KEY)."

    try:
        if not client.indices.exists(index=INDICE_LIBROS):
            return 0, ""
        resp = client.count(index=INDICE_LIBROS)
        return resp.get("count", 0), ""
    except Exception as e:
        return 0, str(e)


# ============================================================
# Búsqueda de libros
# ============================================================

def buscar_libros(
    texto: str = "",
    autor: str = "",
    anio_desde: str = "",
    anio_hasta: str = "",
    size: int = 50,
) -> Tuple[List[Dict[str, Any]], str]:
    """
    Realiza una búsqueda en Elasticsearch con filtros por texto, autor y rango de años.
    Devuelve (lista_resultados, mensaje_error).
    """
    client = _crear_cliente()
    if client is None:
        return [], "No se pudo crear el cliente de Elasticsearch (revisa ES_CLOUD_URL / ES_API_KEY)."

    must_clauses: List[Dict[str, Any]] = []

    if texto:
        must_clauses.append({
            "multi_match": {
                "query": texto,
                "fields": ["titulo^3", "autor^2", "descripcion", "temas"],
            }
        })

    if autor:
        must_clauses.append({
            "match": {
                "autor": autor
            }
        })

    if anio_desde or anio_hasta:
        rango: Dict[str, Any] = {}
        if anio_desde:
            rango["gte"] = int(anio_desde)
        if anio_hasta:
            rango["lte"] = int(anio_hasta)

        must_clauses.append({
            "range": {
                "anio": rango
            }
        })

    if not must_clauses:
        # Si no hay filtros, que traiga algo razonable
        query: Dict[str, Any] = {"match_all": {}}
    else:
        query = {"bool": {"must": must_clauses}}

    body = {"query": query}

    try:
        resp = client.search(index=INDICE_LIBROS, body=body, size=size)
        resultados: List[Dict[str, Any]] = []
        for hit in resp.get("hits", {}).get("hits", []):
            doc = hit.get("_source", {})
            doc["id"] = hit.get("_id")
            resultados.append(doc)

        return resultados, ""
    except Exception as e:
        return [], str(e)


# ============================================================
# Indexación de libros
# ============================================================

def indexar_libros_en_elastic(libros: List[Dict[str, Any]]) -> Tuple[int, str]:
    """
    Recibe una lista de diccionarios de libros (venidos de MongoDB) y los indexa en Elasticsearch.
    Cada libro debería tener al menos: titulo, autor, anio, descripcion, temas.
    Devuelve (cantidad_indexada, mensaje_error).
    """
    client = _crear_cliente()
    if client is None:
        return 0, "No se pudo crear el cliente de Elasticsearch (revisa ES_CLOUD_URL / ES_API_KEY)."

    acciones = []
    for libro in libros:
        acciones.append({
            "_index": INDICE_LIBROS,
            "_id": str(libro.get("_id", "")),
            "_source": {
                "titulo": libro.get("titulo"),
                "autor": libro.get("autor"),
                "anio": libro.get("anio"),
                "descripcion": libro.get("descripcion"),
                "temas": libro.get("temas"),
            },
        })

    if not acciones:
        return 0, ""

    try:
        helpers.bulk(client, acciones)
        client.indices.refresh(index=INDICE_LIBROS)
        return len(acciones), ""
    except Exception as e:
        return 0, str(e)
