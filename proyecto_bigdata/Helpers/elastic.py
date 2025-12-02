import json
import os
from typing import Tuple, List, Any
from elasticsearch import Elasticsearch, helpers


# ============================================================
# FUNCIÓN: Crear cliente Elasticsearch desde variables de entorno
# ============================================================

def _crear_cliente() -> Elasticsearch | None:
    """
    Crea una instancia de cliente de Elasticsearch usando las variables
    de entorno ES_CLOUD_URL y ES_API_KEY.
    """
    es_cloud_url = os.getenv("ES_CLOUD_URL")
    es_api_key = os.getenv("ES_API_KEY")

    if not es_cloud_url or not es_api_key:
        print("[ERROR] Faltan variables de entorno ES_CLOUD_URL o ES_API_KEY")
        return None

    try:
        client = Elasticsearch(
            es_cloud_url,
            api_key=es_api_key,
            request_timeout=30
        )
        return client
    except Exception as e:
        print(f"[ERROR] No se pudo crear el cliente de Elasticsearch: {e}")
        return None


# ============================================================
# FUNCIÓN: Probar conexión
# ============================================================

def ping_elasticsearch() -> Tuple[bool, str]:
    """
    Verifica si Elasticsearch está accesible con las credenciales actuales.
    """
    client = _crear_cliente()
    if not client:
        return False, "No se pudo crear el cliente."

    try:
        if client.ping():
            return True, "Conexión exitosa con Elasticsearch."
        else:
            return False, "No se pudo hacer ping al cluster."
    except Exception as e:
        return False, f"Error al conectar con Elasticsearch: {e}"


# ============================================================
# FUNCIÓN: Contar documentos en un índice
# ============================================================

def contar_documentos(client: Elasticsearch, indice: str) -> int:
    """
    Retorna el número de documentos en un índice específico.
    """
    try:
        if not client.indices.exists(index=indice):
            return 0
        resp = client.count(index=indice)
        return resp.get("count", 0)
    except Exception as e:
        print(f"[ERROR] No se pudo contar documentos en '{indice}': {e}")
        return 0


# ============================================================
# FUNCIÓN: Buscar libros
# ============================================================

def buscar_libros(client: Elasticsearch, texto: str = "", autor: str = "", anio_desde: str = "", anio_hasta: str = "", indice: str = "libros") -> List[dict]:
    """
    Búsqueda flexible por texto, autor y rango de años.
    """
    try:
        must = []
        if texto:
            must.append({"multi_match": {"query": texto, "fields": ["titulo", "descripcion"]}})
        if autor:
            must.append({"match": {"autor": autor}})
        if anio_desde or anio_hasta:
            rango = {}
            if anio_desde:
                rango["gte"] = anio_desde
            if anio_hasta:
                rango["lte"] = anio_hasta
            must.append({"range": {"anio": rango}})

        query = {"bool": {"must": must}} if must else {"match_all": {}}

        response = client.search(index=indice, query=query, size=50)
        return [hit["_source"] for hit in response["hits"]["hits"]]
    except Exception as e:
        print(f"[ERROR] No se pudo buscar libros: {e}")
        return []



# ============================================================
# FUNCIÓN: Parsear archivo JSON o NDJSON con libros
# ============================================================

def parsear_json_libros(contenido: str) -> List[dict]:
    """
    Convierte el texto del archivo cargado en una lista de diccionarios.
    Acepta JSON (lista) o NDJSON (líneas separadas).
    """
    try:
        contenido = contenido.strip()
        if contenido.startswith("["):
            return json.loads(contenido)
        else:
            return [json.loads(linea) for linea in contenido.splitlines() if linea.strip()]
    except Exception as e:
        print(f"[ERROR] No se pudo parsear el JSON: {e}")
        return []


# ============================================================
# FUNCIÓN: Indexar libros en Elasticsearch
# ============================================================

def indexar_libros_en_elastic(client: Elasticsearch, libros: List[dict]) -> Tuple[int, List[str]]:
    """
    Indexa múltiples documentos en Elasticsearch usando la API bulk.
    Devuelve (número_indexados, lista_errores).
    """
    errores = []
    total_indexados = 0

    if not libros:
        return 0, ["No hay libros para indexar."]

    acciones = [
        {"_index": "libros", "_source": libro}
        for libro in libros
    ]

    try:
        resp = helpers.bulk(client, acciones, raise_on_error=False)
        total_indexados = resp[0]
    except Exception as e:
        errores.append(str(e))

    return total_indexados, errores


# ============================================================
# 🔧 ALIASES DE COMPATIBILIDAD PARA app.py
# ============================================================

def ping_elastic() -> Tuple[bool, str]:
    """
    Alias de compatibilidad para app.py.
    Internamente llama a ping_elasticsearch().
    """
    return ping_elasticsearch()


def indexar_libros_desde_json_str(contenido_json: str) -> int:
    """
    Recibe el contenido del archivo subido (JSON/NDJSON),
    lo parsea e indexa los documentos en Elasticsearch.
    Devuelve el número total de documentos indexados.
    """
    client = _crear_cliente()
    if not client:
        raise RuntimeError("No se pudo crear el cliente de Elasticsearch.")

    libros = parsear_json_libros(contenido_json)
    total_indexados, errores = indexar_libros_en_elastic(client, libros)

    if errores:
        print(f"[indexar_libros_desde_json_str] {len(errores)} errores detectados:")
        for e in errores[:5]:
            print(" -", e)

    return total_indexados


# ============================================================
# BLOQUE DE PRUEBA LOCAL
# ============================================================

if __name__ == "__main__":
    print("Ping a Elasticsearch (usando get_es_client()):")
    ok, error = ping_elasticsearch()
    print("Resultado:", ok, "| Mensaje:", error)
