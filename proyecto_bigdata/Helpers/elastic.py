# proyecto_bigdata/Helpers/elastic.py
import json
import os
from typing import List, Dict, Tuple, Optional

from dotenv import load_dotenv
from elasticsearch import Elasticsearch, helpers

load_dotenv()

ELASTIC_URL = os.getenv("ELASTIC_URL", "http://localhost:9200")
ELASTIC_USER = os.getenv("ELASTIC_USER")
ELASTIC_PASSWORD = os.getenv("ELASTIC_PASSWORD")
ELASTIC_INDEX = os.getenv("ELASTIC_INDEX", "libros_bigdata")


def get_client() -> Elasticsearch:
    """
    Crea el cliente de Elasticsearch usando las variables de entorno.
    - ELASTIC_URL (por ejemplo, https://xxxx.es.io:443)
    - ELASTIC_USER
    - ELASTIC_PASSWORD
    """
    kwargs = {"hosts": [ELASTIC_URL]}

    if ELASTIC_USER and ELASTIC_PASSWORD:
        kwargs["basic_auth"] = (ELASTIC_USER, ELASTIC_PASSWORD)

    # Para Elastic Cloud suele ser HTTPS, el cliente maneja el certificado.
    client = Elasticsearch(**kwargs)
    return client


def ping_elastic() -> bool:
    try:
        es = get_client()
        return bool(es.ping())
    except Exception:
        return False


def contar_documentos(index: Optional[str] = None) -> int:
    index = index or ELASTIC_INDEX
    try:
        es = get_client()
        if not es.indices.exists(index=index):
            return 0
        respuesta = es.count(index=index)
        return int(respuesta.get("count", 0))
    except Exception:
        return 0


def buscar_libros(
    texto: str = "",
    autor: str = "",
    anio_desde: str = "",
    anio_hasta: str = "",
    index: Optional[str] = None,
    size: int = 50,
) -> Tuple[List[Dict], int]:
    """
    Realiza la consulta en Elasticsearch.
    Devuelve (lista_de_resultados, total_resultados).
    """
    index = index or ELASTIC_INDEX
    es = get_client()

    must = []
    filtros = []

    texto = (texto or "").strip()
    autor = (autor or "").strip()
    anio_desde = (anio_desde or "").strip()
    anio_hasta = (anio_hasta or "").strip()

    if texto:
        must.append(
            {
                "multi_match": {
                    "query": texto,
                    "fields": [
                        "titulo^3",
                        "autor^2",
                        "resumen",
                        "tags",
                    ],
                }
            }
        )

    if autor:
        must.append(
            {
                "match_phrase": {
                    "autor": autor,
                }
            }
        )

    if anio_desde or anio_hasta:
        rango = {}
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
            filtros.append(
                {
                    "range": {
                        "anio": rango
                    }
                }
            )

    if not must:
        # Si no hay filtros de texto, hacemos un match_all y solo aplicamos rangos.
        query = {
            "bool": {
                "must": [{"match_all": {}}],
                "filter": filtros,
            }
        }
    else:
        query = {
            "bool": {
                "must": must,
                "filter": filtros,
            }
        }

    cuerpo = {
        "query": query,
        "size": size,
    }

    respuesta = es.search(index=index, body=cuerpo)
    hits = respuesta["hits"]["hits"]
    total = respuesta["hits"]["total"]["value"]

    resultados = []
    for h in hits:
        src = h.get("_source", {})
        resultados.append(
            {
                "titulo": src.get("titulo", ""),
                "autor": src.get("autor", ""),
                "anio": src.get("anio"),
                "categoria": src.get("categoria", ""),
                "resumen": src.get("resumen", ""),
                "id": h.get("_id"),
            }
        )

    return resultados, int(total)


def parsear_json_libros(json_str: str) -> List[Dict]:
    """
    Recibe un string JSON con una lista de libros.
    Valida mínimamente el formato.
    """
    datos = json.loads(json_str)

    if isinstance(datos, dict):
        # Por si viene {"libros": [...]}
        if "libros" in datos and isinstance(datos["libros"], list):
            datos = datos["libros"]
        else:
            raise ValueError("El JSON debe ser una lista de libros o un dict con clave 'libros'.")

    if not isinstance(datos, list):
        raise ValueError("El JSON debe ser una lista de libros.")

    libros_limpios = []
    for i, libro in enumerate(datos, start=1):
        if not isinstance(libro, dict):
            raise ValueError(f"El elemento {i} no es un objeto JSON.")

        # Campos básicos esperados
        titulo = str(libro.get("titulo", "")).strip()
        autor = str(libro.get("autor", "")).strip()

        if not titulo:
            raise ValueError(f"El libro {i} no tiene 'titulo'.")

        if not autor:
            raise ValueError(f"El libro {i} no tiene 'autor'.")

        # Normalizamos año si existe
        anio = libro.get("anio")
        if anio is not None:
            try:
                anio = int(anio)
            except (TypeError, ValueError):
                raise ValueError(f"El libro {i} tiene un 'anio' inválido.")

        libro_limpio = {
            "titulo": titulo,
            "autor": autor,
            "anio": anio,
            "categoria": str(libro.get("categoria", "")).strip(),
            "resumen": str(libro.get("resumen", "")).strip(),
            "tags": libro.get("tags", []),
        }
        libros_limpios.append(libro_limpio)

    return libros_limpios


def indexar_libros_en_elastic(libros: List[Dict], index: Optional[str] = None) -> int:
    """
    Indexa los libros en Elasticsearch mediante bulk.
    Devuelve cuántos documentos se intentaron indexar.
    """
    index = index or ELASTIC_INDEX
    es = get_client()

    acciones = (
        {
            "_index": index,
            "_source": libro,
        }
        for libro in libros
    )

    helpers.bulk(es, acciones)
    return len(libros)


def indexar_libros_desde_json_str(json_str: str) -> int:
    """
    Atajo: recibe un string JSON, lo parsea y lo indexa.
    Devuelve el número de libros.
    """
    libros = parsear_json_libros(json_str)
    return indexar_libros_en_elastic(libros)
