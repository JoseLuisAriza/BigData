import os
from typing import List, Dict, Any

from pymongo import MongoClient

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB = os.getenv("MONGO_DB", "bigdata")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "libros")

_client = None


def _get_client() -> MongoClient:
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client


def _get_collection():
    client = _get_client()
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


def insertar_libros(libros: List[Dict[str, Any]]) -> int:
    """Inserta una lista de libros en la colección."""
    if not libros:
        return 0

    col = _get_collection()
    result = col.insert_many(libros)
    return len(result.inserted_ids)


def obtener_todos_los_libros() -> List[Dict[str, Any]]:
    """Devuelve todos los libros almacenados en MongoDB."""
    col = _get_collection()
    return list(col.find({}))
