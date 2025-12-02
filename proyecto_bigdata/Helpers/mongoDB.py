# proyecto_bigdata/Helpers/mongoDB.py
import os
from typing import List, Dict

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "")
MONGO_DB_NAME = os.getenv("MONGO_DB_NAME", "biblioteca_bigdata")
MONGO_COLLECTION_LIBROS = os.getenv("MONGO_COLLECTION_LIBROS", "libros")

_client = None


def get_client() -> MongoClient:
    global _client
    if _client is None:
        if not MONGO_URI:
            raise RuntimeError("MONGO_URI no está configurada.")
        _client = MongoClient(MONGO_URI)
    return _client


def guardar_libros_mongo(libros: List[Dict]) -> int:
    """
    Guarda la lista de libros en MongoDB.
    Devuelve cuántos documentos se insertaron.
    """
    if not libros:
        return 0

    db = get_client()[MONGO_DB_NAME]
    col = db[MONGO_COLLECTION_LIBROS]
    resultado = col.insert_many(libros)
    return len(resultado.inserted_ids)


def contar_libros_mongo() -> int:
    try:
        db = get_client()[MONGO_DB_NAME]
        return db[MONGO_COLLECTION_LIBROS].count_documents({})
    except Exception:
        return 0


def obtener_estadisticas_libros() -> Dict:
    """
    Por ahora solo devuelve el total de libros en Mongo.
    Se puede extender fácil si tu profe pide más métricas.
    """
    total = contar_libros_mongo()
    return {
        "total_libros_mongo": total,
    }
