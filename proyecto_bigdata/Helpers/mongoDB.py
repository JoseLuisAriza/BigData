import os
from datetime import datetime
from typing import Any, Dict, List

from pymongo import MongoClient


def get_collection():
    """
    Devuelve la colección de libros.
    Si no hay variables de entorno configuradas, usa una BD local.
    """
    uri = os.getenv("MONGO_URI", "mongodb://localhost:27017")
    db_name = os.getenv("MONGO_DB", "mini_biblioteca")
    col_name = os.getenv("MONGO_COLLECTION", "libros")

    client = MongoClient(uri)
    db = client[db_name]
    return db[col_name]


def guardar_libro(doc: Dict[str, Any]) -> None:
    """
    Inserta un documento en la colección de libros.
    """
    col = get_collection()
    if "fecha_carga" not in doc:
        doc["fecha_carga"] = datetime.utcnow()
    col.insert_one(doc)


def listar_usuarios() -> List[Dict[str, Any]]:
    """
    Lista mínima de usuarios para que admin_usuarios.html no reviente.
    Si quieres algo real, se puede ampliar luego.
    """
    return [
        {"usuario": "admin", "rol": "Administrador"},
    ]
