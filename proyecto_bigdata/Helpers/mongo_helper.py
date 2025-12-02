import os
from pymongo import MongoClient


def get_mongo_client():
    uri = os.environ.get("MONGO_URI")
    if not uri:
        raise RuntimeError("MONGO_URI no está configurada")
    return MongoClient(uri)


def get_users_collection():
    """
    Devuelve la colección de usuarios.
    DB por defecto: bigdata_app
    Colección: users
    """
    client = get_mongo_client()
    db_name = os.environ.get("MONGO_DB_NAME", "bigdata_app")
    db = client[db_name]
    return db["users"]
