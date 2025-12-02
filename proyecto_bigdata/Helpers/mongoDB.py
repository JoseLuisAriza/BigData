import os
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash

MONGO_URI = os.environ.get("MONGO_URI")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI no está definido en las variables de entorno.")

MONGO_DB_NAME = os.environ.get("MONGO_DB_NAME", "bigdata_db")

_client = MongoClient(MONGO_URI)
_db = _client[MONGO_DB_NAME]
_usuarios = _db["usuarios"]


def get_usuarios_collection():
    """
    Devuelve la colección de usuarios.
    """
    return _usuarios


def crear_usuario(username: str, password: str, es_admin: bool = False) -> bool:
    """
    Crea un usuario si no existe. Devuelve True si se crea, False si ya existía.
    """
    if _usuarios.find_one({"username": username}):
        return False

    _usuarios.insert_one(
        {
            "username": username,
            "password": generate_password_hash(password),
            "es_admin": bool(es_admin),
        }
    )
    return True


def validar_login(username: str, password: str):
    """
    Devuelve el documento de usuario si el login es correcto; en caso contrario None.
    """
    user = _usuarios.find_one({"username": username})
    if not user:
        return None

    if not check_password_hash(user["password"], password):
        return None

    return user


def asegurar_admin_por_defecto():
    """
    Si no existe ningún admin, crea uno por defecto (admin / admin).
    """
    if _usuarios.count_documents({"es_admin": True}) == 0:
        crear_usuario("admin", "admin", es_admin=True)
