import os
from pymongo import MongoClient
from werkzeug.security import generate_password_hash, check_password_hash

MONGO_URI = os.environ.get("MONGO_URI")

if not MONGO_URI:
    raise RuntimeError("Falta la variable de entorno MONGO_URI")

_client = MongoClient(MONGO_URI)
_db = _client["bigdata_app"]          # nombre de la base dentro del cluster
_users = _db["users"]                 # colección de usuarios


def get_users_collection():
    return _users


def create_user(username: str, password: str, is_admin: bool = False):
    """
    Crea un usuario nuevo. Devuelve (ok, mensaje).
    """
    existing = _users.find_one({"username": username})
    if existing:
        return False, "El usuario ya existe"

    hashed = generate_password_hash(password)
    _users.insert_one(
        {
            "username": username,
            "password": hashed,
            "is_admin": bool(is_admin),
        }
    )
    return True, "Usuario creado correctamente"


def verify_user(username: str, password: str):
    """
    Devuelve el documento de usuario si la contraseña es correcta, si no devuelve None.
    """
    user = _users.find_one({"username": username})
    if not user:
        return None

    if not check_password_hash(user["password"], password):
        return None

    return user


def ensure_admin_user():
    """
    Si no existe ningún admin, crea uno por defecto.
    """
    admin = _users.find_one({"is_admin": True})
    if not admin:
        create_user("admin", "admin123", is_admin=True)
