# proyecto_bigdata/Helpers/funciones.py
from typing import Optional, Dict, List

# Usuarios de prueba para el sistema
# IMPORTANTE: usuario y contraseña están en español en toda la app
USUARIOS_DEMO: List[Dict] = [
    {
        "username": "admin_jose",
        "password": "AdminBD2025!",
        "rol": "admin",
        "nombre": "José Luis Ariza",
    },
    {
        "username": "admin_ana",
        "password": "AdminDatos01!",
        "rol": "admin",
        "nombre": "Ana Gómez",
    },
    {
        "username": "carlos",
        "password": "lector123",
        "rol": "usuario",
        "nombre": "Carlos Pérez",
    },
    {
        "username": "luisa",
        "password": "lectora123",
        "rol": "usuario",
        "nombre": "Luisa Martínez",
    },
]


def obtener_usuario(username: str, password: Optional[str] = None) -> Optional[Dict]:
    """
    Devuelve el usuario cuyo username coincide.
    Si se pasa password, también valida la contraseña.
    """
    username = (username or "").strip()
    password = (password or "").strip()

    for u in USUARIOS_DEMO:
        if u["username"] == username:
            if password is None or u["password"] == password:
                return u
    return None


def usuarios_sin_password() -> List[Dict]:
    """
    Lista de usuarios para mostrar en la interfaz
    (sin exponer las contraseñas).
    """
    lista = []
    for u in USUARIOS_DEMO:
        copia = u.copy()
        copia.pop("password", None)
        lista.append(copia)
    return lista
