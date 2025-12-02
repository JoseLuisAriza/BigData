from functools import wraps
from flask import session, redirect, url_for, flash


def admin_requerido(func):
    """
    Decorador para rutas que requieren usuario admin.
    """
    @wraps(func)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            flash("Debes iniciar sesión.", "warning")
            return redirect(url_for("login"))

        if not session.get("es_admin"):
            flash("No tienes permisos para acceder a esta sección.", "danger")
            return redirect(url_for("index"))

        return func(*args, **kwargs)

    return wrapper
