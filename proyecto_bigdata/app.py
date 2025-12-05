# proyecto_bigdata/app.py
import os
from functools import wraps

from dotenv import load_dotenv
from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
)

from Helpers.elastic import (
    buscar_libros,
    contar_documentos,
    ping_elastic,
    indexar_libros_desde_json_str,
)
from Helpers.mongoDB import guardar_libros_mongo, obtener_estadisticas_libros
from Helpers.funciones import obtener_usuario, usuarios_sin_password

load_dotenv()

APP_NAME = "Mini Biblioteca BigData"

app = Flask(__name__, template_folder="templates", static_folder="static")
app.secret_key = os.getenv("SECRET_KEY", "clave_super_secreta")


# ---------------------------------------------------------------------------
# Decoradores de autenticación
# ---------------------------------------------------------------------------

def login_requerido(vista):
    @wraps(vista)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            flash("Debes iniciar sesión para acceder a esta sección.", "warning")
            return redirect(url_for("login"))
        return vista(*args, **kwargs)

    return wrapper


def admin_requerido(vista):
    @wraps(vista)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            flash("Debes iniciar sesión para acceder a esta sección.", "warning")
            return redirect(url_for("login"))

        if session.get("rol") != "admin":
            flash("No tienes permisos de administrador.", "danger")
            return redirect(url_for("buscar"))

        return vista(*args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------------
# Rutas públicas
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("landing.html", app_nombre=APP_NAME)


@app.route("/buscar", methods=["GET"])
def buscar():
    texto = request.args.get("texto", "").strip()

    resultados = []
    total_resultados = 0
    error = None

    hay_filtros = any([texto])

    if hay_filtros:
        try:
            resultados, total_resultados = buscar_libros(
                texto=texto,
            )
        except Exception as e:
            error = f"Error al consultar Elasticsearch: {e}"
            flash(error, "danger")

    return render_template(
        "buscador.html",
        app_nombre=APP_NAME,
        texto=texto,
        resultados=resultados,
        total_resultados=total_resultados,
        error=error,
    )


# ---------------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario_form = request.form.get("usuario", "").strip()
        clave_form = request.form.get("clave", "").strip()

        usuario = obtener_usuario(usuario_form, clave_form)

        if usuario:
            session["usuario"] = usuario["username"]
            session["rol"] = usuario["rol"]
            flash("Inicio de sesión correcto.", "success")

            if usuario["rol"] == "admin":
                return redirect(url_for("admin"))
            else:
                return redirect(url_for("buscar"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    return render_template("login.html", app_nombre=APP_NAME)


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada correctamente.", "info")
    return redirect(url_for("index"))


# ---------------------------------------------------------------------------
# Panel de administración
# ---------------------------------------------------------------------------

@app.route("/admin")
@admin_requerido
def admin():
    # Estadísticas básicas desde MongoDB y Elasticsearch
    estadisticas_mongo = obtener_estadisticas_libros()
    total_es = contar_documentos()

    return render_template(
        "admin.html",
        app_nombre=APP_NAME,
        estadisticas_mongo=estadisticas_mongo,
        total_indexados_es=total_es,
    )


@app.route("/admin/elastic")
@admin_requerido
def admin_elastic():
    indice_actual = os.getenv("ELASTIC_INDEX", "libros_bigdata")

    estado_ping = ping_elastic()
    total_indexados = contar_documentos()

    error = None
    if not estado_ping:
        error = "No se pudo conectar con Elasticsearch."

    return render_template(
        "admin_elastic.html",
        app_nombre=APP_NAME,
        indice_actual=indice_actual,
        total_indexados=total_indexados,
        estado_ping=estado_ping,
        error=error,
    )


@app.route("/admin/usuarios")
@admin_requerido
def admin_usuarios():
    lista = usuarios_sin_password()
    return render_template(
        "admin_usuarios.html",
        app_nombre=APP_NAME,
        usuarios=lista,
    )


@app.route("/admin/cargar", methods=["GET", "POST"])
@admin_requerido
def admin_cargar():
    total_insertados_mongo = None
    total_indexados_es = None
    error = None

    if request.method == "POST":
        archivo = request.files.get("archivo")

        if not archivo or archivo.filename == "":
            flash("Debes seleccionar un archivo JSON.", "warning")
        else:
            try:
                contenido = archivo.read().decode("utf-8")
                # Primero indexamos en Elasticsearch
                n_es = indexar_libros_desde_json_str(contenido)
                total_indexados_es = n_es

                # También guardamos en Mongo
                from Helpers.elastic import parsear_json_libros

                libros = parsear_json_libros(contenido)
                n_mongo = guardar_libros_mongo(libros)
                total_insertados_mongo = n_mongo

                flash(
                    f"Se cargaron {n_es} libros en Elasticsearch y {n_mongo} en MongoDB.",
                    "success",
                )
            except Exception as e:
                error = f"Error al procesar el archivo: {e}"
                flash(error, "danger")

    # Estadísticas actuales
    total_es_actual = contar_documentos()
    estadisticas_mongo = obtener_estadisticas_libros()

    return render_template(
        "cargar_archivos.html",
        app_nombre=APP_NAME,
        total_insertados_mongo=total_insertados_mongo,
        total_indexados_es=total_indexados_es,
        error=error,
        total_es_actual=total_es_actual,
        estadisticas_mongo=estadisticas_mongo,
    )


# ---------------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    debug = os.getenv("FLASK_DEBUG", "0") == "1"
    app.run(host="0.0.0.0", port=5000, debug=debug)
