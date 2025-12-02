# app.py
import os
from functools import wraps

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
)
from werkzeug.utils import secure_filename

# Estos imports son los que pide la estructura del profe
from Helpers import mongoDB  # noqa: F401
from Helpers import elastic  # noqa: F401

# Conexiones directas (para no depender de nombres de funciones internas)
from pymongo import MongoClient
from elasticsearch import Elasticsearch

# -------------------------------------------------------------------------
# Configuración básica
# -------------------------------------------------------------------------

# Si tienes variables de entorno en Render (.env allí no se carga solo),
# las lee desde el entorno del sistema.
MONGO_URI = os.environ.get("MONGO_URI", "mongodb://localhost:27017")
MONGO_DB_NAME = os.environ.get("MONGO_DB", "biblioteca_bigdata")

ES_URL = os.environ.get("ES_URL", "http://localhost:9200")
ES_INDEX = os.environ.get("ES_INDEX", "libros")

ADMIN_USER = os.environ.get("ADMIN_USER", "admin")
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin123")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "static", "uploads")
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER
app.secret_key = os.environ.get("SECRET_KEY", "BigData_Ucentral_2025")

# -------------------------------------------------------------------------
# Clientes de MongoDB y ElasticSearch
# -------------------------------------------------------------------------

mongo_client = MongoClient(MONGO_URI)
mongo_db = mongo_client[MONGO_DB_NAME]

# verify_certs=False evita problemas si es un ES simple de pruebas
es = Elasticsearch(ES_URL, verify_certs=False)


# -------------------------------------------------------------------------
# Helpers de autenticación
# -------------------------------------------------------------------------

def login_requerido(vista):
    @wraps(vista)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            flash("Debe iniciar sesión para acceder al panel de administración.", "warning")
            return redirect(url_for("login"))
        return vista(*args, **kwargs)

    return wrapper


# -------------------------------------------------------------------------
# Rutas públicas
# -------------------------------------------------------------------------

@app.route("/")
def index():
    """Landing page. En base.html se usa url_for('index'), por eso
    la función debe llamarse exactamente 'index'."""
    return render_template("landing.html")


@app.route("/buscar", methods=["GET"])
def buscar():
    """
    Buscador público.
    Lee el parámetro de búsqueda de varios nombres posibles:
    q / termino / query, para adaptarse al formulario que tengas.
    """
    termino = (
        request.args.get("q")
        or request.args.get("termino")
        or request.args.get("query")
        or ""
    ).strip()

    resultados = []
    total_resultados = 0
    error = None

    if termino:
        try:
            consulta = {
                "query": {
                    "multi_match": {
                        "query": termino,
                        "fields": ["titulo^2", "autor", "texto", "nombre"],
                        "fuzziness": "AUTO",
                    }
                }
            }

            respuesta = es.search(index=ES_INDEX, body=consulta)
            total_resultados = respuesta["hits"]["total"]["value"]

            for hit in respuesta["hits"]["hits"]:
                doc = hit.get("_source", {}).copy()
                doc["_score"] = hit.get("_score", 0)
                resultados.append(doc)

        except Exception as exc:  # noqa: BLE001
            # No queremos que el usuario vea un 500 si falla Elastic
            error = f"Error al buscar en ElasticSearch: {exc}"

    return render_template(
        "buscador.html",
        termino=termino,
        resultados=resultados,
        total_resultados=total_resultados,
        error=error,
    )


# -------------------------------------------------------------------------
# Login / Logout
# -------------------------------------------------------------------------

@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        usuario = request.form.get("usuario", "").strip()
        clave = request.form.get("clave", "").strip()

        if usuario == ADMIN_USER and clave == ADMIN_PASS:
            session["usuario"] = usuario
            flash("Inicio de sesión correcto.", "success")
            return redirect(url_for("admin"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    return render_template("login.html")


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("index"))


# -------------------------------------------------------------------------
# Panel de administración
# -------------------------------------------------------------------------

@app.route("/admin")
@login_requerido
def admin():
    return render_template("admin.html")


@app.route("/admin/usuarios")
@app.route("/admin_usuarios")
@login_requerido
def admin_usuarios():
    """
    Ejemplo sencillo: listamos los usuarios guardados en Mongo (si existen).
    Si no hay colección, simplemente devolvemos lista vacía.
    """
    try:
        usuarios = list(mongo_db["usuarios"].find())
    except Exception:  # noqa: BLE001
        usuarios = []

    return render_template("admin_usuarios.html", usuarios=usuarios)


@app.route("/admin/elastic")
@app.route("/admin_elastic")
@login_requerido
def admin_elastic():
    """Pantalla de estado de Elastic."""
    indice_existe = False
    cantidad = 0
    error = None

    try:
        indice_existe = es.indices.exists(index=ES_INDEX)
        if indice_existe:
            resp = es.count(index=ES_INDEX)
            cantidad = resp.get("count", 0)
    except Exception as exc:  # noqa: BLE001
        error = f"No se pudo conectar a ElasticSearch: {exc}"

    return render_template(
        "admin_elastic.html",
        indice=ES_INDEX,
        indice_existe=indice_existe,
        cantidad=cantidad,
        error=error,
    )


# -------------------------------------------------------------------------
# Carga de archivos
# -------------------------------------------------------------------------

@app.route("/admin/cargar", methods=["GET", "POST"])
@app.route("/admin/cargar_archivos", methods=["GET", "POST"])
@app.route("/cargar_archivos", methods=["GET", "POST"])
@login_requerido
def cargar_archivos():
    """
    Cargar documentos al sistema.
    - Guarda los archivos en static/uploads
    - Inserta un registro mínimo en Mongo
    - Intenta indexar el documento en Elastic (solo nombre y ruta)
    """
    if request.method == "POST":
        archivos = [f for f in request.files.values() if f and f.filename]
        guardados = []

        for archivo in archivos:
            filename = secure_filename(archivo.filename)
            ruta_absoluta = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            ruta_relativa = os.path.relpath(ruta_absoluta, BASE_DIR)

            archivo.save(ruta_absoluta)

            # Registro básico en Mongo
            doc_mongo = {
                "nombre": filename,
                "ruta": ruta_relativa.replace("\\", "/"),
            }
            try:
                mongo_db["documentos"].insert_one(doc_mongo)
            except Exception:
                # No rompemos la app si Mongo falla
                pass

            # Indexación básica en Elastic (solo nombre y ruta)
            try:
                es.index(
                    index=ES_INDEX,
                    document={
                        "nombre": filename,
                        "ruta": ruta_relativa.replace("\\", "/"),
                    },
                )
            except Exception:
                # Tampoco rompemos si Elastic falla; se informa solo con flash
                pass

            guardados.append(filename)

        if guardados:
            flash(f"Se cargaron {len(guardados)} archivo(s) correctamente.", "success")
        else:
            flash("No se recibió ningún archivo.", "warning")

    return render_template("cargar_archivos.html")


# -------------------------------------------------------------------------
# Punto de entrada local
# -------------------------------------------------------------------------

if __name__ == "__main__":
    # En Render se ignora este debug, porque usa gunicorn.
    app.run(debug=True)
