import os
from functools import wraps
from datetime import datetime

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

from Helpers import elastic as elastic_helper
from Helpers import mongoDB as mongo_helper

# -------------------------------------------------------
# Configuración básica
# -------------------------------------------------------

APP_NAME = "Mini Biblioteca BigData"

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
UPLOAD_FOLDER = os.path.join(BASE_DIR, "uploads")

os.makedirs(UPLOAD_FOLDER, exist_ok=True)

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "dev-secret-key")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

ALLOWED_EXTENSIONS = {"pdf"}


def allowed_file(filename: str) -> bool:
    return "." in filename and filename.rsplit(".", 1)[1].lower() in ALLOWED_EXTENSIONS


def login_requerido(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            flash("Debes iniciar sesión para acceder a esta sección.", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapper


# -------------------------------------------------------
# Rutas públicas
# -------------------------------------------------------


@app.route("/")
def landing():
    return render_template("landing.html", app_nombre=APP_NAME)


@app.route("/index")
def index():
    # Solo para que url_for('index') funcione en la barra de navegación
    return redirect(url_for("landing"))


@app.route("/buscar", methods=["GET"])
def buscar():
    """
    Página pública de búsqueda contra Elasticsearch.
    """
    termino = (request.args.get("texto_libre") or "").strip()
    autor = (request.args.get("autor") or "").strip()
    anio_desde = (request.args.get("anio_desde") or "").strip()
    anio_hasta = (request.args.get("anio_hasta") or "").strip()

    hay_filtros = any([termino, autor, anio_desde, anio_hasta])

    resultados = []
    total = 0
    error = None

    if hay_filtros:
        try:
            resultados = elastic_helper.buscar_libros(
                termino=termino,
                autor=autor,
                anio_desde=anio_desde,
                anio_hasta=anio_hasta,
            )
            total = len(resultados)
        except Exception as exc:
            error = f"No se pudo consultar Elasticsearch: {exc}"

    return render_template(
        "buscador.html",
        app_nombre=APP_NAME,
        termino=termino,
        autor=autor,
        anio_desde=anio_desde,
        anio_hasta=anio_hasta,
        resultados=resultados,
        total_resultados=total,
        error=error,
    )


# -------------------------------------------------------
# Login / Logout
# -------------------------------------------------------

# Usuario y contraseña de administrador:
# si no configuras variables de entorno, quedan en admin / admin
ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin")


@app.route("/login", methods=["GET", "POST"])
def login():
    if request.method == "POST":
        # Nombres EXACTOS de los campos en login.html
        usuario = (request.form.get("username") or "").strip()
        clave = (request.form.get("password") or "").strip()

        if usuario == ADMIN_USER and clave == ADMIN_PASS:
            session["usuario"] = usuario
            flash("Inicio de sesión correcto.", "success")
            return redirect(url_for("admin"))
        else:
            flash("Usuario o contraseña incorrectos.", "danger")

    return render_template("login.html", app_nombre=APP_NAME)


@app.route("/logout")
def logout():
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("index"))


# -------------------------------------------------------
# Panel de administración
# -------------------------------------------------------


@app.route("/admin")
@login_requerido
def admin():
    # Panel principal (por ahora solo muestra la plantilla)
    return render_template("admin.html", app_nombre=APP_NAME)


@app.route("/admin/cargar", methods=["GET", "POST"])
@login_requerido
def admin_cargar():
    """
    Cargar un PDF, guardarlo y mandarlo a Mongo + Elasticsearch.
    """
    mensaje = None
    error = None

    if request.method == "POST":
        # Nombres EXACTOS de los campos en cargar_archivos.html
        titulo = (request.form.get("titulo") or "").strip()
        autor = (request.form.get("autor") or "").strip()
        anio_str = (request.form.get("anio") or "").strip()
        descripcion = (request.form.get("descripcion") or "").strip()

        fichero = request.files.get("archivo")

        if not fichero or fichero.filename == "":
            error = "Debes seleccionar un archivo PDF."
        elif not allowed_file(fichero.filename):
            error = "Solo se permiten archivos PDF."
        else:
            try:
                anio = int(anio_str) if anio_str else None
            except ValueError:
                anio = None

            filename = secure_filename(fichero.filename)
            ruta_guardado = os.path.join(app.config["UPLOAD_FOLDER"], filename)
            fichero.save(ruta_guardado)

            # Guardar en MongoDB (si está configurado)
            try:
                doc_mongo = {
                    "titulo": titulo,
                    "autor": autor,
                    "anio": anio,
                    "descripcion": descripcion,
                    "archivo": filename,
                    "ruta_archivo": ruta_guardado,
                    "fecha_carga": datetime.utcnow(),
                }
                mongo_helper.guardar_libro(doc_mongo)
            except Exception:
                # Si Mongo no está configurado, no rompemos todo
                pass

            # Indexar en Elasticsearch
            try:
                elastic_helper.indexar_libro(
                    titulo=titulo,
                    autor=autor,
                    anio=anio,
                    descripcion=descripcion,
                    archivo=filename,
                    ruta_archivo=ruta_guardado,
                )
                mensaje = "Documento cargado e indexado correctamente."
            except Exception as exc:
                error = (
                    "El archivo se guardó, pero falló el índice en Elasticsearch: "
                    f"{exc}"
                )

    return render_template(
        "cargar_archivos.html",
        app_nombre=APP_NAME,
        mensaje=mensaje,
        error=error,
    )


@app.route("/admin/elastic")
@login_requerido
def admin_elastic():
    """
    Página de estado de Elasticsearch.
    """
    info = elastic_helper.obtener_estado_indice()

    return render_template(
        "admin_elastic.html",
        app_nombre=APP_NAME,
        indice_actual=info.get("indice"),
        total_docs=info.get("total_docs", 0),
        elastic_error=info.get("error"),
    )


@app.route("/admin/usuarios")
@login_requerido
def admin_usuarios():
    """
    Página de usuarios (mínima, para que la plantilla funcione).
    """
    usuarios = mongo_helper.listar_usuarios()
    return render_template(
        "admin_usuarios.html",
        app_nombre=APP_NAME,
        usuarios=usuarios,
    )


# -------------------------------------------------------
# Punto de entrada
# -------------------------------------------------------

if __name__ == "__main__":
    # Para pruebas locales. En Render se usa gunicorn.
    app.run(host="0.0.0.0", port=5000, debug=True)
