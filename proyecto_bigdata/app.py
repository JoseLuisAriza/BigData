import os
import json
from functools import wraps
from io import TextIOWrapper

from flask import (
    Flask,
    render_template,
    request,
    redirect,
    url_for,
    flash,
    session,
)
from dotenv import load_dotenv

# Helpers propios
from Helpers.mongoDB import insertar_libros, obtener_todos_los_libros
from Helpers.elastic import (
    ping_elastic,
    contar_documentos,
    indexar_libros,
    buscar_libros,
)

# ---------------------------------------------------------------------
# Configuración básica
# ---------------------------------------------------------------------
load_dotenv()

APP_NAME = "Mini Biblioteca BigData"

ADMIN_USER = os.getenv("ADMIN_USER", "admin")
ADMIN_PASS = os.getenv("ADMIN_PASS", "admin123")
SECRET_KEY = os.getenv("SECRET_KEY", "cambia_esta_clave_en_.env")

app = Flask(__name__)
app.secret_key = SECRET_KEY


# ---------------------------------------------------------------------
# Contexto global para templates
# ---------------------------------------------------------------------
@app.context_processor
def inject_app_name():
    return {"app_nombre": APP_NAME}


# ---------------------------------------------------------------------
# Decorador de login requerido
# ---------------------------------------------------------------------
def login_requerido(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            flash("Debes iniciar sesión para acceder al panel de administración.", "warning")
            return redirect(url_for("login"))
        return view_func(*args, **kwargs)

    return wrapper


# ---------------------------------------------------------------------
# Rutas públicas
# ---------------------------------------------------------------------
@app.route("/")
def index():
    """Landing page."""
    return render_template("landing.html")


@app.route("/buscar")
def buscar():
    """Buscador público contra Elasticsearch."""
    texto = request.args.get("texto", "").strip()
    autor = request.args.get("autor", "").strip()
    anio_desde = request.args.get("anio_desde", "").strip() or None
    anio_hasta = request.args.get("anio_hasta", "").strip() or None

    resultados = []
    total = 0
    error = None

    # Solo buscamos si hay algún filtro
    if texto or autor or anio_desde or anio_hasta:
        try:
            resultados, total = buscar_libros(
                texto=texto,
                autor=autor,
                anio_desde=anio_desde,
                anio_hasta=anio_hasta,
            )
        except Exception as e:  # noqa: BLE001
            error = str(e)
            flash(f"Error al consultar Elasticsearch: {e}", "danger")

    return render_template(
        "buscador.html",
        texto=texto,
        autor=autor,
        anio_desde=anio_desde or "",
        anio_hasta=anio_hasta or "",
        resultados=resultados,
        total_resultados=total,
        error=error,
    )


# ---------------------------------------------------------------------
# Login / Logout
# ---------------------------------------------------------------------
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
    flash("Sesión cerrada correctamente.", "info")
    return redirect(url_for("index"))


# ---------------------------------------------------------------------
# Panel de administración
# ---------------------------------------------------------------------
@app.route("/admin")
@login_requerido
def admin():
    return render_template("admin.html")


@app.route("/admin/usuarios")
@login_requerido
def admin_usuarios():
    """Solo plantilla informativa, sin lógica compleja."""
    return render_template("admin_usuarios.html")


@app.route("/admin/elastic")
@login_requerido
def admin_elastic():
    """Panel básico de estado de Elasticsearch."""
    estado = {
        "elastic_ok": False,
        "total_docs": 0,
        "error": None,
    }

    try:
        estado["elastic_ok"] = ping_elastic()
        if estado["elastic_ok"]:
            estado["total_docs"] = contar_documentos()
        else:
            estado["error"] = "No se pudo hacer ping al servidor de Elasticsearch."
    except Exception as e:  # noqa: BLE001
        estado["error"] = str(e)

    return render_template("admin_elastic.html", estado=estado)


@app.route("/admin/cargar", methods=["GET", "POST"])
@login_requerido
def cargar_archivos():
    """
    Carga un archivo JSON con libros,
    los guarda en MongoDB y los indexa en Elasticsearch.
    """
    estado = {
        "total_mongo": None,
        "total_elastic": None,
        "error": None,
    }

    if request.method == "POST":
        archivo = request.files.get("archivo")

        if not archivo or archivo.filename == "":
            flash("Selecciona un archivo JSON con los libros.", "warning")
        else:
            try:
                # Leemos el JSON directamente desde el stream
                wrapper = TextIOWrapper(archivo.stream, encoding="utf-8")
                data = json.load(wrapper)

                # Aceptamos: lista de libros o dict con clave 'libros'
                if isinstance(data, dict):
                    if "libros" in data:
                        data = data["libros"]
                    else:
                        data = [data]

                if not isinstance(data, list):
                    raise ValueError("El JSON debe contener una lista de libros.")

                # Filtramos solo dicts
                libros = [d for d in data if isinstance(d, dict)]

                if not libros:
                    raise ValueError("No se encontraron libros válidos en el archivo.")

                # Guardamos en MongoDB
                total_mongo = insertar_libros(libros)

                # Indexamos en Elasticsearch
                total_elastic = indexar_libros(libros)

                estado["total_mongo"] = total_mongo
                estado["total_elastic"] = total_elastic

                flash(
                    f"Se cargaron {total_mongo} libros en MongoDB "
                    f"y se indexaron {total_elastic} en Elasticsearch.",
                    "success",
                )
            except Exception as e:  # noqa: BLE001
                estado["error"] = str(e)
                flash(f"Error al procesar el archivo: {e}", "danger")

    # Info del índice actual
    try:
        total_docs_index = contar_documentos()
    except Exception:  # noqa: BLE001
        total_docs_index = 0

    return render_template(
        "cargar_archivos.html",
        estado=estado,
        total_docs_index=total_docs_index,
    )


# ---------------------------------------------------------------------
# Punto de entrada
# ---------------------------------------------------------------------
if __name__ == "__main__":
    # Para desarrollo local
    app.run(host="0.0.0.0", port=5000, debug=True)
