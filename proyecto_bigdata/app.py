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

# ==============================
# Configuración básica
# ==============================

APP_NOMBRE = "Mini Biblioteca BigData"

# Puedes cambiarlos si quieres
ADMIN_USER = "admin"
ADMIN_PASS = "admin123"

# Carpeta de subida de archivos (dentro de static/uploads)
UPLOAD_FOLDER = os.path.join("static", "uploads")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "cambia_esta_clave_secreta")
app.config["UPLOAD_FOLDER"] = UPLOAD_FOLDER

# Crear carpeta de uploads si no existe
os.makedirs(os.path.join(app.root_path, UPLOAD_FOLDER), exist_ok=True)

# ==============================
# Imports de Helpers
# ==============================

# NO creamos archivos nuevos, solo usamos los que ya existen
from Helpers import elastic as elastic_helper       # Helpers/elastic.py
from Helpers import mongoDB as mongo_helper         # Helpers/mongoDB.py
from Helpers import funciones as func_helper        # Helpers/funciones.py
from Helpers import PLN as pln_helper               # Helpers/PLN.py (aunque no lo usemos mucho)


# ==============================
# Utilidades generales
# ==============================

@app.context_processor
def inject_app_nombre():
    """Disponible como {{ app_nombre }} en todas las plantillas."""
    return {"app_nombre": APP_NOMBRE}


def login_requerido(vista):
    """Decorador para proteger rutas de administración."""
    @wraps(vista)
    def wrapper(*args, **kwargs):
        if "usuario" not in session:
            flash("Debes iniciar sesión para acceder al administrador.", "warning")
            return redirect(url_for("login"))
        return vista(*args, **kwargs)

    return wrapper


# ==============================
# Rutas públicas
# ==============================

@app.route("/")
@app.route("/index")
def index():
    """Página de inicio: landing."""
    return render_template("landing.html")


@app.route("/buscar")
def buscar():
    """
    Buscador público.
    Llega con query string ?q=termino
    """
    termino = request.args.get("q", "").strip()
    resultados = []
    total_resultados = 0
    error = None

    if termino:
        if hasattr(elastic_helper, "buscar_libros"):
            try:
                # Se asume que Helpers/elastic.py define buscar_libros(termino)
                resultados = elastic_helper.buscar_libros(termino)
                # Normalmente debería devolver una lista de dicts
                total_resultados = len(resultados) if resultados else 0
            except Exception as e:  # noqa: BLE001
                error = f"Error al buscar en ElasticSearch: {e}"
        else:
            error = (
                "La función 'buscar_libros' no está definida en Helpers/elastic.py "
                "y por eso no se puede realizar la búsqueda."
            )

    return render_template(
        "buscador.html",
        termino=termino,
        resultados=resultados,
        total_resultados=total_resultados,
        error=error,
    )


# ==============================
# Login / Logout
# ==============================

@app.route("/login", methods=["GET", "POST"])
def login():
    """
    Login de administrador.
    El formulario de login.html debe usar:
      name="usuario" para el usuario
      name="clave"   para la contraseña
    """
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
    """Cerrar sesión de administrador."""
    session.clear()
    flash("Sesión cerrada.", "info")
    return redirect(url_for("index"))


# ==============================
# Panel de administración
# ==============================

@app.route("/admin")
@login_requerido
def admin():
    """Panel principal de administración."""
    return render_template("admin.html")


@app.route("/admin/usuarios")
@login_requerido
def admin_usuarios():
    """
    Gestión de usuarios.
    Si tu plantilla admin_usuarios.html necesita datos,
    puedes cargarlos aquí desde mongo_helper.
    """
    # Ejemplo de uso opcional de Helpers.mongoDB:
    usuarios = []
    if hasattr(mongo_helper, "listar_usuarios"):
        try:
            usuarios = mongo_helper.listar_usuarios()
        except Exception:
            # No rompemos la app si falla; simplemente no mostramos usuarios
            usuarios = []

    return render_template("admin_usuarios.html", usuarios=usuarios)


@app.route("/admin/elastic")
@login_requerido
def admin_elastic():
    """
    Gestión / diagnóstico de ElasticSearch.
    Aquí podrías mostrar información del índice, etc.
    """
    estado_elastic = None
    error = None

    if hasattr(elastic_helper, "estado_elastic"):
        try:
            estado_elastic = elastic_helper.estado_elastic()
        except Exception as e:  # noqa: BLE001
            error = f"Error al consultar ElasticSearch: {e}"

    return render_template(
        "admin_elastic.html",
        estado_elastic=estado_elastic,
        error=error,
    )


@app.route("/admin/cargar", methods=["GET", "POST"])
@login_requerido
def cargar_archivos():
    """
    Página para cargar PDFs, guardarlos en Mongo
    y, si está implementado en Helpers, indexarlos en Elastic.
    """
    mensaje = None
    error = None

    if request.method == "POST":
        archivo = request.files.get("archivo")

        if not archivo or archivo.filename.strip() == "":
            error = "Debes seleccionar un archivo PDF."
        else:
            try:
                # Guardar archivo físicamente en static/uploads
                from werkzeug.utils import secure_filename

                nombre_seguro = secure_filename(archivo.filename)
                ruta_relativa = os.path.join(app.config["UPLOAD_FOLDER"], nombre_seguro)
                ruta_absoluta = os.path.join(app.root_path, ruta_relativa)

                archivo.save(ruta_absoluta)

                # Extraer texto del PDF si la función existe
                texto = None
                if hasattr(func_helper, "procesar_pdf"):
                    try:
                        texto = func_helper.procesar_pdf(ruta_absoluta)
                    except Exception:
                        texto = None

                # Guardar en Mongo si la función existe
                id_mongo = None
                if hasattr(mongo_helper, "guardar_libro_mongo"):
                    try:
                        id_mongo = mongo_helper.guardar_libro_mongo(
                            ruta_pdf=ruta_relativa,
                            texto=texto,
                        )
                    except Exception:
                        id_mongo = None

                # Indexar en Elastic si la función existe
                if hasattr(elastic_helper, "indexar_libro") and texto is not None:
                    try:
                        elastic_helper.indexar_libro(
                            ruta_pdf=ruta_relativa,
                            texto=texto,
                            mongo_id=str(id_mongo) if id_mongo else None,
                        )
                    except Exception:
                        # Si falla el indexado, no rompemos; solo no se indexa
                        pass

                mensaje = "Archivo cargado correctamente."
                if id_mongo:
                    mensaje += f" ID Mongo: {id_mongo}"

            except Exception as e:  # noqa: BLE001
                error = f"Error al procesar el archivo: {e}"

    return render_template(
        "cargar_archivos.html",
        mensaje=mensaje,
        error=error,
    )


# ==============================
# Punto de entrada local
# ==============================

if __name__ == "__main__":
    # Para pruebas locales; en Render se usa gunicorn app:app
    app.run(debug=True, host="0.0.0.0", port=5000)
